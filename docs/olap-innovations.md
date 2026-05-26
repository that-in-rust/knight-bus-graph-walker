# Three OLAP Innovations From the Knight Bus Storage Format

> **Premise:** Knight Bus proved that when you pre-compile your data layout to
> match your access pattern, queries collapse from dynamic discovery into static
> arithmetic on contiguous arrays. This document reasons out three innovations
> that apply the same structural insight to OLAP workloads.

---

## The Knight Bus Pattern — Distilled

Before the innovations, the exact pattern that makes Knight Bus work:

```
Neo4j query-time path (what we eliminated):

  string_key
    → hash/B-tree lookup → node record
      → follow pointer → first relationship
        → walk linked list → gather neighbors
          → follow pointer → neighbor node records
            → repeat for multi-hop

Knight Bus query-time path (what we built):

  string_key
    → binary search sorted key_index → dense_id (u32)
      → offsets[dense_id] → start (u64)
      → offsets[dense_id + 1] → end (u64)
        → peers[start..end] → neighbor dense_ids (contiguous u32 slice)
```

Every step is either binary search on a sorted array or direct array indexing.
No pointer chasing. No hash probing. No dynamic allocation. The data layout
IS the query plan.

The five structural elements:

1. **Dense IDs:** String keys → u32 array positions. Nodes ARE indices.
2. **Offsets + Peers (CSR):** Adjacency is a contiguous slice found by arithmetic.
3. **Dual CSR:** Both forward and reverse are pre-built. Neither direction requires reconstruction.
4. **mmap:** The OS manages paging. Only touched regions enter RAM.
5. **Immutability:** No writes, no transactions, no locks, no GC. Build once, query forever.

The code that proves it (`runtime.rs:320-331`):

```rust
fn read_neighbor_ids(&self, dense_id: DenseNodeId, direction: WalkDirection) -> Vec<u32> {
    let (offsets_mmap, peers_mmap) = match direction {
        WalkDirection::Forward => (&self.forward_offsets, &self.forward_peers),
        WalkDirection::Backward => (&self.reverse_offsets, &self.reverse_peers),
    };
    let start = read_u64_from_mmap(offsets_mmap, dense_id.get() as usize) as usize;
    let end = read_u64_from_mmap(offsets_mmap, dense_id.get() as usize + 1) as usize;
    (start..end)
        .map(|index| read_u32_from_mmap(peers_mmap, index))
        .collect()
}
```

Two array reads for the range. One contiguous slice for the neighbors. That is the
entire query engine.

---

## Innovation 1: Offset-Mapped Joins

### The OLAP Problem

Star-schema joins are the backbone of data warehouse queries:

```sql
SELECT d.region, SUM(f.revenue)
FROM facts f
JOIN dim_customer d ON f.customer_id = d.id
GROUP BY d.region
```

What the OLAP engine does at query time:

```
for each fact row:
    hash(fact.customer_id)           ← compute hash
    probe hash_table → dim record    ← random memory access (cache miss)
    read dim.region                  ← another potential cache miss
    hash(region)                     ← compute hash for GROUP BY
    probe/update agg_table           ← another random memory access
```

Two hash table operations per fact row. Every hash probe is a random memory
access — structurally identical to Neo4j chasing relationship pointers through
scattered records.

### The Structural Parallel

A foreign key in a fact table IS an edge in a graph:

```
fact row  --[customer_id]--> dimension record
```

This is the same structure as:

```
node  --[relationship]--> neighbor node
```

Neo4j follows pointers to traverse edges. OLAP engines probe hash tables
to resolve foreign keys. Both are scatter-gather. Both are the same class
of problem.

### The Innovation

Apply the Knight Bus build-time resolution pattern to foreign keys.

**At build time:**

```
1. Assign dense IDs to all dimension records
     customer "ACME Corp" → dense_id 0
     customer "Globex"    → dense_id 1
     ...

2. Store dimension attributes as flat arrays indexed by dense ID
     dim_region.bin:   ["west", "east", ...]  (array, indexed by dense customer ID)
     dim_segment.bin:  ["enterprise", "smb", ...]

3. Replace foreign key columns in the fact table with dense IDs
     fact_customer_id.bin: [0, 1, 0, 3, 1, ...]  (u32 array, one per fact row)

4. Build reverse CSR: dimension → fact rows (like Knight Bus reverse CSR)
     customer_offsets.bin:  [0, 3, 5, ...]   (u64, one per customer + 1)
     customer_fact_ids.bin: [0, 2, 7, 1, 4, ...]  (u32, fact row indices)
```

**At query time:**

```
SELECT d.region, SUM(f.revenue) ... JOIN ... GROUP BY d.region

becomes:

for fact_row in 0..fact_count:
    cust_id = fact_customer_id[fact_row]     // u32 array read — contiguous
    region  = dim_region[cust_id]            // direct array lookup — NO HASH
    revenue = fact_revenue[fact_row]         // u32 array read — contiguous
    accumulator[region_dense_id] += revenue  // fixed-size array — NO HASH
```

The hash join is gone. The GROUP BY hash table is gone. Every access is
a direct array index.

**The reverse direction is also pre-built:**

```
"Show me all orders for customer X"

  dense_id = resolve("ACME Corp")          // binary search, like Knight Bus
  start    = customer_offsets[dense_id]     // u64 read
  end      = customer_offsets[dense_id + 1] // u64 read
  fact_ids = customer_fact_ids[start..end]  // contiguous slice

Same pattern as: peers[offsets[id]..offsets[id+1]]
```

### Snapshot File Layout

```
manifest.json
dim_customer_keys.bin        ← sorted key index (like key_index.bin)
dim_customer_region.bin      ← flat attribute array indexed by dense customer ID
dim_customer_segment.bin     ← flat attribute array indexed by dense customer ID
fact_customer_dense_id.bin   ← u32 array, one per fact row (the "forward edge")
fact_revenue.bin             ← u32 array, one per fact row
customer_to_fact_offsets.bin ← u64 CSR offsets (like forward.offsets.bin)
customer_to_fact_ids.bin     ← u32 CSR peers (like forward.peers.bin)
```

This is Knight Bus's snapshot structure, applied to relational foreign keys
instead of graph edges.

### Why This Works

The same reasons Knight Bus's CSR beats Neo4j:

- Dense IDs turn foreign key resolution into array indexing instead of hash probing
- Contiguous arrays mean sequential CPU cache line reads instead of random access
- mmap means the OS pages in only the dimension columns and fact ranges you touch
- Immutability means no lock contention, no MVCC, no GC
- The "join" is pre-resolved at build time — query time just reads the answer

---

## Innovation 2: Aggregate Stripes

### The OLAP Problem

GROUP BY is the most common OLAP operation:

```sql
SELECT product_category, COUNT(*), SUM(amount), AVG(price)
FROM orders
GROUP BY product_category
```

What the engine does:

```
for each row:
    category = orders.product_category[row]
    hash(category)                              ← compute hash
    bucket = hash_table.probe_or_insert(hash)   ← random memory access
    bucket.count += 1                           ← update in-place
    bucket.sum_amount += amount                 ← update in-place
```

With high cardinality (millions of distinct categories), the hash table
becomes large, cold, and cache-unfriendly. Every probe is a potential L3
miss. This is the same scatter-gather pathology as Neo4j traversing a
linked list of relationships — you are chasing hash buckets through
non-contiguous memory.

### The Structural Parallel

A group IS a node. Its member rows ARE its peers.

```
Knight Bus:     node_id  → peers[offsets[id]..offsets[id+1]]     = neighbors
Aggregate CSR:  group_id → rows[group_offsets[id]..group_offsets[id+1]] = member rows
```

The structures are identical. GROUP BY membership is adjacency.

### The Innovation

Pre-sort data by common GROUP BY columns and build CSR-style offset arrays
for each one. Pre-compute common aggregates as flat arrays.

**At build time:**

```
1. Sort all rows by product_category
     row indices: [row_42, row_7, row_99, ...]  (sorted by category)

2. Assign dense category IDs
     "electronics" → 0
     "furniture"   → 1
     ...

3. Build CSR offsets for category grouping
     category_offsets.bin: [0, 15000, 28000, ...]
       "electronics" owns rows 0..15000
       "furniture" owns rows 15000..28000

4. Pre-compute aggregates as flat arrays
     category_count.bin:      [15000, 13000, ...]
     category_sum_amount.bin: [4500000, 2100000, ...]
     category_min_price.bin:  [3.99, 12.50, ...]
     category_max_price.bin:  [2999.99, 8500.00, ...]
```

**At query time:**

```
SELECT product_category, COUNT(*), SUM(amount) GROUP BY product_category

becomes:

for cat_id in 0..category_count:
    count  = category_count[cat_id]       // sequential array read
    sum    = category_sum_amount[cat_id]  // sequential array read
    name   = category_name[cat_id]        // sequential array read
```

No hash table. No hash probing. Pure sequential reads through contiguous
arrays.

**For ad-hoc aggregates not pre-computed:**

```
SELECT product_category, STDDEV(price) GROUP BY product_category

  start = category_offsets[cat_id]
  end   = category_offsets[cat_id + 1]
  rows  = sorted_data[start..end]     // contiguous slice — like peers[start..end]
  stddev = compute_over(rows)
```

Even the fallback path is a contiguous slice scan — no scatter-gather.

### Multi-Dimensional: Multiple Group CSRs

Just as Knight Bus builds both forward CSR and reverse CSR, build one
offsets+values pair per commonly-queried GROUP BY column:

```
by_category_offsets.bin   + by_category_row_ids.bin
by_region_offsets.bin     + by_region_row_ids.bin
by_date_month_offsets.bin + by_date_month_row_ids.bin
```

Each is an independent CSR. Each makes its GROUP BY dimension O(1).
This is dual CSR generalized to N dimensions.

### Why This Works

Knight Bus's `offsets[id]..offsets[id+1]` turns "find all neighbors"
into "read a contiguous slice." The same structure turns "find all rows
in a group" into "read a contiguous slice."

The pre-computed aggregate arrays are analogous to pre-computing the
neighbor count per node — information that exists in the offsets array
implicitly (`offsets[id+1] - offsets[id]` = neighbor count) but is
made explicit for faster access.

---

## Innovation 3: Walk-Aggregate Snapshots

### The OLAP Problem That Nobody Has Solved

The hardest analytical query pattern combines graph traversal with
aggregation:

```
"For each microservice, what is the total error count of all services
 within 3 hops of its dependency graph?"

"For each account, what is the sum of transaction amounts across all
 accounts within 2 hops of its transfer network?"

"For each gene, how many associated diseases are reachable within
 4 hops of the protein interaction network?"
```

These queries require:
1. Graph traversal (find k-hop neighbors)
2. Property lookup (read attributes of reached nodes)
3. Aggregation (sum, count, min, max over those attributes)

Today's landscape:

| Engine | Graph Traversal | Aggregation | Combined |
| --- | --- | --- | --- |
| Neo4j | Slow (pointer chase) | Slow (not columnar) | Very slow |
| PostgreSQL | Recursive CTE (slow, memory-hungry) | Fast (columnar) | Slow overall |
| ClickHouse/DuckDB | Not supported natively | Very fast | Can't do it |
| Knight Bus (current) | Very fast (CSR) | Not supported | Can't do it |

Nobody does both well. The graph engines can't aggregate efficiently.
The OLAP engines can't traverse efficiently. And piping data between
them loses freshness and adds pipeline complexity.

### The Structural Parallel

Knight Bus already solves the traversal part. The code in `graph.rs:88-116`:

```rust
pub fn collect_neighbors_within_hops<F>(
    seed_dense_id: u32,
    hops: HopCount,
    mut neighbors_for_id: F,
) -> Vec<u32>
where
    F: FnMut(u32) -> Vec<u32>,
{
    let mut deduped_ids = BTreeSet::new();
    for dense_id in neighbors_for_id(seed_dense_id) {
        if dense_id != seed_dense_id {
            deduped_ids.insert(dense_id);
        }
    }
    if hops == HopCount::Two {
        let one_hop_frontier = deduped_ids.iter().copied().collect::<Vec<_>>();
        for direct_neighbor_id in one_hop_frontier {
            for dense_id in neighbors_for_id(direct_neighbor_id) {
                if dense_id != seed_dense_id {
                    deduped_ids.insert(dense_id);
                }
            }
        }
    }
    deduped_ids.into_iter().collect()
}
```

This function already returns all dense IDs within k hops. The only
missing step is: look up a property for each reached ID, and aggregate.

### The Innovation

Pre-compute walk-aggregate results at build time. Store them as flat
arrays indexed by dense node ID. Query time becomes a single array lookup.

**At build time:**

```
For each (direction, hop_depth, property, aggregate_function):

  For each node dense_id in 0..node_count:
    reached_ids = collect_neighbors_within_hops(dense_id, hop_depth, direction)
    values      = reached_ids.map(|id| property_column[id])
    result      = aggregate_function(values)    // SUM, COUNT, MIN, MAX, AVG

  Store as: walk_agg_{direction}_{hops}_{property}_{function}.bin
    → flat array, indexed by dense_id
```

**Concrete example:**

```
Build:
  For "forward, 2-hop, error_count, SUM":
    node 0: 2-hop forward neighbors = [3, 7, 12]
            error_counts = [5, 0, 3]
            SUM = 8
    node 1: 2-hop forward neighbors = [0, 4]
            error_counts = [2, 1]
            SUM = 3
    ...

  walk_agg_forward_2_error_count_sum.bin: [8, 3, ...]

Query:
  "Total error count within 2 hops of service X"
  = walk_agg_forward_2_error_count_sum[resolve_dense_id("service_X")]
  = single array read. O(1).
```

**The query-time code would be:**

```rust
fn query_walk_aggregate(
    &self,
    entity_key: &NodeKey,
    direction: WalkDirection,
    hops: HopCount,
    property: &str,
    aggregate: AggregateFunction,
) -> Result<f64, KnightBusError> {
    let dense_id = self.resolve_dense_id(entity_key)?;
    let agg_array = self.walk_aggregate_mmap(direction, hops, property, aggregate)?;
    Ok(read_f64_from_mmap(agg_array, dense_id.get() as usize))
}
```

Binary search for the key. One array read for the answer. Done.

### Snapshot File Layout

```
manifest.json
forward.offsets.bin              ← existing Knight Bus CSR
forward.peers.bin                ← existing Knight Bus CSR
reverse.offsets.bin              ← existing Knight Bus CSR
reverse.peers.bin                ← existing Knight Bus CSR
node_table.bin                   ← existing Knight Bus node table
strings.bin                      ← existing Knight Bus strings
key_index.bin                    ← existing Knight Bus key index
prop_error_count.bin             ← node property column (u64 per node)
prop_cost.bin                    ← node property column (f64 per node)
walk_agg_fwd_1_error_count_sum.bin   ← pre-computed: one f64 per node
walk_agg_fwd_1_error_count_count.bin ← pre-computed: one u64 per node
walk_agg_fwd_2_error_count_sum.bin   ← pre-computed: one f64 per node
walk_agg_rev_1_cost_sum.bin          ← pre-computed: one f64 per node
walk_agg_rev_2_cost_max.bin          ← pre-computed: one f64 per node
```

Each walk-aggregate file is the same size: `node_count * 8 bytes`.
All mmap'd. All immutable.

### Why This Works

This is the Knight Bus pattern taken to its logical extreme:

- Knight Bus pre-compiles adjacency so traversal is O(1)
- Walk-Aggregate Snapshots pre-compile traversal + aggregation so the
  combined analytical query is O(1)

The build is expensive — you walk every node's neighborhood and compute
aggregates. But:

- Each node's walk is independent → embarrassingly parallelizable
- Knight Bus's external merge sort pattern handles memory budgets
- The result is immutable → mmap-friendly, no GC, no locks
- Query time is a single array read regardless of graph size

The trade-off is the same one Knight Bus already makes: you sacrifice
write capability and ad-hoc flexibility for extreme read performance
on pre-defined analytical questions. For OLAP workloads where the
questions are known in advance (dashboards, monitoring, reporting),
this trade-off is perfect.

---

## Summary: The Pattern Across All Three

| | Neo4j Problem | Knight Bus Solution | OLAP Problem | OLAP Innovation |
| --- | --- | --- | --- | --- |
| **1** | Pointer-chase to find neighbors | CSR: offsets + peers arrays | Hash-probe to resolve foreign keys | Offset-Mapped Joins: dense IDs + attribute arrays |
| **2** | Linked-list scan for reverse edges | Dual CSR: reverse offsets + peers | Hash-table for GROUP BY aggregation | Aggregate Stripes: group offsets + pre-computed agg arrays |
| **3** | Can't combine traversal + analytics | Fast CSR walk | Can't combine graph traversal + OLAP aggregation | Walk-Aggregate Snapshots: pre-computed walk results as flat arrays |

The structural insight is the same in every case:

**If you know the question at build time, you can make the answer a
contiguous array read at query time.**

Neo4j doesn't know the question at build time — it stores a general-purpose
graph and discovers paths dynamically. Knight Bus knows the question (which
nodes are adjacent?) and pre-compiles the answer into CSR arrays.

OLAP engines don't know the question at build time — they store general-purpose
columnar data and compute joins/aggregates dynamically. These three innovations
know the question and pre-compile the answer into flat arrays.

The price is always the same: immutability, a build phase, and limited
flexibility. The reward is always the same: queries collapse from dynamic
discovery into static arithmetic.
