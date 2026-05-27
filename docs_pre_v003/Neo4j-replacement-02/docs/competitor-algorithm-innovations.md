# Competitor Algorithm Analysis & Knight Bus Storage Innovations

> **Method:** Read the actual source code of Neo4j, DuckDB, ClickHouse, Apache AGE,
> Memgraph, ArangoDB, and JanusGraph. For each algorithm or storage pattern found,
> reason from first principles how the Knight Bus pattern (pre-compile at build time
> so query is a contiguous array read) would solve the same problem better.
>
> **The Knight Bus Insight:** If you know the question at build time, you can make
> the answer a contiguous array read at query time. Every innovation below applies
> this single principle to a specific competitor algorithm.

---

## Table of Contents

1. [Neo4j: Linked-List Relationship Chains → CSR Offset Arrays](#1-neo4j-linked-list-relationship-chains)
2. [Neo4j: Relationship Group Records → Type-Partitioned CSR](#2-neo4j-relationship-group-records)
3. [Neo4j: Property Chain Linked Lists → Columnar Property Stripes](#3-neo4j-property-chain-linked-lists)
4. [Neo4j: Dynamic Label Records → Bitmap Label Planes](#4-neo4j-dynamic-label-records)
5. [Neo4j GDS: Pregel PageRank → Pre-Materialized Rank Vectors](#5-neo4j-gds-pregel-pagerank)
6. [Neo4j GDS: Louvain Community Detection → Static Community Partition Arrays](#6-neo4j-gds-louvain-community-detection)
7. [Neo4j GDS: Label Propagation → Pre-Baked Label Arrays](#7-neo4j-gds-label-propagation)
8. [Neo4j GDS: CSRGraphStore Re-Import → Zero-Copy Native CSR](#8-neo4j-gds-csrgraphstore-re-import)
9. [DuckDB: Hash Join → Offset-Mapped Dense Joins](#9-duckdb-hash-join)
10. [DuckDB: Perfect Hash Join → Native Dense-ID Joins](#10-duckdb-perfect-hash-join)
11. [DuckDB: Perfect Hash Aggregate → Pre-Computed Group Stripes](#11-duckdb-perfect-hash-aggregate)
12. [DuckDB: Vectorized Execution → Snapshot-Aligned Column Batches](#12-duckdb-vectorized-execution)
13. [ClickHouse: MergeTree Sorted Parts → Build-Time Sorted CSR Parts](#13-clickhouse-mergetree-sorted-parts)
14. [ClickHouse: Index Granularity & Mark Files → Offset-Indexed Ranges](#14-clickhouse-index-granularity)
15. [ClickHouse: Background Merge/Compaction → Snapshot Rebuild as Merge](#15-clickhouse-background-merge)
16. [Apache AGE: VLE DFS with Hash Table → CSR BFS/DFS with Bitmap](#16-apache-age-vle-dfs)
17. [Apache AGE: Global Graph Context Hash Tables → Native CSR](#17-apache-age-global-graph-context)
18. [Apache AGE: Edge Property Matching → Pre-Filtered Edge Bitmaps](#18-apache-age-edge-property-matching)
19. [Memgraph: In-Memory Vertex Adjacency Vectors → Flat CSR](#19-memgraph-vertex-adjacency-vectors)
20. [Memgraph: MVCC Delta Chains → Immutable Snapshots](#20-memgraph-mvcc-delta-chains)
21. [ArangoDB: RocksDB Key-Value Graph Traversal → CSR Bypass](#21-arangodb-rocksdb-traversal)
22. [JanusGraph: Wide-Row Edge Serialization → Dense CSR](#22-janusgraph-wide-row-serialization)
23. [Cross-Cutting: Graph + OLAP Hybrid Queries → Walk-Aggregate Snapshots](#23-cross-cutting-walk-aggregate)
24. [Cross-Cutting: Schema-Free Property Access → Typed Column Arrays](#24-cross-cutting-schema-free-properties)

---

## 1. Neo4j: Linked-List Relationship Chains → CSR Offset Arrays {#1-neo4j-linked-list-relationship-chains}

### What the competitor does

**Source:** `neo4j-src/community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/format/standard/RelationshipRecordFormat.java:31-35`

```java
// in_use(byte)+first_node(int)+second_node(int)+rel_type(int)+
// first_prev_rel_id(int)+first_next_rel_id+second_prev_rel_id(int)+
// second_next_rel_id+next_prop_id(int)+first-in-chain-markers(1)
public static final int RECORD_SIZE = 34;
```

Each relationship is a 34-byte fixed record containing **four linked-list pointers** (firstPrevRel, firstNextRel, secondPrevRel, secondNextRel). To find all relationships of a node, Neo4j reads the node's `nextRel` pointer (from NodeRecordFormat.java:31, 15-byte record), then walks the linked list, reading 34 bytes per hop, chasing a pointer each time.

**The pain:** To find N neighbors of a node, Neo4j does N random reads of 34 bytes each, following chain pointers. Each pointer-chase is a potential cache miss. The doubly-linked structure exists purely to support O(1) relationship deletion in a mutable store.

### Knight Bus innovation

**Already implemented** in `src/snapshot.rs` and `src/graph.rs`. CSR offsets array + dense peers array. `offsets[dense_id]..offsets[dense_id+1]` gives a contiguous `&[u32]` slice of all neighbors. Zero pointer-chasing, linear memory scan.

**Key difference from Neo4j:** Neo4j pays 34 bytes × N_relationships of random I/O per traversal. Knight Bus pays one offset lookup (8 bytes) + one contiguous read of 4 bytes × degree. For a node with degree 1000, Neo4j does 1000 random 34-byte reads (34KB scattered); Knight Bus does one 4KB contiguous read.

---

## 2. Neo4j: Relationship Group Records → Type-Partitioned CSR {#2-neo4j-relationship-group-records}

### What the competitor does

**Source:** `neo4j-src/community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/format/standard/RelationshipGroupRecordFormat.java:31-36`

```java
/* Record layout
 *
 * [type+inUse+highbits,next,firstOut,firstIn,firstLoop,owningNode] = 25B
 *
 * One record holds first relationship links (out,in,loop) to relationships
 * for one type for one entity.
 */
public static final int RECORD_SIZE = 25;
```

When a node becomes "dense" (exceeds a relationship threshold, flagged by `extra & 0x1` in NodeRecordFormat.java:77), Neo4j switches from a single relationship chain to a **linked list of RelationshipGroupRecords**, one per relationship type. Each group record is 25 bytes containing three chain heads: firstOut, firstIn, firstLoop. To find all `:KNOWS` relationships, you walk the group chain until you find type=KNOWS, then walk that sub-chain.

**The pain:** Two levels of linked-list traversal. First walk groups to find the type, then walk relationships within that type. Supernodes with many types create long group chains.

### Knight Bus innovation: Type-Partitioned CSR

At snapshot build time, for each (node, relationship_type) pair, emit a separate CSR:

```
type_offsets[type_id][dense_id] → start position in type_peers[type_id]
type_peers[type_id][start..end] → neighbors of that type
```

Or more compactly, a single CSR with a type-offset index:

```
// Build-time: sort edges by (source_node, type), then build offsets
// Query: "all KNOWS neighbors of node X"
//   type_start = type_csr_offsets[KNOWS_TYPE_ID][dense_id]
//   type_end   = type_csr_offsets[KNOWS_TYPE_ID][dense_id + 1]
//   neighbors  = type_csr_peers[KNOWS_TYPE_ID][type_start..type_end]
```

**Result:** Single contiguous read. No group chain walking. The "dense node" concept disappears entirely — all nodes are dense by default because CSR handles any degree with a single offset lookup.

**Comparison:** Neo4j needs the 25-byte group records as an optimization for supernodes. Knight Bus doesn't need any special supernode handling because CSR inherently handles variable-degree nodes through the offsets array. A node with degree 1 and a node with degree 10,000,000 use the same access pattern: `offsets[id]..offsets[id+1]`.

---

## 3. Neo4j: Property Chain Linked Lists → Columnar Property Stripes {#3-neo4j-property-chain-linked-lists}

### What the competitor does

**Source:** `neo4j-src/community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/format/standard/PropertyRecordFormat.java:37-39`

```java
public static final int RECORD_SIZE =
    1 /*next and prev high bits*/ + 4 /*next*/ + 4 /*prev*/ + DEFAULT_PAYLOAD_SIZE /*property blocks*/;
// = 41
```

Properties are stored as a **doubly-linked list** of 41-byte records. Each record contains prev/next pointers and a 32-byte payload that can hold up to 4 property blocks (8 bytes each). Property blocks encode type + key + value inline for small values, or point to a DynamicStringStore/DynamicArrayStore for large values.

Reading all properties of a node: follow `nextProp` from the node record, then walk the chain reading 41 bytes per hop, parsing property blocks inline.

**The pain:** Properties are interleaved across the chain. To read property "name" of 1000 nodes, Neo4j does 1000+ random reads into the property store, following chains. There's no columnar access.

### Knight Bus innovation: Columnar Property Stripes

Already partially implemented in `src/snapshot.rs` (node_table + string_table split). Extend to full columnar property storage:

```
// Build-time: for each property key, create a dense array
// property_stripe_name[dense_id]  → offset into string_table
// property_stripe_age[dense_id]   → u32 (inline numeric)
// property_stripe_score[dense_id] → f64 (inline float)

// Query: "name of node X"
//   string_offset = property_stripe_name[dense_id]
//   name = &string_table[string_offset..next_offset]

// Query: "average age of all nodes"
//   sum(property_stripe_age[0..N]) / N  ← pure sequential scan, SIMD-friendly
```

**Result:** Each property becomes a dense column. Analytical queries (SUM, AVG, filtering) become sequential scans over contiguous arrays. This is the same insight as DuckDB's columnar storage, but applied to graph nodes.

**Comparison:** Neo4j's property chain stores all properties of one node together (row-oriented within the chain). Knight Bus stores all values of one property together across nodes (column-oriented). For OLAP queries touching one property across many nodes, this is orders of magnitude faster.

---

## 4. Neo4j: Dynamic Label Records → Bitmap Label Planes {#4-neo4j-dynamic-label-records}

### What the competitor does

**Source:** `neo4j-src/community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/record/NodeRecord.java:34-38`

```java
private long nextRel;
private long labels;   // Label bitmap or pointer to dynamic records
private List<DynamicRecord> dynamicLabelRecords;  // Overflow labels
private boolean isLight;
private boolean dense;
```

Labels are stored as a 40-bit field in the node record. When a node has more than ~5 labels, the field becomes a pointer to a DynamicArrayStore chain, adding another level of indirection.

**The pain:** Label-based filtering requires reading each node record (15 bytes minimum, plus overflow chains) to check labels. "Find all :Person nodes" is a full scan of the node store.

### Knight Bus innovation: Bitmap Label Planes

```
// Build-time: for each label, create a bitset
// label_bitmap_Person[dense_id / 64] → u64 bitmask
// label_bitmap_Company[dense_id / 64] → u64 bitmask

// Query: "all Person nodes"
//   for word in label_bitmap_Person:
//     for set_bit in word: yield dense_id

// Query: "all nodes that are both Person AND Employee"
//   label_bitmap_Person[i] & label_bitmap_Employee[i]  ← bitwise AND
```

**Result:** Label filtering becomes bitwise operations on dense bitmaps. Intersection/union of labels is a single CPU instruction per 64 nodes. No record reading, no overflow chains.

**Space:** For N nodes with L labels: N×L/8 bytes. For 1M nodes with 10 labels: 1.25 MB total. Neo4j's approach: 15 bytes × 1M = 15 MB minimum (node records alone, not counting label overflow chains).

---

## 5. Neo4j GDS: Pregel PageRank → Pre-Materialized Rank Vectors {#5-neo4j-gds-pregel-pagerank}

### What the competitor does

**Source:** `neo4j-gds-src/algo/src/main/java/org/neo4j/gds/pagerank/PageRankComputation.java:78-98`

```java
public void compute(ComputeContext<C> context, Messages messages) {
    double rank = context.doubleNodeValue(PAGE_RANK);
    double delta = rank;

    if (!context.isInitialSuperstep()) {
        double sum = 0;
        for (var message : messages) {
            sum += message;
        }
        delta = dampingFactor * sum;
        context.setNodeValue(PAGE_RANK, rank + delta);
    }

    if (delta > tolerance || context.isInitialSuperstep()) {
        var degree = degreeFunction.applyAsDouble(context.nodeId());
        if (degree > 0) {
            context.sendToNeighbors(delta / degree);
        }
    } else {
        context.voteToHalt();
    }
}
```

GDS implements PageRank using the Pregel framework: iterative message-passing where each node sends `delta/degree` to all neighbors, accumulates incoming messages with SUM reduction, and converges when delta < tolerance. Each superstep requires a full graph traversal and message aggregation.

**The pain:** PageRank is computed on-demand. Each invocation iterates 20+ supersteps over the entire graph. Even though GDS uses CSR internally (CSRGraphStore), the computation itself is repeated every time. The Pregel framework adds overhead: message serialization, vertex halting protocol, multi-threaded coordination.

### Knight Bus innovation: Pre-Materialized Rank Vectors

```
// Build-time: run PageRank to convergence during snapshot creation
// Store result as a dense array:
// pagerank_vector[dense_id] → f64

// Query: "PageRank of node X"
//   pagerank_vector[dense_id]  ← single f64 read, O(1)

// Query: "top 100 nodes by PageRank"
//   pre-sorted index: pagerank_sorted_ids[0..100]  ← contiguous read
```

**Result:** PageRank query is a single array read. The computation cost is amortized into snapshot build time. Since Knight Bus already rebuilds snapshots periodically (via external merge sort in `src/low_ram.rs`), adding PageRank computation to the build pipeline is natural.

**Extension:** Pre-materialize multiple centrality measures in parallel during build:
- `pagerank_vector[dense_id] → f64`
- `betweenness_vector[dense_id] → f64`  
- `closeness_vector[dense_id] → f64`

Each becomes a single column stripe, queryable in O(1).

**Comparison:** GDS runs PageRank as an algorithm that returns results. Knight Bus would bake PageRank INTO the storage format so there's nothing to "run" — the answer is pre-computed.

---

## 6. Neo4j GDS: Louvain Community Detection → Static Community Partition Arrays {#6-neo4j-gds-louvain-community-detection}

### What the competitor does

**Source:** `neo4j-gds-src/algo/src/main/java/org/neo4j/gds/louvain/Louvain.java:104-131`

```java
public LouvainResult compute() {
    Graph workingGraph = rootGraph;
    NodePropertyValues nextSeedingValues = seedingValues;
    long oldNodeCount = rootGraph.nodeCount();
    
    for (ranLevels = 0; ranLevels < maxLevels; ranLevels++) {
        var modularityOptimizationResult = runModularityOptimization(
            workingGraph, nextSeedingValues
        );
        modularities[ranLevels] = modularityOptimizationResult.modularity();
        dendrogramManager.prepareNextLevel(ranLevels);
        long maxCommunityId = buildDendrogram(
            workingGraph, ranLevels, modularityOptimizationResult
        );
        workingGraph = summarizeGraph(workingGraph, modularityOptimizationResult, maxCommunityId);
        // ... convergence check
    }
}
```

Louvain runs multi-level community detection: (1) optimize modularity by reassigning nodes to communities, (2) build a dendrogram, (3) summarize the graph by collapsing communities into super-nodes, (4) repeat. Each level creates a new graph copy.

**The pain:** Expensive computation at query time. Multiple graph copies. The dendrogram (hierarchical community assignment) is computed fresh each time.

### Knight Bus innovation: Static Community Partition Arrays

```
// Build-time: run Louvain to convergence during snapshot creation
// community_id[dense_id] → u32 (which community this node belongs to)
// community_level_0[dense_id] → u32 (finest granularity)
// community_level_1[dense_id] → u32 (coarser)
// community_level_2[dense_id] → u32 (coarsest)

// Query: "which community is node X in?"
//   community_id[dense_id]  ← single u32 read

// Query: "all nodes in the same community as X"
//   Build a community CSR at build time:
//   community_members_offsets[community_id] → start
//   community_members[start..end] → list of dense_ids
```

**Result:** Community membership is a single array lookup. "All members of community C" is the same CSR pattern as adjacency. The dendrogram levels become additional column stripes.

**The key insight:** Community detection IS a partitioning operation. The output is a mapping node→community_id. That mapping is exactly a dense array. Store it as one.

---

## 7. Neo4j GDS: Label Propagation → Pre-Baked Label Arrays {#7-neo4j-gds-label-propagation}

### What the competitor does

**Source:** `neo4j-gds-src/algo/src/main/java/org/neo4j/gds/labelpropagation/LabelPropagation.java:99-125`

```java
public LabelPropagationResult compute() {
    if (labels == null || labels.size() != nodeCount) {
        labels = HugeLongArray.newArray(nodeCount);
    }
    long ranIterations = 0L;
    boolean didConverge = false;
    List<StepRunner> stepRunners = stepRunners();
    while (ranIterations < parameters.maxIterations()) {
        RunWithConcurrency.builder()
            .concurrency(parameters.concurrency())
            .tasks(stepRunners)
            .executor(executor)
            .run();
        ++ranIterations;
        // ... convergence check
    }
}
```

LabelPropagation iteratively assigns each node the label most common among its neighbors, using `HugeLongArray` for label storage. Each iteration requires scanning all nodes and their neighborhoods.

**The pain:** Same as Louvain — on-demand computation, multiple iterations over the full graph.

### Knight Bus innovation: Pre-Baked Label Arrays

Identical pattern to community detection. At snapshot build time, run label propagation to convergence and store:

```
propagated_label[dense_id] → u64
```

One array read per query. The `HugeLongArray` that LabelPropagation already uses is essentially what we'd store — we just persist it into the snapshot instead of computing it on-demand.

---

## 8. Neo4j GDS: CSRGraphStore Re-Import → Zero-Copy Native CSR {#8-neo4j-gds-csrgraphstore-re-import}

### What the competitor does

**Source:** `neo4j-gds-src/core/src/main/java/org/neo4j/gds/core/loading/CSRGraphStore.java:80`

```java
public final class CSRGraphStore implements GraphStore {
```

GDS already converts Neo4j's linked-list format to CSR for algorithm execution. But this requires:
1. Reading all Neo4j records (linked-list traversal)
2. Building an IdMap (original ID → dense ID mapping)
3. Constructing CSR adjacency in memory
4. Running the algorithm
5. Writing results back to Neo4j

This re-import happens every time `gds.graph.project()` is called.

**The pain:** The CSR is built as a transient in-memory copy. It's not persisted. Every algorithm invocation pays the import cost. The data lives in two formats simultaneously (Neo4j records on disk + CSR in memory).

### Knight Bus innovation: Zero-Copy Native CSR

Knight Bus IS the persistent CSR. There's no import/export step. The on-disk format IS the CSR:

```
// snapshot.rs already does this:
// - node_table: dense array of node metadata
// - offsets: u64 array for CSR
// - peers: u32 array of neighbor dense_ids
// - string_table: variable-length string data

// Memory-mapped via runtime.rs:326-327:
// offsets[dense_id]..offsets[dense_id+1] → neighbor range
```

**Result:** GDS pays O(V + E) to import. Knight Bus pays O(1) to start — the file IS the data structure. Algorithm implementations can directly reference `&offsets[..]` and `&peers[..]` via memory mapping. No copy.

**Comparison:** Neo4j GDS's CSRGraphStore is validation that CSR is the right format for graph algorithms. They just made it transient. Knight Bus makes it persistent.

---

## 9. DuckDB: Hash Join → Offset-Mapped Dense Joins {#9-duckdb-hash-join}

### What the competitor does

**Source:** `duckdb-src/src/execution/operator/join/physical_hash_join.cpp:42-96`

```cpp
PhysicalHashJoin::PhysicalHashJoin(/* ... */)
    : PhysicalComparisonJoin(physical_plan, op, PhysicalOperatorType::HASH_JOIN,
                             std::move(conds), join_type, estimated_cardinality),
      delim_types(std::move(delim_types)) {
    // Build side (RHS): materialized into a JoinHashTable
    // Probe side (LHS): scanned and hashed, probed against build side
    // Radix partitioning for cache efficiency
}
```

DuckDB builds a hash table from the right side of the join, then probes it with each row from the left side. Uses radix partitioning for cache efficiency. The hash table stores keys + payloads, handles collisions, and supports various join types (inner, left, right, anti, semi, mark).

**The pain:** Hash table construction is O(|build|), probing is O(|probe|) with hash computation + collision resolution per row. Memory consumption is proportional to the build side. Hash collisions cause unpredictable branching.

### Knight Bus innovation: Offset-Mapped Dense Joins

When join keys are foreign-key relationships (which they usually are in star schemas), treat them as graph edges. Pre-resolve at build time:

```
// Star schema: fact_orders.customer_id → dim_customers.id
// At build time: assign dense IDs to both tables
// fact_customer_dense[fact_row] → u32 (dense customer ID)
// dim_data[dim_dense_id] → customer data

// Query: SELECT customer.name, order.amount FROM ...
//   for each fact_row:
//     customer_dense = fact_customer_dense[fact_row]
//     name = dim_name_stripe[customer_dense]  ← direct array index
```

**Result:** Join becomes array indexing. No hash table, no collision resolution, no hash computation. O(1) per row instead of O(1) amortized with hash overhead.

**When this applies:** Foreign-key joins where the dimension table fits in memory (the common case for star schemas). This is exactly the pattern from `docs/olap-innovations.md` Innovation #1.

---

## 10. DuckDB: Perfect Hash Join → Native Dense-ID Joins {#10-duckdb-perfect-hash-join}

### What the competitor does

**Source:** `duckdb-src/src/execution/operator/join/perfect_hash_join_executor.cpp:70-131`

```cpp
bool PerfectHashJoinExecutor::CanDoPerfectHashJoin(
    const PhysicalHashJoin &op, const Value &min, const Value &max) {
    // Only for inner joins with one integer equality condition
    // Build range must be < MAX_BUILD_SIZE (1048576)
    if (op.join_type != JoinType::INNER || op.conditions.size() != 1 ||
        !TypeIsInteger(key_type.InternalType())) {
        return false;
    }
    static constexpr idx_t MAX_BUILD_SIZE = 1048576;
    if (build_range > Hugeint::Convert(MAX_BUILD_SIZE)) {
        return false;
    }
    // ...
}
```

DuckDB already recognizes that when build keys are integers with a small range, a **direct-indexed array** (perfect hash) beats a hash table. It allocates an array of size `max - min + 1` and maps `key → array[key - min]`. This eliminates hash computation and collision handling.

**The pain:** DuckDB discovers this optimization at query time by checking min/max statistics. It's limited to 1M range. It only works for single integer equality conditions. The "perfect hash table" is still built per-query.

### Knight Bus innovation: Native Dense-ID Joins

Knight Bus's dense ID assignment IS the perfect hash, but:
- No 1M limit — all nodes get dense IDs regardless of count
- No runtime detection needed — the format guarantees dense IDs exist
- No per-query construction — the mapping is baked into the snapshot
- Works for all join patterns, not just single-integer equality

```
// DuckDB discovers at query time: "hey, these IDs are 0..999999, let's use an array!"
// Knight Bus: "all IDs ARE 0..N-1 by construction. Array access is the default."

// DuckDB's CanDoPerfectHashJoin is a runtime check.
// Knight Bus's snapshot format makes the check unnecessary — it's always true.
```

**Key insight from code:** DuckDB's `MAX_BUILD_SIZE = 1048576` is a practical limit because they allocate the array at query time. Knight Bus doesn't have this limit because the dense ID space is allocated once at build time and persists.

---

## 11. DuckDB: Perfect Hash Aggregate → Pre-Computed Group Stripes {#11-duckdb-perfect-hash-aggregate}

### What the competitor does

**Source:** `duckdb-src/src/execution/operator/aggregate/physical_perfecthash_aggregate.cpp:10-78`

```cpp
PhysicalPerfectHashAggregate::PhysicalPerfectHashAggregate(
    /* ... */
    vector<unique_ptr<Expression>> groups_p,
    const vector<unique_ptr<BaseStatistics>> &group_stats,
    vector<idx_t> required_bits_p, /* ... */) {
    // When GROUP BY keys have small-range integer values,
    // use direct array indexing instead of hash table
    group_minima.reserve(group_stats.size());
    for (auto &stats : group_stats) {
        group_minima.push_back(NumericStats::Min(nstats));
    }
}
```

DuckDB's `PhysicalPerfectHashAggregate` creates a flat array indexed by group key value. `required_bits` determines how many bits each group key needs; the aggregate state array is indexed by the combined group key (using bit-shifting for multi-column groups). Same insight as perfect hash join: small-range integer keys → direct array.

**The pain:** Still computed per-query. The aggregate states must be initialized, filled, and discarded for each GROUP BY execution. Multi-column groups require bit-shifting math at runtime.

### Knight Bus innovation: Pre-Computed Group Stripes

At build time, pre-compute common aggregates as dense arrays:

```
// Build-time:
// For each GROUP BY pattern that's commonly queried:
// 1. Sort data by group columns
// 2. Build group CSR: group_offsets[group_dense_id] → member range
// 3. Pre-compute aggregates:
//    group_sum_amount[group_dense_id] → f64
//    group_count[group_dense_id] → u64
//    group_min_price[group_dense_id] → f64
//    group_max_price[group_dense_id] → f64

// Query: "SELECT category, SUM(amount) GROUP BY category"
//   group_sum_amount[category_dense_id]  ← single read per group
```

**Result:** GROUP BY becomes a lookup table read. DuckDB's perfect hash aggregate computes the answer at query time with O(N) work; Knight Bus pre-computes it at build time so query time is O(G) where G = number of groups (typically << N).

**This is Innovation #2 from olap-innovations.md** — Aggregate Stripes — now derived directly from DuckDB's own source code showing they already know array indexing beats hash tables.

---

## 12. DuckDB: Vectorized Execution → Snapshot-Aligned Column Batches {#12-duckdb-vectorized-execution}

### What the competitor does

DuckDB processes data in vectorized batches of 2048 rows. Each `DataChunk` contains multiple `Vector` columns. Operators pull chunks through the pipeline, applying SIMD-friendly operations to contiguous column data.

**The pain:** The vectorized engine achieves excellent throughput for scans and aggregation, but join operators break the vectorized flow — hash table probes are inherently scalar (each key maps to a different bucket). The perfect hash join partially recovers vectorization by making the lookup a simple array offset.

### Knight Bus innovation: Snapshot-Aligned Column Batches

Since Knight Bus controls the physical layout, align the column stripes to match the vectorized processing width:

```
// Build-time: pad column stripes to 2048-row boundaries
// Align property arrays to cache-line boundaries (64 bytes)
// Store column data contiguously per 2048-row chunk

// Query-time: memory-map the stripe, read 2048 values directly
// No "filling a DataChunk from row-oriented records" step
// The on-disk format IS the DataChunk layout
```

**Result:** DuckDB builds DataChunks by materializing from its storage format. Knight Bus's storage format IS already columnar and aligned — the memory-mapped array IS the vector.

---

## 13. ClickHouse: MergeTree Sorted Parts → Build-Time Sorted CSR Parts {#13-clickhouse-mergetree-sorted-parts}

### What the competitor does

**Source:** `clickhouse-src/src/Storages/StorageMergeTree.cpp` and `clickhouse-src/src/Storages/MergeTree/MergeTreeDataPartWriterWide.cpp:47-80`

```cpp
Granules getGranulesToWrite(const MergeTreeIndexGranularity &index_granularity,
    size_t block_rows, size_t current_mark, size_t rows_written_in_last_mark) {
    // Divide block into granules (typically 8192 rows each)
    // Each granule gets a mark pointing to its position
}
```

ClickHouse MergeTree stores data as immutable "parts." Each part is sorted by the primary key and split into "granules" (default 8192 rows). Marks record the position of each granule for index lookups. New data creates new parts; background merges combine small parts into larger sorted ones.

**The pain:** The merge process is continuous background work. Parts may not be fully merged at query time, requiring query-time merge of multiple sorted streams. Primary key sorts are one-dimensional — secondary access patterns (GROUP BY a different column) still require full scans.

### Knight Bus innovation: Build-Time Sorted CSR Parts

Apply ClickHouse's sorted-parts concept to graph data:

```
// Build-time: the snapshot IS one fully-merged "part"
// External merge sort (src/low_ram.rs) already produces globally-sorted output
// No background merging needed — each snapshot is complete

// Extension: build multiple sort orders at snapshot time
// sort_by_timestamp: offsets_timestamp[dense_id] → position in timestamp-sorted peers
// sort_by_label: offsets_label[label_id] → position in label-sorted node array

// Query: "neighbors of X sorted by timestamp"
//   Use timestamp-sorted CSR variant instead of default CSR
```

**Key insight from ClickHouse:** Immutable sorted parts + background merge is their way of handling mutations while keeping data sorted. Knight Bus eliminates mutations entirely (immutable snapshots), so there's no need for background merge — the snapshot build IS the merge.

**Comparison:** ClickHouse's `MergeTreeDataPartWriterWide` writes each column to a separate `.bin` file with marks. Knight Bus writes each property to a separate stripe in the snapshot file. Same columnar principle, but Knight Bus's snapshot is always one fully-merged piece.

---

## 14. ClickHouse: Index Granularity & Mark Files → Offset-Indexed Ranges {#14-clickhouse-index-granularity}

### What the competitor does

ClickHouse's mark files record the byte offset of each granule in each column file. To read rows 16384–24576, ClickHouse looks up mark 2 (at granule_size = 8192) and reads from that file offset. Marks provide O(1) positional access into sorted column data.

**The pain:** Mark files are a separate I/O artifact. They must be loaded and parsed before column data can be accessed. Adaptive granularity (variable-size granules) adds complexity.

### Knight Bus innovation: Offset-Indexed Ranges

Knight Bus's CSR offsets array IS the mark file, but built into the data structure:

```
// ClickHouse:
//   mark[granule_id] → byte_offset in column.bin
//   Read column.bin[byte_offset .. byte_offset + granule_size * value_size]

// Knight Bus:
//   offsets[dense_id] → start position in peers array
//   Read peers[start..end]

// Same pattern: an offsets array provides O(1) positional access
// But Knight Bus's offsets are per-entity (per-node), not per-granule
// This gives finer-grained access: individual node neighborhoods
// vs. ClickHouse's granule-level (8192 row) access
```

**Result:** Knight Bus achieves node-level precision in O(1), while ClickHouse achieves granule-level precision. For graph queries ("neighbors of X"), node-level precision eliminates unnecessary data reads entirely.

---

## 15. ClickHouse: Background Merge/Compaction → Snapshot Rebuild as Merge {#15-clickhouse-background-merge}

### What the competitor does

ClickHouse continuously merges small parts into larger ones in the background. This is an LSM-tree-inspired approach: write-optimized (fast inserts create new parts) with read optimization via merging.

**The pain:** Write amplification — data is rewritten multiple times as parts merge. Read amplification — queries may need to read from multiple parts and merge results. Compaction scheduling is complex (choosing which parts to merge, when to merge, size tiering vs. leveled).

### Knight Bus innovation: Snapshot Rebuild as Merge

Knight Bus's external merge sort (`src/low_ram.rs`) IS a single complete merge:

```
// low_ram.rs two-phase approach:
// Phase 1: Sort edges into spill files, resolving string keys to dense IDs
// Phase 2: Merge spill files, emit final CSR

// This IS ClickHouse's merge, but done ONCE at build time
// No ongoing background work
// No write amplification — the snapshot is written once
// No read amplification — there's exactly one "part" (the snapshot)
```

**Comparison:** ClickHouse needs continuous merging because it accepts ongoing writes. Knight Bus accepts a batch of changes, rebuilds the snapshot, and serves the new version. The tradeoff is clear: Knight Bus trades write latency (batch rebuilds) for zero read amplification and zero ongoing merge work.

---

## 16. Apache AGE: VLE DFS with Hash Table → CSR BFS/DFS with Bitmap {#16-apache-age-vle-dfs}

### What the competitor does

**Source:** `age-src/src/backend/utils/adt/age_vle.c:86-92,995-1111`

```c
typedef struct edge_state_entry {
    graphid edge_id;         /* edge id, it is also the hash key */
    bool used_in_path;       /* like visited but more descriptive */
    bool has_been_matched;   /* have we checked for a match */
    bool matched;            /* is it a match */
} edge_state_entry;

// DFS traversal: dfs_find_a_path_between()
while (!(gid_stack_is_empty(edge_stack))) {
    edge_id = gid_stack_peek(edge_stack);
    ese = get_edge_state(vlelctx, edge_id);  // hash_search()
    if (ese->used_in_path) {
        // backtrack: pop from path stack, reset used_in_path
        gid_stack_pop(path_stack);
        ese->used_in_path = false;
    }
    ese->used_in_path = true;
    gid_stack_push(path_stack, edge_id);
    ee = get_edge_entry(vlelctx->ggctx, edge_id);  // hash_search()
    next_vertex_id = get_next_vertex(vlelctx, ee);
    // ... check bounds, add edges
}
```

AGE implements VLE (Variable-Length Edge traversal, i.e., `(a)-[*1..3]->(b)`) using explicit DFS with three stacks (vertex, edge, path). Edge state is tracked in a PostgreSQL hash table (`HASH_ENTER` via `hash_search`). Each step does TWO hash lookups: one for edge state, one for edge→vertex mapping.

**The pain:** Hash table lookups per edge per traversal step. The `edge_state_hashtable` (initialized at 100K entries) grows dynamically. `HASH_ENTER` allocates memory for each new entry. For a graph with 1M edges, the hash table alone consumes significant memory, and each `hash_search` is an unpredictable memory access.

### Knight Bus innovation: CSR BFS/DFS with Bitmap

Replace AGE's hash tables with Knight Bus's CSR + bitmap:

```
// Knight Bus already has CSR for adjacency:
// offsets[dense_id]..offsets[dense_id+1] → neighbor dense_ids

// For VLE, replace the edge_state_hashtable with a flat bitmap:
// edge_visited[edge_dense_id / 64] → u64 bitmask
// Set bit: edge_visited[eid/64] |= (1 << (eid % 64))
// Check: edge_visited[eid/64] & (1 << (eid % 64))

// DFS traversal becomes:
//   neighbors = peers[offsets[v]..offsets[v+1]]  // contiguous read
//   for each neighbor n in neighbors:
//     edge_id = edges[offset + i]  // edge dense ID from CSR
//     if !(edge_visited[edge_id/64] & (1 << (edge_id%64))):
//       push(stack, n)
//       edge_visited[edge_id/64] |= (1 << (edge_id%64))
```

**Result:**
- Neighbor lookup: O(1) CSR offset read + contiguous array scan (vs. AGE's hash_search per edge)
- Edge state check: single bitwise AND (vs. AGE's hash_search with collision resolution)
- Memory: E/8 bytes for the bitmap (vs. AGE's hash table at ~40 bytes per entry with HTAB overhead)
- For 1M edges: bitmap = 125 KB, AGE's hash table = ~40 MB

**Comparison:** AGE's `EDGE_STATE_HTAB_INITIAL_SIZE = 100000` preallocates hash buckets for 100K entries. Knight Bus's bitmap for 100K edges = 12.5 KB. Three orders of magnitude less memory, with deterministic O(1) access instead of amortized O(1) with hash collisions.

---

## 17. Apache AGE: Global Graph Context Hash Tables → Native CSR {#17-apache-age-global-graph-context}

### What the competitor does

**Source:** `age-src/src/backend/utils/adt/age_global_graph.c:104-122`

```c
typedef struct vertex_entry {
    graphid vertex_id;             /* vertex id, hash key */
    ListGraphId *edges_in;         /* List of entering edges */
    ListGraphId *edges_out;        /* List of exiting edges */
    ListGraphId *edges_self;       /* List of selfloop edges */
    Oid vertex_label_table_oid;
    ItemPointerData tid;           /* physical tuple location */
} vertex_entry;

typedef struct edge_entry {
    graphid edge_id;               /* edge id, hash key */
    Oid edge_label_table_oid;
    ItemPointerData tid;
    graphid start_vertex_id;
    graphid end_vertex_id;
} edge_entry;
```

AGE builds a `GRAPH_global_context` by loading all vertices and edges from PostgreSQL tables into TWO hash tables: `vertex_hashtable` (vertex → edge lists) and `edge_hashtable` (edge → endpoints). This is the in-memory graph representation used for all traversals.

**The pain:**
1. Two separate hash tables, each with PostgreSQL HTAB overhead
2. Edge lists (`ListGraphId`) are linked lists within the hash entries — another level of pointer-chasing
3. Loading requires scanning PostgreSQL heap tables and inserting into hash tables
4. `VERTEX_HTAB_INITIAL_SIZE = 10000`, `EDGE_HTAB_INITIAL_SIZE = 10000` — both resize dynamically

### Knight Bus innovation: Native CSR Replaces Both Hash Tables

The vertex_hashtable + edge_hashtable pattern is exactly what CSR replaces:

```
// AGE's vertex_entry.edges_out → Knight Bus's offsets[dense_id]..offsets[dense_id+1]
// AGE's vertex_entry.edges_in  → Knight Bus's rev_offsets[dense_id]..rev_offsets[dense_id+1]
// AGE's edge_entry → Not needed! Edge endpoints are implicit in the CSR structure.

// AGE: hash_search(vertex_hashtable, vertex_id) → vertex_entry → walk edges_out list
// KB:  peers[offsets[dense_id]..offsets[dense_id+1]]  ← contiguous u32 array
```

**Result:** Both hash tables eliminated. AGE's `edges_in/edges_out/edges_self` linked lists become CSR ranges. The `edge_hashtable` becomes unnecessary because edge endpoints are encoded in the CSR structure itself (edge at position `offsets[src] + i` connects `src` to `peers[offsets[src] + i]`).

**Memory comparison for 1M vertices, 10M edges:**
- AGE: ~1M × sizeof(vertex_entry) + 10M × sizeof(edge_entry) + HTAB overhead ≈ 500+ MB
- Knight Bus: (1M+1) × 8 (offsets) + 10M × 4 (peers) ≈ 48 MB

---

## 18. Apache AGE: Edge Property Matching → Pre-Filtered Edge Bitmaps {#18-apache-age-edge-property-matching}

### What the competitor does

**Source:** `age-src/src/backend/utils/adt/age_vle.c:400-474`

```c
// Fast path: if label matched and no property constraints
if (num_edge_property_constraints == 0) {
    return true;
}
// Fetch edge properties (heap_fetch via get_edge_entry_properties())
Datum edge_props_datum = get_edge_entry_properties(ee);
// Compare constraints
if (num_edge_property_constraints == num_edge_properties) {
    // Hash comparison first, then datum_image_eq
    uint32 edge_props_hash = datum_image_hash(edge_props_datum, false, -1);
    if (vlelctx->edge_property_constraint_hash == edge_props_hash) {
        if (datum_image_eq(...)) { return true; }
    }
    return false;
}
// Deep contains check
return agtype_deep_contains(&property_it, &constraint_it, false);
```

AGE evaluates edge property constraints DURING DFS traversal. For each edge candidate, it:
1. Fetches edge properties from the heap (potential I/O)
2. Hashes the properties
3. Compares against the constraint

**The pain:** Property fetching (`get_edge_entry_properties`) is a PostgreSQL heap fetch PER EDGE during traversal. For VLE patterns like `[:KNOWS {since: 2020}*1..5]`, every edge in the traversal frontier triggers a heap fetch + hash + comparison.

### Knight Bus innovation: Pre-Filtered Edge Bitmaps

At build time, pre-compute filter bitmaps for common property predicates:

```
// Build-time: for commonly queried predicates, create bitmaps
// edge_bitmap_knows[edge_dense_id / 64] → u64 (edges with label KNOWS)
// edge_bitmap_since_2020[edge_dense_id / 64] → u64 (edges where since >= 2020)
// edge_property_since[edge_dense_id] → u32 (year, as column stripe)

// Query: (a)-[:KNOWS {since: 2020}*1..3]->(b)
//   During traversal, filter check becomes:
//   if (edge_bitmap_knows[eid/64] & (1 << (eid%64))) &&
//      (edge_property_since[eid] == 2020)

// No heap fetch. No hash computation. Direct array reads.
```

**For dynamic predicates** (arbitrary property constraints not known at build time), store edge properties as column stripes (similar to Innovation #3). The column stripe read is still O(1) per edge — far cheaper than AGE's heap fetch.

---

## 19. Memgraph: In-Memory Vertex Adjacency Vectors → Flat CSR {#19-memgraph-vertex-adjacency-vectors}

### What the competitor does

**Source:** `memgraph-src/src/storage/v2/vertex.hpp:29-66`

```cpp
using EdgeTriple = std::tuple<EdgeTypeId, Vertex*, EdgeRef>;
using Edges = utils::small_vector<EdgeTriple, memory::DbAwareAllocator<EdgeTriple>>;

struct Vertex {
    const Gid gid;
    utils::small_vector<LabelId, memory::DbAwareAllocator<LabelId>> labels;
    Edges in_edges;
    Edges out_edges;
    PropertyStore properties;
    mutable utils::RWSpinLock lock;
    // ...
};
static_assert(sizeof(Vertex) == 80, "...");
```

Memgraph stores each vertex as an 80-byte struct containing `small_vector`s of `EdgeTriple` tuples. Each `EdgeTriple` is `(EdgeTypeId, Vertex*, EdgeRef)` — a pointer to the neighbor vertex plus edge type and edge reference. The `small_vector` stores a few entries inline and heap-allocates for overflow.

**The pain:**
1. **80 bytes per vertex** — wasteful for simple graphs
2. **Pointer-based adjacency** — `Vertex*` pointers chase through memory
3. **Per-vertex lock** (`RWSpinLock`) adds contention
4. **Edge triples are fat** — each stores type + vertex pointer + edge ref, requiring ~24 bytes per edge reference
5. **Small vector** optimization helps for low-degree nodes but heap-allocates for high-degree ones, creating fragmented memory

### Knight Bus innovation: Flat CSR

Replace Memgraph's per-vertex vectors with global CSR arrays:

```
// Memgraph: 80 bytes per vertex + ~24 bytes per edge triple
//   Vertex* → small_vector<(EdgeTypeId, Vertex*, EdgeRef)>
//   Memory scattered across heap, per-vertex locks

// Knight Bus: 8 bytes per vertex (one u64 offset) + 4 bytes per edge (one u32 dense_id)
//   offsets[dense_id] → u64 (position in peers array)
//   peers[position] → u32 (neighbor dense_id)
//   No locks — immutable snapshot

// Memory for 1M vertices, 10M edges:
// Memgraph: 1M × 80 + 10M × ~24 = 320 MB (plus heap fragmentation)
// Knight Bus: 1M × 8 + 10M × 4 = 48 MB (contiguous, no fragmentation)
```

**Result:** 6.7× less memory, zero fragmentation, zero lock contention, and sequential memory access patterns instead of pointer-chasing.

---

## 20. Memgraph: MVCC Delta Chains → Immutable Snapshots {#20-memgraph-mvcc-delta-chains}

### What the competitor does

**Source:** `memgraph-src/src/storage/v2/vertex.hpp:49-65`

```cpp
Delta *delta() const { return delta_.GetPtr(); }
void SetDelta(Delta *d) { delta_.SetPtr(d); }
bool deleted() const { return delta_.Get<kDeletedBit>() != 0; }
```

Memgraph uses MVCC (Multi-Version Concurrency Control) with delta chains. Each vertex/edge has a pointer to a linked list of `Delta` objects representing uncommitted changes. Reading a vertex at a specific transaction snapshot requires walking the delta chain to reconstruct the visible version.

**The pain:** Delta chains add indirection on every read. Even read-only queries must traverse deltas to determine visibility. The `PointerPack<Delta, 2>` packs bits into the pointer for flags, adding decode overhead. Concurrent write transactions create contention on vertex locks.

### Knight Bus innovation: Immutable Snapshots

Knight Bus eliminates MVCC entirely through immutable snapshots:

```
// Memgraph: read vertex → check delta chain → apply/skip deltas → return visible version
// Knight Bus: read vertex → return vertex (it IS the only version)

// No delta chains. No transaction visibility checks.
// New data → build new snapshot → atomic swap of memory-mapped file
// Readers see a consistent snapshot without any locking
```

**Result:** Zero overhead per read. No delta traversal, no lock acquisition, no visibility checks. The tradeoff is well-understood: Knight Bus doesn't support interactive transactions (it's a read-optimized analytical store). But for read-heavy workloads (which graph analytics IS), this is pure gain.

---

## 21. ArangoDB: RocksDB Key-Value Graph Traversal → CSR Bypass {#21-arangodb-rocksdb-traversal}

### What the competitor does

ArangoDB stores graph data in RocksDB (an LSM-tree key-value store). Vertices and edges are serialized as key-value pairs. The `TraverserEngine` performs graph traversals by:
1. Looking up a vertex by key in RocksDB
2. Scanning the edge index (prefix scan on `_from` or `_to` key) to find connected edges
3. For each edge, looking up the other endpoint vertex by key
4. Repeating for multi-hop

**Source:** `arangodb-src/arangod/Graph/TraverserOptions.cpp`, `arangodb-src/arangod/Graph/Cache/RefactoredTraverserCache.cpp`

**The pain:** Each traversal hop requires at least two RocksDB lookups (edge scan + vertex fetch). RocksDB lookups involve: memtable check → bloom filter checks → binary search in SST blocks → decompression. Multi-hop traversals multiply this cost. The `RefactoredTraverserCache` attempts to mitigate by caching recently-accessed vertices, but cache misses still go to RocksDB.

### Knight Bus innovation: CSR Bypass

Replace the entire RocksDB lookup chain with CSR:

```
// ArangoDB: vertex_key → RocksDB Get → deserialize → edge prefix scan → 
//           RocksDB Seek → iterate edges → for each: RocksDB Get(other_vertex)
// Cost per hop: 2+ RocksDB lookups × (bloom filter + binary search + decompress)

// Knight Bus: dense_id → offsets[dense_id] → peers[start..end]
// Cost per hop: 2 array reads (offset lookup + neighbor read)
```

**Result:** ArangoDB pays the cost of a general-purpose key-value store for every graph operation. Knight Bus uses a purpose-built data structure (CSR) that encodes graph topology directly. The RocksDB layer provides durability and transactions that Knight Bus doesn't need (immutable snapshots provide durability; single-writer provides consistency).

**Comparison:** ArangoDB's multi-model approach (document + graph + key-value on one engine) means graph traversal goes through layers designed for general key-value operations. Knight Bus is single-purpose: graph topology queries. The specialization enables 10-100× faster traversal.

---

## 22. JanusGraph: Wide-Row Edge Serialization → Dense CSR {#22-janusgraph-wide-row-serialization}

### What the competitor does

**Source:** `janusgraph-src/janusgraph-core/src/main/java/org/janusgraph/graphdb/database/EdgeSerializer.java:80-155`

```java
public RelationCache parseRelation(Entry data, boolean excludeProperties, TypeInspector tx) {
    ReadBuffer in = data.asReadBuffer();
    RelationTypeParse typeAndDir = IDHandler.readRelationType(in);
    long typeId = typeAndDir.typeId;
    Direction dir = typeAndDir.dirID.getDirection();
    RelationType relationType = tx.getExistingRelationType(typeId);
    // ... read otherVertexId, relationId, sort key, properties
    // Variable-length encoding throughout (VariableLong)
}
```

JanusGraph stores graph data in wide-row column stores (Cassandra, HBase, BerkeleyDB). Each vertex is a row key; its edges are columns within that row. The `EdgeSerializer` encodes each edge as a column entry containing: direction, type ID, other vertex ID, relation ID, sort key, and properties — all variable-length encoded.

To read edges: do a column range scan on the vertex's row (SliceQuery), deserialize each column entry through the EdgeSerializer.

**The pain:**
1. **Variable-length encoding** requires sequential parsing — can't jump to a specific edge without scanning predecessors
2. **Backend abstraction** (`KeyColumnValueStore`) adds indirection — each operation goes through the storage adapter
3. **Column store overhead** — Cassandra/HBase have their own compaction, bloom filters, memtables
4. **Serialization cost** — every edge must be deserialized through `parseRelation`, which involves type lookups (`tx.getExistingRelationType`)

### Knight Bus innovation: Dense CSR

Replace the entire wide-row + serialization stack:

```
// JanusGraph: vertex_row → SliceQuery(start_col, end_col) → iterate columns →
//             for each: ReadBuffer → IDHandler.readRelationType → readVertexId → 
//             VariableLong.readPositive → readPropertyValue → ...
// Each edge: variable bytes, sequential parsing, type system lookups

// Knight Bus: offsets[dense_id] → start/end → peers[start..end]
// Each edge: fixed 4 bytes (u32 dense_id), direct array read, no parsing
```

**Result:** JanusGraph's EdgeSerializer does ~10 operations per edge (read type, read direction, read vertex ID, read relation ID, read sort key, read properties — each with variable-length decoding). Knight Bus does 1 operation per edge (read u32 from array).

**The critical insight:** JanusGraph's `VariableLong` encoding saves space but requires sequential byte-by-byte parsing. Knight Bus's fixed-width u32 dense IDs waste a few bytes per edge but enable random access and SIMD processing. For analytical workloads that scan many edges, the fixed-width format wins overwhelmingly.

---

## 23. Cross-Cutting: Graph + OLAP Hybrid Queries → Walk-Aggregate Snapshots {#23-cross-cutting-walk-aggregate}

### The problem across all competitors

Every system we examined separates graph traversal from aggregation:
- **Neo4j:** Traverse graph → collect results → aggregate in Cypher runtime
- **DuckDB:** Join tables → aggregate (no native graph traversal)
- **ClickHouse:** Aggregate columns → no graph awareness
- **AGE:** DFS/BFS traversal → collect paths → aggregate in PostgreSQL
- **ArangoDB:** RocksDB traversal → AQL aggregation
- **JanusGraph:** Gremlin traversal → step-by-step aggregation

Hybrid queries like "average revenue of companies within 2 hops of company X" require two phases: traverse, then aggregate. The traversal result is materialized before aggregation begins.

### Knight Bus innovation: Walk-Aggregate Snapshots

Pre-compute traversal + aggregation at build time:

```
// Build-time: for common (traversal_depth, property, aggregate_fn) triples:
// walk_agg_fwd_2_revenue_sum[dense_id] → f64
//   = SUM(revenue) for all nodes within 2 forward hops of dense_id
// walk_agg_fwd_2_revenue_count[dense_id] → u64
//   = COUNT of nodes within 2 forward hops
// walk_agg_fwd_2_revenue_avg[dense_id] → f64
//   = walk_agg_fwd_2_revenue_sum[dense_id] / walk_agg_fwd_2_revenue_count[dense_id]

// Query: "average revenue within 2 hops of X"
//   walk_agg_fwd_2_revenue_avg[dense_id_of_X]  ← single f64 read, O(1)
```

**This is Innovation #3 from olap-innovations.md**, now motivated by reading all 7 competitor codebases and seeing that NONE of them can answer graph+aggregation queries in O(1).

**What makes this unique to Knight Bus:** Only Knight Bus has both:
1. A CSR adjacency structure for O(1) neighbor access
2. An immutable snapshot model where pre-computation is natural

Other systems either lack CSR (AGE, ArangoDB, JanusGraph use hash tables or KV stores) or don't persist CSR (Neo4j GDS builds transient CSR). Knight Bus persists CSR as the storage format, making walk-aggregate pre-computation a natural extension of the build process.

---

## 24. Cross-Cutting: Schema-Free Property Access → Typed Column Arrays {#24-cross-cutting-schema-free-properties}

### The problem across all competitors

All graph databases support schema-free property access (any node can have any properties):
- **Neo4j:** Property chains (linked list of 41-byte records)
- **Memgraph:** `PropertyStore` per vertex (variable-size serialized blob)
- **AGE:** `agtype` JSONB-like values (PostgreSQL varlena)
- **ArangoDB:** JSON documents in RocksDB (VelocyPack encoding)
- **JanusGraph:** Properties serialized within edge columns (variable-length)

**The pain:** Schema-free flexibility means no columnar access. "SUM(amount) for all nodes" requires deserializing every node's property blob, extracting "amount" by name, type-checking, and accumulating. No SIMD, no vectorization, unpredictable memory access.

### Knight Bus innovation: Typed Column Arrays

At snapshot build time, discover the de facto schema and emit typed column stripes:

```
// Build-time analysis:
// - Scan all nodes, collect property keys and types
// - For each property present on >N% of nodes:
//   Emit a typed dense array (column stripe)
// 
// property_stripe_amount: [f64; node_count]
//   property_stripe_amount[dense_id] → f64 (NaN if absent)
// property_stripe_name: [u32; node_count]  (offsets into string_table)
//   property_stripe_name[dense_id] → string_table offset
// property_stripe_active: [(u64, bitset); node_count/64]
//   Single bit per node for boolean properties

// Query: "SUM(amount)"
//   sequential scan of property_stripe_amount[0..N]
//   SIMD-friendly: 8 f64s per AVX-512 operation
```

**Result:** Schema-free properties become columnar at build time. The flexibility of schema-free storage is preserved at write time (any node can have any properties); the performance of columnar access is gained at read time.

**Key insight:** Graph databases assume schema-free means you can't have columns. But if the data is immutable after build, you CAN discover the implicit schema and emit columns. The schema-free property is a WRITE-TIME feature; columnar access is a READ-TIME optimization. They're not contradictory in an immutable snapshot model.

---

## Summary: The Unifying Pattern

Every competitor algorithm we examined falls into one of these categories:

| Category | Competitor Pattern | Knight Bus Pattern | Innovation |
|----------|-------------------|-------------------|------------|
| **Adjacency** | Linked lists (Neo4j), hash tables (AGE), vectors (Memgraph), KV lookups (ArangoDB), wide rows (JanusGraph) | CSR offsets + peers | One contiguous read vs. pointer-chasing |
| **Properties** | Linked chains (Neo4j), blobs (Memgraph), JSON (ArangoDB), variable-length (JanusGraph) | Typed column stripes | Sequential scan + SIMD vs. deserialization |
| **Joins** | Hash tables (DuckDB), hash probing | Dense-ID array indexing | O(1) array read vs. hash + collision |
| **Aggregation** | Runtime computation (DuckDB, ClickHouse) | Pre-computed aggregate stripes | O(1) lookup vs. O(N) scan |
| **Graph algorithms** | On-demand computation (Neo4j GDS) | Pre-materialized result vectors | O(1) read vs. iterative computation |
| **Traversal state** | Hash tables for visited edges (AGE) | Bitmaps indexed by dense ID | 1 bit per edge vs. ~40 bytes per hash entry |
| **Concurrency** | MVCC delta chains (Memgraph), locks | Immutable snapshots | Zero overhead reads vs. delta traversal |
| **Mutations** | LSM merge (ClickHouse, ArangoDB), linked-list insert (Neo4j) | Snapshot rebuild | One-time cost vs. ongoing merge work |
| **Graph+OLAP** | Two-phase: traverse then aggregate (all) | Walk-aggregate snapshots | O(1) vs. O(E^k) traversal + O(N) aggregation |

**The single principle:** If you know the question at build time, make the answer a contiguous array read at query time. Every innovation in this document is an application of this one idea to a specific algorithm found in competitor source code.

---

## Source Code References

All analysis based on actual source code reading:

- **Neo4j:** `NodeRecordFormat.java`, `RelationshipRecordFormat.java`, `RelationshipGroupRecordFormat.java`, `PropertyRecordFormat.java`, `NodeRecord.java` in `community/record-storage-engine/`
- **Neo4j GDS:** `PageRankComputation.java`, `Louvain.java`, `LabelPropagation.java`, `CSRGraphStore.java`
- **DuckDB:** `physical_hash_join.cpp`, `perfect_hash_join_executor.cpp`, `physical_perfecthash_aggregate.cpp`
- **ClickHouse:** `StorageMergeTree.cpp`, `MergeTreeDataPartWriterWide.cpp`, `IMergeTreeDataPart.cpp`
- **Apache AGE:** `age_vle.c` (VLE traversal, DFS algorithms, edge state hash tables), `age_global_graph.c` (vertex/edge hash tables)
- **Memgraph:** `vertex.hpp` (80-byte vertex struct, EdgeTriple vectors), `edge.hpp` (edge struct)
- **ArangoDB:** `TraverserOptions.cpp`, `RefactoredTraverserCache.cpp` (RocksDB-based traversal)
- **JanusGraph:** `EdgeSerializer.java` (wide-row edge encoding, variable-length parsing)
- **Knight Bus:** `snapshot.rs`, `runtime.rs`, `graph.rs`, `low_ram.rs`, `types.rs`
