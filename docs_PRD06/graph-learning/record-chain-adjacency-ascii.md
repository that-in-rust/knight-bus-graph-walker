# Record Chain Adjacency — ASCII

| Field | Value |
| --- | --- |
| Kind | storage |
| Pair | `record-chain-adjacency-ascii.md` / `record-chain-adjacency-mermaid.md` |
| One-line job | Make edge traversal a pointer chase instead of an index lookup — Neo4j's fixed-size record chains ("index-free adjacency") versus Kuzu's disk-resident packed CSR: two on-disk answers to "where are this node's edges?" |

## 1. The job

A graph database's defining read is expand: given a node, produce
its neighbors. The whole category is defined by making expand NOT
a B-tree lookup per edge (what relational engines do with a join
index) but a direct address computation:

```text
relational:  neighbors(v) = index-scan(edges WHERE src = v)
                            O(log N) per seek, per edge
graph-db:    neighbors(v) = follow physical pointers / offsets
                            O(1) per edge after the first
```

Two structurally opposite implementations live in this corpus.

## 2. Neo4j: fixed-size records + doubly-linked chains

Every node and relationship is a FIXED-SIZE record in a flat
store file; IDs are record numbers, so address = id x size:

```text
NodeRecordFormat.java:32   RECORD_SIZE = 15 bytes
    [inUse:1][nextRel:4][nextProp:4][labels:5][extra:1]
    (+ modifier bits in the header byte extend the 32-bit
     fields to 35-bit IDs — :59-76)
RelationshipRecordFormat.java:35   RECORD_SIZE = 34 bytes
    firstNode, secondNode (:70-82), type,
    firstPrevRel/firstNextRel   — chain links for firstNode
    secondPrevRel/secondNextRel — chain links for secondNode
    (:129-139), plus nextProp

node.nextRel -> first relationship record; each relationship
record is a member of TWO doubly-linked lists at once (one per
endpoint). expand(v) = read node record, walk v's chain,
at each hop picking the next pointer for WHICHEVER endpoint
v is (first or second).
```

Address arithmetic replaces every index: node 12345 lives at
byte 12345 x 15 in the node store. This is what "index-free
adjacency" means mechanically.

## 3. Kuzu: packed CSR in node groups

Kuzu instead stores adjacency columnar, as CSR per node group
(2^17 nodes each), with slack for updates:

```text
rel_table_data.h:23-26   CSRHeaderColumns { offset, length }
    — two COLUMNS: per-node offset into the packed edge
      array + list length (pattern 7's offsets array, made
      a disk column; length is explicit because gaps exist)
csr_node_group.h:81-98   CSRIndex — per-node row indices for
    the in-memory (delta) part
csr_node_group.h:100-110 PackedCSRInfo — leaf regions with
    density calibration: NODE_GROUP_SIZE_LOG2 vs
    CSR_LEAF_REGION_SIZE_LOG2, highDensityStep interpolating
    between PACKED_CSR_DENSITY and LEAF_HIGH_CSR_DENSITY
```

The density machinery is the interesting part: leave each CSR
leaf region PARTIALLY full (e.g. target density < 1), so most
edge inserts land in existing slack; only overflow rebalances a
region — amortized O(1) inserts into a sorted, scan-friendly
layout. Deltas accumulate in memory (CSRIndex) and checkpoint
into the packed form — LSM discipline (pattern 1) applied to
adjacency.

## 4. The trade, worked

```text
expand(v) with degree 100:
neo4j chains:  100 record reads at 34 B, but records are
    allocated in INSERT ORDER — a cold chain can touch up to
    100 different 8 KB pages = 800 KB of I/O for 3.4 KB of data
kuzu CSR:      offset[v], length[v], then ONE contiguous run of
    100 edge entries — 1-2 pages regardless of insert history
write an edge (a)-[r]->(b):
neo4j:  allocate record, splice into TWO chains = patch up to
    4 neighbor records + 2 node records — but each patch is a
    tiny in-place write; genuinely O(1), no rebalancing ever
kuzu:   append into slack; occasionally rebalance a leaf
    region; checkpoint rewrites node-group columns
```

Chains optimize WRITE locality and never degrade; CSR optimizes
READ locality and pays periodic maintenance. OLTP-flavored vs
OLAP-flavored graph storage in one line.

## 5. Worked example — size arithmetic

```text
10M nodes, 100M relationships:
neo4j:  nodes 10M x 15 B = 150 MB; rels 100M x 34 B = 3.4 GB
        every record inflated by chain pointers (16 of the
        34 bytes are the four chain links + props pointer)
kuzu:   per direction: offsets ~10M x ~4-8 B + neighbor IDs
        100M x ~4 B compressed — ~1 GB total, x2 directions
        (kuzu stores fwd + bwd CSR; neo4j's one relationship
        record serves both directions via its two chains)
```

## 6. Worked example — 2-hop traversal cost

```text
friends-of-friends from one node, avg degree 50 (2500 leaves):
neo4j:  51 chain walks; worst case ~2550 random page touches;
        page cache hit rate decides everything — hot graphs
        fly, cold graphs thrash
kuzu:   51 offset lookups + 51 contiguous runs ~ 100-150 page
        touches, mostly sequential — and the scan produces
        vectors ready for pattern 8-style batch processing
```

## 7. Where the corpus sits on this axis

- Chain camp: Neo4j (the archetype); OrientDB/ArcadeDB inherit
  record-pointer designs.
- CSR-on-disk camp: Kuzu (packed CSR above), TuGraph and
  GraphScope's GART (CSR variants), DuckPGQ (CSR built on the
  fly per query over DuckDB tables).
- KV-mapping camp (neither): JanusGraph/NebulaGraph/dgraph
  serialize adjacency lists as values in a KV store (pattern 1's
  LSM) keyed by (node, direction, type) — adjacency becomes a
  posting list (pattern 17) stored under a key.
- Analytics kinship: kuzu's on-disk CSR is pattern 7 made
  durable and updatable; the density-calibrated slack is the
  same trick as B-tree fill factor and PMA (packed memory
  arrays).

## 8. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| neo4j | `reference-repos-neo4j-family/neo4j-src/community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/format/standard/NodeRecordFormat.java` | 15-byte node record, nextRel/nextProp (32, 59-76) |
| neo4j | `reference-repos-neo4j-family/neo4j-src/community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/format/standard/RelationshipRecordFormat.java` | 34-byte rel record, two doubly-linked chains (35, 70-82, 129-139) |
| kuzu | `reference-repos-competitors/kuzu-src/src/include/storage/table/rel_table_data.h` | CSRHeaderColumns offset+length (23-26) |
| kuzu | `reference-repos-competitors/kuzu-src/src/include/storage/table/csr_node_group.h` | CSRIndex deltas (81-98), PackedCSRInfo density calibration (100-110) |
| janusgraph | `reference-repos-competitors/janusgraph-src/janusgraph-core/src/main/java/org/janusgraph/graphdb/database/EdgeSerializer.java` | the KV camp's witness: edges serialized into column-family values under vertex keys |

## 9. Cross-references

- Sibling patterns: `csr-adjacency-layout` (7 — the in-memory
  ancestor of kuzu's disk CSR), `lsm-compaction-tradeoff` (1 —
  kuzu's delta+checkpoint and the KV-camp's storage),
  `posting-block-compression` (17 — adjacency-as-posting-list in
  the KV camp), `mvcc-snapshot-visibility` (4 — kuzu's version
  record handlers sit right beside the CSR code).
- Next in category: property storage & the query pipeline
  (parse -> plan -> execute) as the following pairs.
- Verification note (docs_PRD06 thesis): adjacency STORAGE is
  unobservable from Cypher — only expand RESULTS are. A
  differential harness compares neighbor SETS per (node,
  direction, type); insert-order-dependent chain layouts vs
  rebalanced CSR make byte-level comparison meaningless. This is
  thesis condition 1 in its purest form: define the observable
  surface (query results), test that, and let the layouts differ.
- 202606 digest overlap: digests recorded "index-free adjacency"
  as Neo4j's slogan; this pair adds the record byte layouts, the
  chain-splice write path, kuzu's density-calibrated packed CSR,
  and the numeric read/write trade.
