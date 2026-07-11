# Record Chain Adjacency — Mermaid

| Field | Value |
| --- | --- |
| Kind | storage |
| Pair | `record-chain-adjacency-ascii.md` / `record-chain-adjacency-mermaid.md` |
| One-line job | Make edge traversal a pointer chase instead of an index lookup — Neo4j's fixed-size record chains ("index-free adjacency") versus Kuzu's disk-resident packed CSR: two on-disk answers to "where are this node's edges?" |

## 1. The defining read

```mermaid
flowchart LR
    E["expand(v): node -> neighbors —<br/>THE graph-db operation"]
    E --> R["relational answer: index-scan<br/>(edges WHERE src=v),<br/>O(log N) seek per edge"]
    E --> G["graph-db answer: physical pointers /<br/>offsets, O(1) per edge after the first"]
    G --> TWO["two opposite implementations<br/>in this corpus: chains vs disk-CSR"]
```

## 2. Neo4j: fixed-size records + chains

```mermaid
flowchart TD
    N["node record: 15 bytes<br/>(NodeRecordFormat.java:32)<br/>[inUse | nextRel | nextProp | labels]"]
    N -->|"address = id x 15:<br/>arithmetic, not an index"| STORE["flat store file"]
    N -->|nextRel| R1["rel record: 34 bytes<br/>(RelationshipRecordFormat.java:35)<br/>firstNode, secondNode (:70-82), type"]
    R1 --> CH["each rel record is in TWO doubly-linked<br/>lists at once: firstPrev/NextRel +<br/>secondPrev/NextRel (:129-139)"]
    CH --> WALK["expand(v): walk v's chain, at each hop<br/>taking the next pointer for WHICHEVER<br/>endpoint v is"]
    WALK --> IFA["'index-free adjacency' =<br/>this address arithmetic + these chains"]
```

## 3. Kuzu: packed CSR in node groups

```mermaid
flowchart TD
    NG["node group (2^17 nodes)"]
    NG --> H["CSRHeaderColumns { offset, length }<br/>(rel_table_data.h:23-26) —<br/>pattern 7's offsets array as a disk<br/>column; explicit length because<br/>gaps exist"]
    NG --> PK["packed neighbor array with SLACK:<br/>leaf regions kept below target density<br/>(PackedCSRInfo, csr_node_group.h:100-110:<br/>highDensityStep interpolates<br/>PACKED_CSR_DENSITY -><br/>LEAF_HIGH_CSR_DENSITY)"]
    NG --> DX["in-memory deltas: CSRIndex<br/>(csr_node_group.h:81-98)"]
    PK --> INS["most inserts land in slack;<br/>overflow rebalances ONE leaf region —<br/>amortized O(1) into a sorted layout"]
    DX --> CP["checkpoint merges deltas into packed<br/>form: LSM discipline (pattern 1)<br/>applied to adjacency"]
```

## 4. The trade

```mermaid
flowchart LR
    subgraph READ["expand(v), degree 100"]
        NC["neo4j: 100 record reads in INSERT<br/>ORDER — cold chain = up to 100 page<br/>touches (800 KB I/O for 3.4 KB data)"]
        KC["kuzu: offset + length + ONE contiguous<br/>run — 1-2 pages regardless of history"]
    end
    subgraph WRITE["insert edge (a)-[r]->(b)"]
        NW["neo4j: splice into two chains — patch<br/>up to 4 neighbor + 2 node records;<br/>tiny in-place writes, NEVER rebalances"]
        KW["kuzu: append into slack; occasional<br/>leaf rebalance; checkpoint rewrites"]
    end
    NC & NW --> V1["chains: write locality,<br/>no degradation — OLTP-flavored"]
    KC & KW --> V2["CSR: read locality,<br/>periodic maintenance — OLAP-flavored"]
```

## 5. Size arithmetic (10M nodes, 100M rels)

```mermaid
flowchart TD
    NEO["neo4j: 10M x 15 B = 150 MB nodes<br/>100M x 34 B = 3.4 GB rels —<br/>16 of 34 bytes are chain links +<br/>props pointer; ONE record serves<br/>both directions via its two chains"]
    KUZ["kuzu: offsets ~10M x 4-8 B +<br/>neighbors 100M x ~4 B compressed<br/>~1 GB — but x2 (fwd + bwd CSR)"]
    NEO & KUZ --> PT["pointer overhead vs direction<br/>duplication: neither is free"]
```

## 6. 2-hop traversal (degree 50, 2500 leaves)

```mermaid
sequenceDiagram
    participant Q as friends-of-friends(v)
    participant N as neo4j chains
    participant K as kuzu CSR
    Q->>N: 51 chain walks
    N-->>Q: worst case ~2550 random page touches —<br/>page-cache hit rate decides everything:<br/>hot graphs fly, cold graphs thrash
    Q->>K: 51 offset lookups + 51 contiguous runs
    K-->>Q: ~100-150 mostly-sequential page touches;<br/>output arrives as vectors ready for<br/>pattern 8-style batch processing
```

## 7. The corpus on this axis

```mermaid
flowchart TD
    AX["where are this node's edges?"]
    AX --> C1["CHAIN camp: neo4j (archetype);<br/>OrientDB/ArcadeDB record pointers"]
    AX --> C2["DISK-CSR camp: kuzu (packed CSR),<br/>TuGraph, GraphScope GART,<br/>DuckPGQ (CSR built per query)"]
    AX --> C3["KV-MAPPING camp: JanusGraph, Nebula,<br/>dgraph — adjacency list as a VALUE<br/>under key (node, direction, type):<br/>a posting list (17) in an LSM (1)"]
    C2 --> KIN["kuzu's slack-calibrated CSR = B-tree<br/>fill factor / packed-memory-array trick;<br/>pattern 7 made durable and updatable"]
```

## 8. The verification angle

```mermaid
flowchart TD
    U["adjacency STORAGE is unobservable from<br/>Cypher — only expand RESULTS are"]
    U --> D["differential harness: compare neighbor<br/>SETS per (node, direction, type)"]
    D --> M["insert-order chains vs rebalanced CSR<br/>make byte comparison meaningless"]
    M --> T["docs_PRD06 thesis condition 1, purest<br/>form: define the observable surface<br/>(query results), test THAT,<br/>let the layouts differ"]
```

## 9. Kinship map

```mermaid
flowchart TD
    K7["pattern 7 CSR: the in-memory ancestor"]
    K1["pattern 1 LSM: kuzu's delta+checkpoint;<br/>the KV camp's whole substrate"]
    K17["pattern 17: adjacency-as-posting-list<br/>in the KV camp"]
    K4["pattern 4 MVCC: kuzu's version record<br/>handlers sit beside the CSR code<br/>(rel_table_data.h:28-55)"]
    K7 & K1 & K17 & K4 --> LAW["corpus law: a graph store is a set<br/>decision (which edges) + a layout<br/>decision (chains, CSR, or KV) —<br/>and the layout decision is exactly the<br/>OLTP/OLAP fork"]
```

## 10. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| neo4j | `reference-repos-neo4j-family/neo4j-src/community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/format/standard/NodeRecordFormat.java` | 15-byte node record (32, 59-76) |
| neo4j | `reference-repos-neo4j-family/neo4j-src/community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/format/standard/RelationshipRecordFormat.java` | 34-byte rel record, dual chains (35, 70-82, 129-139) |
| kuzu | `reference-repos-competitors/kuzu-src/src/include/storage/table/rel_table_data.h` | CSRHeaderColumns (23-26) |
| kuzu | `reference-repos-competitors/kuzu-src/src/include/storage/table/csr_node_group.h` | CSRIndex (81-98), PackedCSRInfo (100-110) |

## 11. Cross-references

- Sibling patterns: see §9 kinship map.
- Next in category: property/columnar storage and the Cypher
  query pipeline (parse -> plan -> execute).
- Paper trail: the Kuzu CIDR paper and PMA (packed memory array)
  literature — queued in `research-papers-ledger.md`.
- 202606 digest overlap: digests recorded "index-free adjacency"
  as a slogan; this pair adds record byte layouts, the
  chain-splice write path, density-calibrated packed CSR, and
  the numeric read/write trade.
