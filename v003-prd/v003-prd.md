

# Structure

``` text
We will follow a minto pyramid

L1 will be small
L2 will be longer
L3 will be even longer and detailed

```

# L1 PRD

```
Neo4j rewritten in Rust

- exact same APIs or surface area with ZERO changes so that the same code can be used
- identical architecture for OLTP queries
- lowest RAM custom storage formats for OLAP queries
  → REAL RAM: 50 GB data processed comfortably on 8 GB systems
- community edition hence single node


to be specific
- OLTP data storage remains Neo4j-shaped.
- OLAP must account for direct and indirect RAM: heap page cache, duplicate layouts, compaction buffers, snapshot build scratch, delta overlays, indexes, and algorithm intermediates.

- O_DIRECT + compio gives EXACT RAM control
  - O_DIRECT bypasses page cache entirely
  - We allocate exactly the buffers we need: 64 KB, 1 MB, 64 MB — our choice
  - RSS = our allocations + stack + heap. Period.
  - DETERMINISTIC: same workload = same RSS every time
  - We CAN promise "PageRank uses exactly X MB"


```
