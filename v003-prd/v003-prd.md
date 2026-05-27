

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
```

## Supporting Documents

| Document | Key Finding |
|---|---|
| `docs_PRD02/OLAP-RAM-8GB-Constraint-Analysis.md` | Level 2 (3.2 GB) beats Neo4j (OOM) on 8 GB system |
| `docs_PRD02/Architecture-Dual-Engine.md` | OLTP identical + OLAP low-RAM dual engine |
| `docs_PRD02/1000IQ-OLAP-Architecture-Deep-Think.md` | 12 of 13 custom layouts INCREASE RAM — killed |
| `docs_PRD02/Deep-Research-Custom-Formats-Per-Family.md` | 25+ papers confirm single CSR base |
| `docs_PRD02/Why-Compio-IS-Right-For-OLAP-RAM.md` | compio O_DIRECT = deterministic RAM |
| `docs_PRD02/Rubber-Duck-13-Families-vs-Neo4j-Source.md` | All claims verified against Neo4j source |
