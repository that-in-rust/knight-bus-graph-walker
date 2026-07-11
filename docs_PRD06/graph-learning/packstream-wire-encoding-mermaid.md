# PackStream Wire Encoding — Mermaid

| Field | Value |
| --- | --- |
| Kind | storage |
| Pair | `packstream-wire-encoding-ascii.md` / `packstream-wire-encoding-mermaid.md` |
| One-line job | Neo4j's binary serialization: a nibble-tagged, big-endian, size-prefixed format where every value starts with one marker byte, and graph entities (Node, Relationship, Path) ride as tagged structs inside ordinary Bolt messages |

## 1. The one-byte dispatch

```mermaid
flowchart TD
    M["marker byte -> (hi, lo) nibbles<br/>(neo4rs de.rs:222-260:<br/>marker >> 4, marker & 0x0F)"]
    M --> H8["0x8: tiny string, lo = len 0-15"]
    M --> H9["0x9: tiny list | 0xA: tiny map<br/>lo = count"]
    M --> HB["0xB: STRUCT, lo = field count,<br/>then 1 signature tag byte"]
    M --> HC["0xC scalars: C0 null, C1 float64,<br/>C2/C3 false/true, C8-CB int 8/16/32/64,<br/>CC-CE bytes"]
    M --> HD["0xD sized: D0-D2 string 8/16/32,<br/>D4-D6 list, D8-DA map"]
    M --> HT["else: marker IS the value —<br/>tiny int -16..127 in one byte"]
```

## 2. The writer picks the smallest encoding

```mermaid
flowchart TD
    I["int value (python driver<br/>packstream/v1/__init__.py:108-124)"]
    I --> T1["-16 <= v < 128 : 1 byte (the marker)"]
    I --> T2["-128 <= v < -16 : C8 + 1 byte"]
    I --> T3["16-bit range : C9 + 2 bytes"]
    I --> T4["32-bit range : CA + 4 bytes"]
    I --> T5["64-bit range : CB + 8 bytes"]
    T1 & T2 & T3 & T4 & T5 --> BE["always BIG-endian; strings/lists/maps<br/>same ladder: tiny (size<=15, one byte<br/>0x80|size, :194-202) then 8/16/32-bit<br/>length prefixes"]
```

## 3. Structs: protocol and graph share one shape

```mermaid
flowchart LR
    S["struct = 0xB(n) + tag + n fields"]
    S --> PM["protocol messages:<br/>RUN = 0xB3, tag 0x10<br/>(neo4rs messages/run.rs:5)<br/>+ begin/commit/pull/hello..."]
    S --> GE["graph entities (python<br/>hydration_handler.py:60-66):<br/>'N' Node | 'R' Relationship<br/>'r' UnboundRel | 'P' Path"]
    GE --> NEST["structs nest like any value:<br/>RECORD msg > list > 'N' struct"]
```

## 4. The Bolt session around it

```mermaid
sequenceDiagram
    participant D as driver
    participant S as server
    D->>S: magic 60 60 B0 17 + 4 proposed versions<br/>(neo4rs connection.rs:140)
    S-->>D: chosen version (gates struct fields:<br/>element_id added in v5.0 —<br/>hydrate_node's back-compat branch)
    D->>S: HELLO {auth...} -> SUCCESS
    D->>S: RUN (0x10) "MATCH..." {params}
    S-->>D: SUCCESS {fields: [...]}
    D->>S: PULL {n}
    S-->>D: RECORD [values...] (repeated)
    S-->>D: SUCCESS {summary}
    Note over D,S: each message chunked: 2-byte length<br/>prefixes, 0x0000 terminator
```

## 5. Worked example — a 14-byte map

```mermaid
flowchart TD
    J["{'age': 30, 'name': 'Ann'}"]
    J --> B["A2 — tiny map, 2 entries<br/>83 61 67 65 — 'age'<br/>1E — int 30, ONE byte<br/>84 6E 61 6D 65 — 'name'<br/>83 41 6E 6E — 'Ann'"]
    B --> CMP["14 bytes vs 26 as JSON —<br/>and no int-vs-float ambiguity"]
```

## 6. Worked example — integer boundaries are contract

```mermaid
flowchart LR
    V7["7 -> 07 (1B)"] --> V127["127 -> 7F (1B)"]
    V127 --> V128["128 -> C9 00 80 (3B)<br/>no INT_8 for positives >127"]
    V128 --> V32k["32768 -> CA.. (5B)"]
    V32k --> V2b["2^31 -> CB.. (9B)"]
    V2b --> C["a rewrite emitting CA where stock<br/>emits C9 = different bytes on the wire —<br/>and 5 driver codebases notice"]
```

## 7. Position in the corpus

```mermaid
flowchart TD
    INT["everything internal:<br/>chains/CSR/permutations (20, 22),<br/>pull pipelines (21)"]
    INT --> EDGE["PackStream = the OBSERVABLE EDGE:<br/>all of it collapses to 'N'/'R' structs"]
    EDGE --> ECO["5 independent drivers<br/>(java/python/go/js/.net) + neo4rs<br/>against one server: the corpus's best<br/>example of a wire contract holding<br/>an ecosystem together"]
    ECO --> EVO["and versioned (element_id in v5):<br/>contract evolution in the wild"]
```

## 8. The verification angle

```mermaid
flowchart TD
    W["the wire is the rewrite's hardest<br/>byte-exact surface — and best oracle"]
    W --> R1["capture stock driver<->server traffic"]
    W --> R2["replay against the rewrite"]
    W --> R3["diff BYTES — legitimate here, unlike<br/>storage layouts: five codebases<br/>depend on exact encodings"]
    R1 & R2 & R3 --> RW["docs_PRD06 thesis: cheap, total,<br/>pre-paid differential signal —<br/>testkit (next pattern's subject)<br/>industrializes exactly this"]
```

## 9. Kinship map

```mermaid
flowchart TD
    K21["pattern 21: Produce's rows<br/>become RECORD messages"]
    K20["pattern 20: what an 'N'<br/>struct abstracts away"]
    K19["pattern 19 contrast: PackStream<br/>optimizes STREAM decode;<br/>FSTs optimize random lookup —<br/>both are byte-level contracts"]
    K17["pattern 17 kinship: smallest-encoding<br/>ladders = the vInt idea, applied<br/>per-value instead of per-block"]
    K21 & K20 & K19 & K17 --> LAW["corpus law: at every boundary —<br/>disk block, index segment, wire —<br/>engines converge on marker + length<br/>+ smallest-that-fits"]
```

## 9b. Five drivers, one contract (the ecosystem test)

```mermaid
flowchart TD
    SRV["one Neo4j server"]
    SRV --- J["java driver"]
    SRV --- PY["python driver:<br/>precomputed lookup tables<br/>(PACKED_UINT_8/16 arrays)"]
    SRV --- GO["go driver"]
    SRV --- JS["javascript driver"]
    SRV --- NET[".net driver"]
    SRV --- RS["neo4rs (community Rust):<br/>nibble match + serde visitors"]
    PY & RS --> STYLE["same bytes, opposite idioms —<br/>a free study in how one binary<br/>contract shapes per-language code"]
    STYLE --> TK["testkit runs ONE cross-driver suite<br/>against all of them: the ecosystem's<br/>own differential harness, and the<br/>template for a rewrite's parity rig"]
```

## 10. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| neo4rs | `reference-repos-neo4j-family/neo4rs-src/lib/src/packstream/de.rs` | nibble-dispatch decoder (222-260) |
| neo4rs | `reference-repos-neo4j-family/neo4rs-src/lib/src/connection.rs` | Bolt magic handshake (140) |
| neo4rs | `reference-repos-neo4j-family/neo4rs-src/lib/src/messages/run.rs` | RUN struct sig 0xB3/0x10 (5) |
| python-driver | `reference-repos-neo4j-family/neo4j-python-driver-src/src/neo4j/_codec/packstream/v1/__init__.py` | smallest-encoding writer (100-124, 194-202) |
| python-driver | `reference-repos-neo4j-family/neo4j-python-driver-src/src/neo4j/_codec/hydration/v1/hydration_handler.py` | N/R/r/P hydration (60-66) |

## 11. Cross-references

- Sibling patterns: `pull-operator-pipeline` (21),
  `record-chain-adjacency` (20), `fst-term-dictionary` (19),
  `posting-block-compression` (17).
- The ecosystem angle: the same PackStream logic exists five
  times in five languages in this corpus — comparing the
  implementations (e.g. python's lookup tables vs neo4rs's
  nibble match) is a free study in how one binary contract
  shapes idiomatic code differently per language.
- Next: neo4j-ecosystem synthesis (drivers, testkit, APOC as
  the compatibility test bed), then dataflow-compute and
  bench-testing to close the corpus.
