# PackStream Wire Encoding — ASCII

| Field | Value |
| --- | --- |
| Kind | storage |
| Pair | `packstream-wire-encoding-ascii.md` / `packstream-wire-encoding-mermaid.md` |
| One-line job | Neo4j's binary serialization: a nibble-tagged, big-endian, size-prefixed format where every value starts with one marker byte, and graph entities (Node, Relationship, Path) ride as tagged structs inside ordinary Bolt messages |

## 1. The job

Every value that crosses the wire between a driver and Neo4j —
query parameters, result rows, nodes, paths — is PackStream.
It's MessagePack's idea (marker byte encodes type + small
sizes inline) with one graph-shaped addition: STRUCTS with
one-byte signature tags, used for both protocol messages (RUN,
PULL) and graph entities (Node = 'N').

## 2. The marker-byte layout

The decoder's whole dispatch is the HIGH NIBBLE of byte one —
neo4rs makes this literal:

```text
neo4rs-src/lib/src/packstream/de.rs:222-260
    let (hi, lo) = (marker >> 4, marker & 0x0F);
    0x8 -> tiny string  (lo = length 0-15)
    0x9 -> tiny list    (lo = count)
    0xA -> tiny map     (lo = count)
    0xB -> struct       (lo = field count), then 1 tag byte
    0xC -> scalars: C0 null, C1 float64, C2 false, C3 true,
           C8/C9/CA/CB int 8/16/32/64, CC/CD/CE bytes 8/16/32
    0xD -> sized: D0/D1/D2 string 8/16/32,
           D4/D5/D6 list, D8/D9/DA map
    else -> the marker IS the value: tiny int -16..127
```

The Python driver writes the mirror image:

```text
neo4j-python-driver-src/src/neo4j/_codec/packstream/v1/__init__.py
:100-124  scalars: b"\xc2" false, b"\xc1"+8B float,
          ints pick the SMALLEST encoding: -16<=v<128 one byte,
          then C8 (1B), C9 (2B), CA (4B), CB (8B) — big-endian
:194-202  tiny-anything: size<=0x0F -> one byte 0x80|size
          (strings; same shape for lists/maps at 0x90/0xA0)
```

Key property: the format is length-prefixed everywhere — a
decoder can skip any value without understanding it, and the
smallest-encoding rule makes the common case (small ints, short
strings) one or two bytes.

## 3. Structs: messages and graph entities

```text
struct = 0xB<fieldcount> <tag> <field>*
protocol messages (neo4rs-src/lib/src/messages/run.rs:5):
    #[signature(0xB3, 0x10)]     RUN: 3 fields, tag 0x10
    (extras: begin/commit/pull/discard/hello... one file each)
graph entities (python driver, hydration/v1/
    hydration_handler.py:60-66):
    b"N" Node                b"R" Relationship
    b"r" UnboundRelationship b"P" Path
    hydrate_node(id_, labels, properties, element_id) — the
    driver rebuilds client-side objects from struct fields
```

So a result row containing a node is: a RECORD message struct,
containing a list, containing an 'N' struct — structs nest like
any other value.

## 4. The Bolt frame around it

```text
handshake (neo4rs-src/lib/src/connection.rs:140):
    magic [0x60, 0x60, 0xB0, 0x17] + 4 proposed versions
    -> server picks one; version gates which struct fields
    exist (element_id was ADDED in v5.0 — hydrate_node's
    "backwards compatibility with Neo4j < 5.0" branch)
then: each message = chunked PackStream (2-byte chunk length
    prefixes, 0x0000 terminator), request/response pairs:
    RUN -> SUCCESS(fields) ; PULL -> RECORD* -> SUCCESS(summary)
```

## 5. Worked example — encode a small map

```text
{"age": 30, "name": "Ann"}  ->
    A2                    tiny map, 2 entries
    83 61 67 65           tiny string len 3, "age"
    1E                    tiny int 30 (one byte!)
    84 6E 61 6D 65        tiny string len 4, "name"
    83 41 6E 6E           tiny string len 3, "Ann"
14 bytes total. The same map as JSON: 26 bytes, plus parsing
ambiguity (is 30 an int or float?) that PackStream never has.
```

## 6. Worked example — integer boundaries

```text
value      encoding              bytes
7          07                    1     (tiny int)
-16        F0                    1     (tiny reaches -16)
-17        C8 EF                 2     (INT_8)
127        7F                    1
128        C9 00 80              3     (INT_16 — no INT_8 for
                                        positives >127!)
32767      C9 7F FF              3
32768      CA 00 00 80 00        5     (INT_32)
2^31       CB + 8 bytes          9     (INT_64)
the boundaries are CONTRACT: a rewrite that emits CA where
stock emits C9 produces different bytes — and byte-diffing
drivers' traffic is the cheapest differential test there is.
```

## 7. Why this matters for the corpus

PackStream is the OBSERVABLE EDGE of everything the graph-db
category does internally: chains, CSR, permutations (20, 22)
all collapse to 'N'/'R' structs here. Five independent driver
implementations (java/python/go/js/.net + neo4rs) against one
server make this the corpus's best example of a wire contract
holding an ecosystem together — the format is small enough to
document exhaustively (this page nearly does) yet versioned
enough (element_id) to show contract evolution in the wild.

## 8. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| neo4rs | `reference-repos-neo4j-family/neo4rs-src/lib/src/packstream/de.rs` | nibble-dispatch decoder (222-260) |
| neo4rs | `reference-repos-neo4j-family/neo4rs-src/lib/src/connection.rs` | Bolt magic handshake (140) |
| neo4rs | `reference-repos-neo4j-family/neo4rs-src/lib/src/messages/run.rs` | RUN = struct sig 0xB3/0x10 (5) |
| python-driver | `reference-repos-neo4j-family/neo4j-python-driver-src/src/neo4j/_codec/packstream/v1/__init__.py` | smallest-encoding writer (100-124, 194-202) |
| python-driver | `reference-repos-neo4j-family/neo4j-python-driver-src/src/neo4j/_codec/hydration/v1/hydration_handler.py` | N/R/r/P entity hydration (60-66) |

## 9. Cross-references

- Sibling patterns: `pull-operator-pipeline` (21 — Produce's
  rows become RECORD messages), `record-chain-adjacency` (20 —
  what an 'N' struct abstracts away), `fst-term-dictionary`
  (19 — contrast: PackStream optimizes for STREAM decode, FSTs
  for random lookup; both are byte-level contracts).
- Verification note (docs_PRD06 thesis): the wire is the
  rewrite's HARDEST byte-exact surface and its BEST oracle —
  capture stock driver<->server traffic, replay against the
  rewrite, diff bytes. Unlike storage layouts, here bytes ARE
  the contract (five driver codebases depend on them), so
  byte-diffing is legitimate, cheap, and total.
- Next: neo4j-ecosystem synthesis (drivers + testkit + APOC as
  the compatibility test bed), then dataflow/bench batches.
