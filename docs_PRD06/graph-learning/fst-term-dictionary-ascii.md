# FST Term Dictionary — ASCII

| Field | Value |
| --- | --- |
| Kind | storage |
| Pair | `fst-term-dictionary-ascii.md` / `fst-term-dictionary-mermaid.md` |
| One-line job | Map every term in the index to its posting-list location through a finite-state transducer — a minimal automaton that shares both prefixes and suffixes, giving a compressed sorted dictionary that answers exact, range, prefix, and regex lookups in one structure |

## 1. The job

Patterns 17-18 assumed you already HAVE the posting list. The term
dictionary is the front door: term bytes -> where its postings
live. Requirements:

```text
- millions of terms, must live (mostly) in RAM
- exact lookup ("graph")
- ordered iteration + range scans (term ordinals)
- prefix queries ("graph*"), fuzzy/regex (Levenshtein automata)
```

Hash maps give only the first. B-tree-ish sorted blocks give the
first three. The Lucene-family answer is the FST — a minimal
deterministic automaton over the sorted term bytes.

## 2. Raw data shape

```text
insert (sorted): graph, graphdb, graphs, grapple

trie shares prefixes only:        FST also shares suffixes:
  g-r-a-p ─ h ─ (db|s)              g-r-a-p ─ h  ┐
          └ p-l-e                           ├─ shared tail states
                                    └ p-l-e ┘
a MINIMAL acyclic automaton = the trie with identical sub-trees
merged. Lucene's FST.java notes the pure-prefix-trie special case
as the non-shared degenerate form (FST.java:46-47); the structure
is "a compact byte[] format" (FST.java:55) — states are not
objects but positions in one byte array.

TRANSDUCER, not just acceptor: each path carries an OUTPUT.
  tantivy: MapBuilder maps term -> u64 term ordinal
           (fst_termdict/termdict.rs:25,35), ordinals are dense
           0..num_terms-1 (:151)
  the ordinal then indexes a bit-packed side table with the real
  payload: postings offset, positions offset, doc freq —
  TermInfoBlockMeta with per-block nbits (term_info_store.rs:15-20),
  pattern 17's bit-packing trick reused on metadata.
```

## 3. Why outputs-along-edges works

FST outputs distribute over the path: the u64 is split so that
shared prefixes carry the COMMON part of all their terms' values,
with remainders pushed toward the leaves. Sorted dense ordinals
make the arithmetic small — mostly increments near the leaf edges.
This is why term ordinals (not raw file offsets) are the FST value:
dense sorted outputs compress; scattered 40-bit offsets would not.

## 4. Step-by-step: the three query shapes

```text
exact("graphs"):
    walk g-r-a-p-h-s edge by edge, summing outputs;
    dead-end = term absent (no false positives, unlike bloom)
range("graphd" ..= "graphz"):
    tantivy exposes range() streaming over the sorted automaton
    (termdict.rs:204-212) — ordered iteration is free because
    the FST IS the sorted order
automaton search (prefix/fuzzy/regex):
    intersect the dictionary FST with a query automaton — walk
    both in lockstep, only visiting dictionary paths the query
    automaton also accepts (termdict.rs:217-218: search() takes
    any Automaton). A Levenshtein automaton of edit distance 2
    intersected with the FST = fuzzy search WITHOUT scanning
    the dictionary.
```

That last shape is the pattern's superpower: the dictionary is a
graph, queries are graphs, and lookup = graph intersection.

## 5. Worked example — size arithmetic

```text
10M terms, average 12 bytes = 120 MB raw sorted strings
prefix sharing (trie): English-like corpora share ~60-70% of
    leading bytes -> ~40-50 MB of edges
suffix sharing (FST minimality): plural/inflection tails merge;
    published Lucene-family FSTs land ~3-6 bytes/term
    -> 10M terms ~ 30-60 MB in one mmap-able byte[]
+ term-info table: ~10-16 bytes/term bit-packed (pattern 17
    §2 trick) — often larger than the FST itself.
alternative in the SAME codebase: tantivy also ships an
sstable_termdict (termdict/sstable_termdict/) — sorted blocks,
simpler, better for object storage: the roaring-vs-blocks
choice (pattern 3) replayed on the dictionary.
```

## 6. Worked example — fuzzy query cost

```text
query: fuzzy("grap", distance 1) over 10M terms
naive: 10M edit-distance computations ~ seconds
FST x Levenshtein automaton: the walk visits only states
    reachable within distance 1 — bounded by (branching at
    each step) x (automaton states ~ query_len x distance)
    ~ thousands of edge traversals, sub-millisecond.
the cost scales with the RESULT neighborhood, not the
dictionary size — the same navigate-don't-scan economics as
HNSW (pattern 13) and skip lists (pattern 17).
```

## 7. Where systems inherit this

- Lucene: util/fst/ (FST.java, FSTCompiler.java) backs the terms
  index of Elasticsearch/OpenSearch/Solr; also used for synonym
  maps and suggesters.
- tantivy: fst_termdict via the tantivy-fst crate (a fork of
  BurntSushi's fst crate — the same library powers ripgrep's
  dictionary tricks); sstable_termdict as the alternative.
- RediSearch/manticore: tries and custom dictionaries — same
  role, weaker automaton composition.
- Graph-db kinship: an FST IS a DAG with labeled edges and
  path-additive values — term lookup is a graph traversal;
  Levenshtein intersection is a product-graph walk (the same
  product construction as bisimulation checks).

## 8. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| lucene | `reference-repos-corpus/lucene-src/lucene/core/src/java/org/apache/lucene/util/fst/FST.java` | the byte[]-encoded automaton (46-47, 55); family-wide terms index |
| lucene | `reference-repos-corpus/lucene-src/lucene/core/src/java/org/apache/lucene/util/fst/FSTCompiler.java` | builds minimal FSTs from sorted input |
| tantivy | `reference-repos-corpus/tantivy-src/src/termdict/fst_termdict/termdict.rs` | MapBuilder term->ordinal (25, 35), dense ordinals (151), range (204-212), automaton search (217-218) |
| tantivy | `reference-repos-corpus/tantivy-src/src/termdict/fst_termdict/term_info_store.rs` | bit-packed TermInfo side table (15-20) |
| tantivy | `reference-repos-corpus/tantivy-src/src/termdict/sstable_termdict/` | the sorted-block alternative dictionary |

## 9. Cross-references

- Sibling patterns: `posting-block-compression` (what the
  dictionary points at; shares the bit-packing trick),
  `bloom-filter-read-shortcuts` (FST exact lookup has NO false
  positives — a stronger, costlier filter), `hnsw-layered-greedy`
  (navigate-don't-scan economics), `roaring-bitmap-idsets`
  (encoding-per-shape choice, replayed as fst-vs-sstable).
- Next in category: the FTS category synthesis pair (17-19 +
  segment lifecycle notes).
- Verification note (docs_PRD06 thesis): FST construction from
  sorted input is deterministic — identical term sets yield
  byte-identical FSTs per library version, but NOT across
  libraries (lucene vs tantivy-fst encode differently). Compare
  at the automaton-semantics level: same accepted set, same
  outputs — i.e., differential-test by streaming both
  dictionaries and diffing the (term, value) sequences.
- 202606 digest overlap: digests mentioned "FST term dictionary"
  as a Lucene keyword; this pair adds the suffix-sharing
  mechanics, output arithmetic, automaton intersection, and the
  size/cost numbers.
