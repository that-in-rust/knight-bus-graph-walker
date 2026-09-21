# Public Research Inputs

## SNAP ca-GrQc

Source: [SNAP General Relativity and Quantum Cosmology collaboration network](https://snap.stanford.edu/data/ca-GrQc.html).

Download: [ca-GrQc.txt.gz](https://snap.stanford.edu/data/ca-GrQc.txt.gz).

The source describes an undirected coauthorship graph derived from arXiv submissions from January 1993 through April 2003. Its published table lists 5,242 nodes and 14,496 edges. Independent parsing observed 28,980 endpoint rows, including 12 self-loop rows and 14,484 reverse/duplicate undirected rows. Removing loops and deduplicating undirected edges yields 5,242 retained nodes, 14,484 simple edges and 28,968 directed memberships. Endpoint IDs are retained even if loop removal leaves a node isolated. Binary neighbor-set Jaccard uses this explicit simple projection, not an assumed match to the source page's edge count.

Source-requested scholarly citation: J. Leskovec, J. Kleinberg and C. Faloutsos, Graph Evolution: Densification and Shrinking Diameters, ACM TKDD 1(1), 2007.

Purpose: exact binary neighbor-set Jaccard experiments for the A05 bounded-seed selector and ordered-endpoint merge. This is public source data, not generated benchmark evidence and not the repository's separate synthetic 2 GB workload. It is a small real-data correctness/cost experiment, not a physical 4 GB scalability demonstration. Download date: 2026-09-20.

SHA-256 of retained `ca-GrQc.txt.gz`: `a254442cdf5d684712578b630c2e0d7543518ab154ef2341cabb607572ce7230`.

Receipts: [initial 54-result experiment](../Similarity-Bounded-Seed-Results.json), [revised 72-result experiment](../Similarity-Merged-Endpoint-Results.json), and [methods, exact semantics and limitations](../../Similarity-Bounded-Seed-Selection.md). No source code or dataset license is inferred from this citation; consult SNAP's source terms for redistribution beyond this research checkout.

Later receipts add the same six degree strata plus 24 previously unselected source IDs (uniform sample without replacement, seed 920520): [five-mode 450-result pruning experiment](../Similarity-Run-Bound-Results.json) and [six-mode 540-result DAAT comparison](../Similarity-Run-Bound-DAAT-Results.json). These are repeated executions of 30 distinct queries on this same graph, not 990 independent query inputs. See [the corrected strongest-comparator analysis](../../Similarity-Run-Bound-Pruning.md).

The same queries support the [360-execution capacity study](../../Similarity-Laminar-Capacity-Results.json), [450-execution eager-pair study](../../Similarity-Paired-Capacity-Results.json), [540-execution lazy-cascade study](../../Similarity-Paired-Cascade-Results.json), and [540-execution serial timing rerun](../../Similarity-Paired-Serial-Results.json). These vary block/summary/evaluation choices, not the source projection. The first three are exploratory timings with overlapping test activity; the final rerun follows terminal tests and agents. No run measures physical RAM or Neo4j/GDS. See [paired methods and limitations](../../Similarity-Paired-Capacity-Recovery.md).

The [540-execution selective-access receipt](../../Similarity-Selective-Access-Results.json) reuses these SAME 30 IDs, B=2, G=4, k=10 and three rotated-order repetitions. It adds an exact lower/upper interval control sharing the paid feature-to-block index. The run followed terminal tests and review. Its stronger-comparator pair-only saving is 47 body memberships, not the earlier 22.5% differential to upper-only evaluation. See [access, build, storage and timing scope](../../Similarity-Selective-Block-Access.md). This is not a new independent query population.

## SNAP Facebook Combined

Source: [Social circles: Facebook](https://snap.stanford.edu/data/ego-Facebook.html). Download: [combined anonymized edge file](https://snap.stanford.edu/data/facebook_combined.txt.gz). Source-requested citation: J. McAuley and J. Leskovec, Learning to Discover Social Circles in Ego Networks, NIPS 2012. Only the combined edge file is retained; no profile attributes are used.

Downloaded 2026-09-20; gzip integrity check passes. SHA-256: `125e84db872eeba443d270c70315c256b0af43a502fcfe51f50621166ad035d7`. Independent normalization observes 88,234 rows, no loops or duplicates, 4,039 nodes, 88,234 undirected edges and 176,468 directed memberships. Consecutive original-ID target pairs have 2,019 full pairs, three disjoint pairs, 12,651 total shared memberships and maximum pair overlap 169. The final singleton is not counted as a full pair.

Purpose: [budget-admitted overlap-bound integration](../../Similarity-Overlap-Query-Integration.md). Chosen for a more overlapping neighborhood regime before timing, not to select a favorable measured outcome. Queries use the preexisting six-stratum-plus-24-seeded-ID rule. It remains a small real-data research workload, not a physical RAM-budget demonstration. Consult the upstream source's terms before redistribution outside this research checkout; no new dataset license is inferred here.

## Paired Overlap Public Checkpoint

The [GrQc receipt](../../Similarity-Overlap-GrQc-Results.json) and [Facebook receipt](../../Similarity-Overlap-Facebook-Results.json) each contain 810 complete-result checks: 30 sources, nine methods and three rotated-order repetitions. They ran sequentially after terminal tests and the independent code reviewer. Both use B=2, G=4, k=10, seed 920520, common cursor cap 2,048, and fixed pair admission budgets recorded in the JSON. The GrQc sources are reused, not a newly sampled population. Facebook's six degree strata and 24 additional source IDs are retained in its receipt.

All outputs matched the direct-set oracle. The additional pair bound lost on the full-query timing aggregate against interval on both sources despite tighter bounds and small body-read savings. See the linked methods document for preparation, retained storage, runtime and physical-memory exclusions. Neither source demonstrated the symbolic trillion-overlap case or a Neo4j/GDS comparison.
