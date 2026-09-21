# Exact Centers And Small Certificates For Disk Embeddings

Date: 2026-09-20. A06 storage derivation following the measured disk-state win. C6 is implemented, independently verified and measured. It saves disk/traffic but is slower in the cached experiment. C5 remains a proposal. Projected large-workload deltas are arithmetic, not measured speedups.

## Problem Revealed By The Complete Workflow

The fused disk engine keeps raw node state in 4 bytes per coordinate and two accumulated normalization endpoints in 16 bytes per coordinate. Its small RAM requirement is genuine at the packed-plane level, but repeated endpoint traffic dominates the file schedule. The complete result is the midpoint of those endpoints; their spread is used only for the numeric error certificate.

Can that distinction eliminate storage without changing answers or weakening correctness? Yes, under the exact existing fixed-scale and bounded-depth contract. One option preserves both answers and the original admission decision. A still smaller option preserves answers but may reject more jobs because it uses a shared error radius.

## Fixed Numerical Contract

The current normalizer encloses a row coordinate using signed integers `(lo,hi)` at scale 2^-28. Layer t has signed integer coefficient a_t, equal to 2 for even t and -1 for odd t, followed by a factor 1/8. There are L+1 layers, with 0<=L<=64. Final candidate output is an exactly representable binary64 dyadic under the existing arithmetic admission.

For node i and coordinate j define

```text
center_ij = sum_t a_t * (lo_tij + hi_tij)
width_ij  = sum_t abs(a_t) * (hi_tij - lo_tij)

candidate_ij = center_ij * 2^-32
numeric error bound_ij = width_ij * 2^-32.
```

These are integer operations. In particular, do not round each layer's midpoint to a lower-precision float before accumulation. The graph-propagation/quantization error is a separate term from the existing observed-defect gate and is not removed by this encoding.

## Two Elementary Range Lemmas

Let z be a nonzero integer row, S=sum_j z_j^2 and ell=floor(sqrt(S)*2^28). For a nonzero coordinate x=z_j, ell>=abs(x)*2^28. The positive-coordinate interval before integer outward rounding has endpoints `abs(x)*2^56/(ell+1)` and `abs(x)*2^56/ell`. Their distance is

```text
abs(x)*2^56 / (ell*(ell+1)) < 1/abs(x) <= 1.
```

Consequently outward floor/ceil gives an integer endpoint width at most two. Negative coordinates reverse signs and obey the same bound; zero coordinates and zero rows give width zero. The same denominator inequality gives `abs(lo),abs(hi)<=2^28`.

Write A_L=sum_(t=0)^L abs(a_t). The current coefficient schedule has A_64=98, so at every partial stage

```text
abs(center_ij) <= 2^29 * A_L <= 52,613,349,376
0 <= width_ij <= 2*A_L <= 196.
```

A signed 5-byte integer therefore suffices for every center through depth 64; one unsigned byte suffices for its accumulated width. At depths 0 and 1 the center fits four bytes. These reservations are independent of feature cardinality, node count and the node-grid exponent B, subject to the normalizer's already enforced integer-overflow envelope.

## Format C6: Lossless Endpoint Recoding

For 2<=L<=64 store `(signed center:5 bytes, unsigned width:1 byte)` for every node-coordinate pair. At L<=1 a 4+1-byte record suffices. Raw propagation state stays unchanged.

Initialization and each fused transition add the new layer's signed center increment and nonnegative width increment. Decode one record, update it in admitted integer scratch, then overwrite the consumed record through the existing independent sequential cursors. At the final pass emit exactly the same `center*2^-32` as before and reduce `max(width)` for the original numeric certificate.

Let the old accumulated endpoints be (L_ij,U_ij), at scale 2^-31. Exact identities give

```text
center_ij = L_ij + U_ij
width_ij  = U_ij - L_ij

L_ij = (center_ij - width_ij)/2
U_ij = (center_ij + width_ij)/2.
```

Center and width have matching parity, so reconstruction is integral. C6 is therefore a lossless recoding of both endpoints, not a new approximation. It preserves the final output bytes, numeric width maximum, propagation-defect terms and acceptance decision. Its update order and in-place cursor safety are the same as the existing disk engine.

```text
Old disk coordinate:       New C6 disk coordinate:
+--------+--------+        +----------+-------+
| lo:8 B | hi:8 B |        | mid:5 B  | w:1 B |
+--------+--------+        +----------+-------+
          |                          |
          +----- identical value ----+
          +----- identical gate -----+
```

At the current bounded depth, endpoint state falls by 62.5% (16 bytes to 6). Combining it with the unchanged 4-byte raw node state halves the two disk planes, from 20 to 10 bytes per coordinate. The complete binary64 output remains unchanged and cannot be counted as a saving.

## Format C5: Lift Widths Into A Shared Certificate

Alternatively store only the 5-byte center. While each layer is normalized, collect

```text
r_t = max_(i,j) (hi_tij-lo_tij)
R = sum_t abs(a_t)*r_t.
```

The same candidate obeys a uniform numeric error bound R*2^-32. The reduction needs O(L) scalars, or one running total for layer-ordered execution. The analytic bound R<=2*A_L is also valid if we choose not to measure r_t. The publication gate substitutes `8*R` for `8*max(width_ij)` in its common 2^-35 integer scale and retains the original propagation-error terms.

This saves the width byte but can enlarge the certificate. In general `max_ij sum_t` is less than `sum_t max_ij`; equality must not be assumed. For example, with weights (2,-1), width sequences (2,0) and (0,2) have original global width four but shared width six. A tolerance between those bounds may admit C6 and reject C5 despite identical candidate output. C5 is appropriate only with explicit admission accounting or enough verified error slack.

The two choices illustrate the actual product decision: C6 keeps acceptance unchanged; C5 spends possible certificate slack to save more storage. Neither uses a signed radius sum: absolute coefficient weights are necessary.

## Exact Eager-Disk Schedule Delta

Let q be the new endpoint-record width, n the node count and d the dimension. Hold all other phases and the current eager initialization/final pass fixed. There is one initial endpoint write, L read/write transition passes, and one final endpoint read. Then

```text
endpoint footprint saved = (16-q)*n*d
logical traffic saved    = 2*(L+1)*(16-q)*n*d.
```

These formulas are for the actually implemented eager pass schedule, not an endpoint-optimized future baseline. The legacy and C6 file/traffic entries below were confirmed by the completed V3 runs; C5 entries remain unexecuted projections.

| Cardinality Case | Legacy Endpoints | Measured C6 | C5 Projection |
|---|---:|---:|---:|
| Bytes per endpoint record | 16 | 6 | 5 |
| Raw plus endpoint plane bytes | 335,544,320 | 167,772,160 | 150,994,944 |
| Peak owned-file bytes | 476,053,576 | 308,281,416 | 291,504,200 |
| Total logical read+write bytes | 6,171,918,728 | 3,152,019,848 | 2,850,029,960 |
| Complete result bytes | 136,314,920 | 136,314,920 | 136,314,920 |
| Candidate values | V1 reference | Identical by recoding | Identical by center sum |
| Admission decision | V1 reference | Identical by recoding | May be more conservative |

For the older illustrative n=20 million,d=64 scenario, the two state planes would fall from 25.6 GB to 12.8 GB with C6, or 11.52 GB with C5. These are not datasets we ran. Preparation, conversion, final output, source retention and generation overlap still require separate charges. Packed 5-byte access can add arithmetic or alignment overhead; less logical traffic does not prove lower latency.

The same format substitution applies to the [blocked-history schedule](Embeddings-Blocked-History-Frontier.md) by replacing its A=16*n*d with q*n*d. Its checkpoint work and RAM formulas otherwise remain unchanged. This can shift the preferred block partition; it does not establish a new globally optimal schedule.

## Verification And Prior Art

The separate algebraic probe `experiments/probe_embedding_center_storage.py` checks exact normalizer enclosures, the two range lemmas, endpoint/center equivalence, signed packing, same midpoint output and shared-radius containment. It also retains a signed-radius counterexample and an explicit shared-certificate overestimation example. Its execution status is recorded below after running; it is not a graph-propagation or filesystem-lifetime test.

Executed with `python3.11 experiments/probe_embedding_center_storage.py` from this research folder, exit 0: 5,460 normalizer rows / 20,510 coordinates, 80 algebraic layer traces / 1,000 final coordinates, 65 depth-width checks, both projected file/traffic rows and two negative examples. Traces use arbitrary integer rows to test the representation algebra; they are not 80 graph datasets. The independent high-precision rational enclosure supplies the numeric reference. No speed or OS-memory measurement is produced by this probe.

Midpoint-radius arithmetic is established. [Johansson's Arb paper](https://arxiv.org/pdf/1611.02831), introduction inspected in primary full text, explains the precision/storage advantage of centers and smaller radii and explicitly discusses extension to normed vector spaces. That is close precedent for the shared-radius option, not merely unrelated background. This proposal specializes exact dyadic centers, a provable byte-sized accumulated width and a fixed graph-output schedule. A competent baseline can use the same encoding; empirical benefit alone will not demonstrate a new interval-arithmetic principle.

The current FLINT documentation URL returned HTTP 403 in this research pass. No unseen documentation claim is used. The paper, not search snippets alone, supplied the prior-art comparison.

## Executed C6 Verification

The `disk6` mode implements C6 using the same fused in-place schedule as `disk`. Its compact reader reconstructs both old endpoints exactly, so the existing normalization accumulation and publication logic are reused. Receipts expose `endpoint_record_bytes` and `endpoint_codec`; declared file admission now prices the actual record width with checked length arithmetic. No C5 radius lifting or initial/final pass elision is included.

The implementer observed the new tests fail before implementation. The lead compiled the completed test/release binaries with `-Dwarnings` and overflow checks enabled: all 21 tests passed. They include every depth 0..64 in codec bounds, full trajectories at depth 64, parity/range rejection, both record lengths, independent cursor crossings, same failure decisions and exact file/traffic savings.

The lead's external suite then passed 14 complete runs over seven sources, including inactive feature columns. All 1,176 coordinates were checked against each receipt's claimed error bound using independent exact rational propagation. Seven full-file comparisons and seven exact publication-bound comparisons passed. Each case also passed the predicted read, write, packed-plane and peak-file identities. Four precision/truncation cases refused publication. See [V3 verification receipts](experiments/packed-verification-v3.jsonl). These are deliberately overlapping controls on seven sources, not 14 unrelated datasets.

The declared-length unit check additionally demonstrates an arithmetic capacity change: for n=20 million,d=128, the old eager schedule's source/prepared-worker files exceed 50 GB while C6's declared files are below it. This is a length-planning result, not an executed giant job, a physical-RAM result, or proof that refresh/retained-input overlap also fits.

## Completed Measurement And Its Consequence

The matched V3 experiment completed 18 fresh-process runs and nine whole-output, exact-bound and accounting comparisons. In cardinality/alignment/depth order, C6 reduced logical traffic by 48.93%/46.66%/48.33% and peak owned files by 35.24%/34.78%/33.90%. Median elapsed time increased by 8.14%/6.98%/5.43%. Median maximum RSS changed by +2.25%/+11.59%/-0.95%, while the accounted packed-plane reservation was identical. All raw repeats, ranges and the distinct OS footprint metric are retained in [V3 evidence](Embeddings-Packed-Workflow-Evidence.md#v3-lossless-endpoint-results).

Thus this is a verified temporary-storage/traffic reduction, not a demonstrated RAM or speed reduction. The result answers the original codec question without changing the eager/final-pass schedule. It also shows why a 62.5% endpoint-plane reduction must not be advertised as 62.5% less total memory or completed-job cost.

Next evidence should test the actual limiting resource: constrained/cold I/O, complete build/refresh/output lifetimes and a real eligible source. Precision must be selected fairly before claiming a minimum-memory frontier. An equalized legacy record-call implementation and a common optimized normalizer would help attribute runtime, but neither would establish novelty. C5 acceptance loss and blocked replay are separate possible experiments, not mandatory routes merely because the formats exist. The scientific priority is a contribution beyond generic factor-aware replay and established midpoint-radius arithmetic.
