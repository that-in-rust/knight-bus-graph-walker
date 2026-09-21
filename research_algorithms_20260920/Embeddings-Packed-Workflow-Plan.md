# Packed Embedding Workflow Implementation Plan

**Goal:** Falsify or demonstrate a useful packed-storage frontier for the A06 mean-history candidate, including input preparation, complete normalized output and a strong external-node-state control.

**Architecture:** An isolated standard-library Rust research executable consumes a canonical node-feature source, validates/copies it, and runs one of three matched plans. An independent Python rational oracle verifies small complete results. Large runs measure fresh-process time/RSS and exact file equality between the same-mean methods. No production Knight Bus API is changed.

**Tech Stack:** rustc 1.93.1, Rust standard library, Python 3.11 Fraction oracle, local filesystem and `/usr/bin/time -l` on this Darwin arm64 host.

## Research Boundary

This continues the approved seven-family research design; it is not a new product scope. Graph-native overlapping features are supplied, not discovered from arbitrary edges. General CSV parsing, external ID mapping, factor discovery and Neo4j export are outside this input contract and cannot be omitted from a later end-to-end Neo4j claim. File preparation for the declared source is included. A small native run is not a physical 4 GB machine experiment.

## Files And Ownership

- `experiments/embedding_packed_workflow.rs`: isolated engine and its unit tests; implementation agent.
- `experiments/verify_embedding_packed_results.py`: independent whole-output rational oracle; lead.
- `Embeddings-Packed-Workflow-Evidence.md`: commands, executed receipts, tradeoffs and failures; lead.
- Existing mean manuscript, synthesis and journal: lead integration only.

No Cargo workspace changes. No commit or push in this continuation.

## Executable Contracts

| ID | WHEN | THEN SHALL |
|---|---|---|
| EP01 | A canonical source is supplied | Validate header, exact length, ordered dense node IDs, distinct/in-range features and active cardinalities; account its prepared copy |
| EP02 | Packed histories are accessed | Preserve signed integers at feature-local byte widths, reject overflow, keep one shared layout rather than L duplicate offset tables |
| EP03 | A mean layer is frozen | Compute exact current sums, collect upward dyadic mean defects, stream compact means, release wide sums, reload means; charge conversion I/O |
| EP04 | Same-input mean and disk modes execute | Produce bit-identical complete normalized output and use the same rounding, clip, initialization and signed layer weights |
| EP05 | A result is admitted | Include all output rows, actual returned binary64 conversion, observed transition errors and norm minima before success publication |
| EP06 | An independent small oracle is run | Compare all coordinates against dense exact rational propagation plus a rigorous normalized enclosure |
| EP07 | A performance run finishes | Report phases, logical bytes read/written, live packed-plane peak and peak file bytes; separately measure fresh-process RSS and wall time |
| EP08 | Any run fails admission or parsing | Do not create a successful final result; retain an honest failure receipt, not a passing subset |

## Stable Interchange

CLI:

```text
generate SOURCE N F SEED
run MODE SOURCE OUTDIR D L B TAU
```

MODE is `sum`, `mean`, or `disk`. OUTDIR must not exist before a run. Source generation is separate from supplied-input execution; preparation is timed inside each run.

Source: magic `KBMEAN01`, followed by N, F, SEED as little-endian u64; N records `(node_id:u64, feature_0:u32, feature_1:u32)`. Header is 32 bytes, record is 16. IDs are 0..N-1. N>=2F, F>=2. The generator makes feature_0=i mod F and feature_1 from the seeded mix below, using a small hot-feature subset for every fifth node, then increments modulo F if it collides with feature_0. Require every used feature cardinality>=2. Weight_f=1+(f mod 17).

Stable initialization: M=2^B. Dimension zero is always M, so these fixtures deliberately have a nonzero norm floor. Other dimensions use `splitmix64(SEED xor (i*C1) xor (j*C2)) mod 3 - 1`, with wrapping u64 arithmetic, C1=0x9e3779b97f4a7c15 and C2=0xbf58476d1ce4e5b9. Here splitmix64 means x+=C1; x=(x xor x>>30)*C2; x=(x xor x>>27)*0x94d049bb133111eb; return x xor x>>31. This is a declared benchmark initialization, not the DFHR-v1 SHA profile or a downstream learning-quality test.

Layer coefficient numerator is 2 for even t, -1 for odd t, all over 8, including t=0. Means use p=0. Sum mode normally uses B=24; mean/disk use B=26 for the no-worse worst-case raw bound, but oracle inputs use the same real ternary R.

Successful output: magic `KBOUT001`, followed by N,D,L,B as little-endian u64; N records `(node_id:u64, D binary64 values)`. Header is 40 bytes. No rounded-zero shortcut at positive observed error. Sum versus mean can differ within their declared absolute tolerance; mean versus disk at identical B must match exactly.

## Numerical Procedure

Use exact i128 weighted numerators and exact quotient/remainder ties-to-even. Reserve signed packed cells with `2+bit_length(max(1,bound))` bits rounded to bytes, using t_f*M for exact sums and M for p=0 means. Handle zero/divisor operands independently. Restrict experiment dimensions/ranges to a proven i128/isqrt envelope and reject unsupported values; this is not an unbounded-bigint implementation.

Normalize integer rows using outward integer square-root intervals, with p_s=p_o=28 and a common output endpoint scale 2^(28+3) after coefficient multiplication. Bound maximum sums so endpoint sums are exactly convertible to binary64; certify the actual output bytes. Collect mean and pre-clipping division defects upward at 32 fractional bits. Use the observed-defect gate already derived in the mean manuscript. Its O(L) maxima/minima do not include the paid local O(L*D) replay slab.

Do not decide certification by an ordinary floating-point sum of error bounds. Candidate midpoint `(lo+hi)*2^-32` is exactly binary64 under the admitted range; its interval error is `(hi-lo)*2^-32`. Round each normalized beta upward to 32 fractional bits using integer division, multiply by absolute alpha numerators, and accumulate with common denominator 2^35, including the numeric error multiplied by eight. Parse TAU as an admitted exact decimal and compare against its downward-rounded 2^-35 threshold. Merely displaying a floating error estimate is not proof of the acceptance decision.

The disk control stores one raw i32 node plane and a disk-backed pair of i64 normalized-output endpoints per coordinate. It may update both in place through independent sequential reader/writer file handles, provided writes never overtake consumed input. Its frozen means make each next node depend only on that node's old value and the means, not another old row that might be overwritten.

Use the stronger fused schedule: initialize raw state, initial output endpoints and H0 in one source pass. After each means freeze, one pass updates the node, adds its normalized output contribution, and scatters its new state into the next H accumulator. Drop old means before converting Hnext. Peak kernel planes are one frozen mean plus one next exact-sum plane, not L mean histories. This avoids both quadratic replay and an unnecessary second raw-state pass per layer, at the cost of node/output disk traffic. No expanded adjacency is required. A failed in-place transient state may be rebuilt from the validated source; this is not crash-safe incremental checkpointing. If implementation instead keeps old/new files, count their overlap and do not call it the strongest state-lifetime baseline.

## Execution Steps

1. Write packed-cell, signed-rounding, normalizer and malformed-source tests; observe the missing implementation fail.
2. Implement shared arithmetic, layouts and bounded source scans; run those tests.
3. Add each mode and tests of complete result equality/refusal; include all layers, not only final-layer output.
4. Write and run the independent rational oracle on all three modes, with varied overlaps and weights.
5. Run fresh processes on a medium source with identical mode inputs. Capture `/usr/bin/time -l`, full receipts, output lengths and `cmp` for mean/disk.
6. Record both gains and losses. A lower packed-plane count is not a whole-process RSS gain; more scans or conversion can make means slower.
7. Update the research audit from actual results. Do not declare seven innovations or physical-budget compliance from this experiment.

No blanket production test-suite run is relevant to this isolated harness. Compile it with warnings denied, run its tests, and verify the exact research claims exercised here.

## Evidence-Driven Extensions

The original three-mode plan above is retained as the V1 contract. V1 has now been implemented and measured; see [the evidence file](Embeddings-Packed-Workflow-Evidence.md). Its exact source/runner/oracle versions are archived beside the receipts.

V2 adds `arena`, the same mean operator and exact publication certificate using one paid history allocation with forward in-place compaction. Its contract and invariant are in [arena compaction](Embeddings-Arena-History-Compaction.md). Seven small source cases, including unused feature columns, pass the independent oracle against claimed bounds.

V3 implements `disk6`, the lossless C6 endpoint recoding in [center/certificate storage](Embeddings-Center-Certificate-Storage.md). The eager disk pass schedule was held unchanged: 21 native tests, seven-source independent verification and 18 matched measurements completed. It saves temporary disk/traffic but was slower on all three cached workloads. C5 shared radii and the blocked executor remain separate, unimplemented follow-ons; do not bundle their effects into a C6 claim.
