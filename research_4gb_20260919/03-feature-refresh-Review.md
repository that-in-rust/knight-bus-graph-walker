# Reader 03: Feature Refresh Proof Review

Date: 2026-09-20 local. One bounded independent review, performed after completing the assigned raw tool-result coverage. No source changes, implementation, benchmark, agents or web audit. Tiny exact rational calculations ran in an ephemeral Python `Fraction` invocation; no runtime source file was created.

## Findings First

**No counterexample found to D17's stated exact-arithmetic residual identity, new-operator error bound, beta contraction or conditional raw-score equality.** The fixed entity/feature universes, binary pruned incidence, symmetric shared-feature-count graph, fixed normalized p and a, matched snapshot provenance and unnormalized finite output are necessary restrictions. Two lifecycle prerequisites remain insufficiently specified for promotion; neither invalidates the algebra.

### F1 [P2]: Singleton Reactivation Requires Information Absent From A Pruned Artifact

Evidence: [D17 pruning convention](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_4gb_20260919/Feature-Refresh-Certificates.md:27), affected-set discovery at lines 75-79; [D16 singleton removal](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_4gb_20260919/Feature-State-PageRank.md:34), stored profile at lines 176-180.

The formula requires complete S. A reverse index built only from the effective, singleton-pruned B cannot always discover it. Consider a fixed universe of three entities and one feature. Raw old world A has singleton membership {1}; raw old world B has singleton membership {3}. Both produce exactly the same empty pruned analytical incidence. The identical delta "insert membership (2,h)" produces new feature {1,2} in A and {2,3} in B. No function of the pruned artifact plus that delta can distinguish the two correct new graphs. Fixed IDs and an exact event stream do not recover discarded old membership identity.

Required admission condition: retain raw singleton ownership/membership and raw feature counts, or retrieve complete matching-version raw member lists from an explicitly available source. Keep raw cardinality separate from the effective active-column count k used by the norm. A feature dictionary plus k=0 is not enough. Charge the extra persisted information, retrieval and source retention. D16 already warns about retaining provenance; D17 needs to make this dependency concrete for its refresh portfolio. The favorable all-large-group example avoids this issue initially, but the general membership-change profile does not.

### F2 [P2]: A Passing Residual Gate Still Needs Index And Block-Copy Admission

Evidence: [D17 reverse portfolio](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_4gb_20260919/Feature-Refresh-Certificates.md:105), failed gate at lines 107; postings/compaction at 77 and old-reader pinning at 91; [conditional score reuse](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_4gb_20260919/Feature-Refresh-Certificates.md:95).

The quoted 44.040GB and 52.040GB arithmetic is correct, but the score-plane fallback is not the only losing branch. Under the same retained-old-plus-new policy, keeping a newly materialized full reverse-posting index beside the old 44.040000008GB portfolio takes **52.048000016GB**, before changed rows, scores or metadata. In compact CSR-like postings, deleting one entry and inserting another can shift long intervening ranges. Its small logical delta does not prove small physical block replacement. A bounded overlay, slotted representation or admitted compaction/reclamation schedule is needed, not an assumed cheap packed-file edit.

Similarly, few changed score rows need not imply few changed bytes at the chosen copy-on-write granularity. An 8GB score plane divided into 4,000 blocks of 2,000,000 bytes can have one changed row in every block. Even a roughly 4,000-row affected set can then force replacement of the entire plane. This is a layout counterexample, not a prediction for typical data. A row overlay or finer-grained sharing can avoid it, but has its own lookup, storage and compaction cost.

Required admission condition: check physical bytes of changed row blocks, postings, score blocks, metadata/checkpoints, old-reader retention and pending compaction before accepting the refresh. Treat the no-extra-iteration branch as conditional on both the error gate and this storage gate. D17 acknowledges overlays and retention; the missing part is a concrete passing and fallback file-lifetime schedule. Do not silently remove the only valid old result, classify retained new indexes as free scratch, or assume readers release immediately.

## Proof Audit

### Residual Identity And The Global Term

D17 lines 44-58 correctly use all rows whose effective B or w changes. With the same retained s and fixed p, a, IDs:

```text
delta' = c'b' + M's - s
       = delta + (c'-c)b + c'(b'-b) + (M'-M)s.
```

The row sums for Delta_b and Delta_M_s are correct; no dense feature matrix is needed. The coefficient on Delta_b is c', not c. Changes in dangling membership alter p0 and c even when only a small local relation changes. The global term can be streamed over F entries; it is not a local-row-only operation. Old residual provenance must identify the exact state, operator, parameters and arithmetic certificate; an old bound alone is not the signed residual vector needed by this identity.

Independent exact counterexample to **omitting** `(c'-c)b`: use D17's one-feature fixture (old {1,2}, new {2,3}, a=1/2, p=(1/2,1/2,0)), but retain s=1/3 instead of the old stationary s=1. D17 explicitly permits nonstationary retained state.

| Quantity | Exact value |
|---|---|
| old c, b, M | 1/2, 2/3, 2/3 |
| new c', b', M' | 2/3, 1/3, 2/3 |
| delta_old | 2/9 |
| `(c'-c)b` | 1/9 |
| `c'Delta_b` | -2/9 |
| Delta_M_s | 0 |
| Incorrect residual without global term | 0 |
| Correct delta_new | 1/9 |
| New raw x'(s) | (1/3, 1/3, 1/9), mass 7/9 |
| New stationary x'_star | (1/3, 4/9, 2/9) |
| Actual L1 error and E_new | 2/9 |

Thus omission would falsely certify exactness, not merely change a conservative bound. D17's complete expression passes. Its published stationary-old-state fixture at lines 125-140 also checks algebraically: delta_new=-1/9 and actual error=2/9. The warm fixture is stronger as a regression test for the omitted-global-term bug because it produces a false zero.

### Affected-Set Closure

Membership edits change raw feature counts. Count/eligibility changes affect effective rows and degrees of all relevant old/new members. Include removed members, newly reactivated singleton members and entities whose B row is unchanged but w changes. For each affected entity, scatter over its **entire** old/new effective row, not only the feature named in the input edit.

Exact degree-only fixture: four entities, old feature f={1,2}, g={2,3}, entity 4 isolated; insert (4,f). Set a=1/2, uniform p. Entities 1 and 2 keep their B rows, but their w values change from 2/3 to 2/5 and 1/3 to 1/4 respectively. Entity 3 keeps w=2/3. The correct S is {1,2,4}, not merely {4}.

```text
old b = (1/4, 1/4)
new b = (21/80, 11/48)
true Delta_b = (1/80, -1/48)
Delta_b from edited entity 4 alone = (1/10, 0)
```

Feature g's membership and count did not change, yet its b component did because member 2's degree changed. At s=0, c'=1/2, omitting that degree-affected contribution misses -1/96 in the g residual. D17's definition covers this; an implementation limited to edited membership rows or scatters limited to count-changing features would not.

This dependency closure is finite at this stage: after determining raw count changes and effective membership eligibility, degree changes do not themselves change other raw feature counts. There is no need to recursively add every entity sharing a feature whose residual entry changed. The residual update may touch many feature entries; subsequent global solution changes are handled by the solver and output rules, not by pretending all affected scores lie in S.

### New-Operator Error And Further Work

For reconstruction using new B', d', w', c', D16 gives the entity residual `r'=a B' delta_new`, including dangling redistribution. The induced P' is column-stochastic. Hence `||(I-aP')^-1||_1 <= 1/(1-a)` and:

```text
||x'(s)-x'_star||_1 <= a/(1-a) * sum_h k'_h |delta_new[h]|.
```

This proof does not require s to be the old exact solution, positive residuals or a normalized reconstructed x. It does require the same new snapshot in both reconstruction and the certificate. Inactive slots have k'=0 and no incidence contribution, so the weighted quantity is a seminorm on all slots and a norm on the active subspace. Discarding inactive-slot influence is justified only because their new operator columns are exactly zero. The all-dangling/a=0 branches correctly return p directly.

For the new active operator, `(k'^T M')_h <= beta' k'_h`, where `beta' = max_active_i a(d'_i+q'_i)/(d'_i+a q'_i) <= 2a/(1+a) < 1`. The fixed-point residual after forming `s_next=s+delta_new` is exactly `delta_next=M'delta_new`. Therefore `E_next <= beta'E_new`, and `beta'^n E_new` is a valid sufficient bound for n further updates. Returning s_next is justified with this propagated bound, not automatically with a stronger unchecked one.

In the warm fixture above, s_next=4/9 and the actual new-state error is 4/27. This equals beta'E_new for beta'=2/3. Substituting a for beta' gives only 1/9, which is false. The first update really can reuse delta_new without another incidence pass. For n>=1, the generic remaining update work is n-1 new-operator applications, plus initial delta discovery, reductions, reconstruction and export. For n=0, those discovery/output costs still remain.

A tighter data-specific beta' must be valid globally after changes. Maintaining a maximum under deletions needs a valid summary, a scan or a conservative retained bound. The universal post-pruning bound avoids that extra max-discovery problem but can imply more passes. Near-one a, outward rounding, residual cancellation and error accumulation can make the numerical or time gate fail. The document appropriately does not claim that the f64 sweep establishes production interval certification.

### Output State And Normalization

The bounded output is x'(s), not x_old(s), x'(s_next) without its propagated bound, or normalized x'(s) without an adjusted contract. In the warm fixture, returning the old raw plane `(5/18,5/18,0)` gives new-target error **4/9**, exceeding E_new=2/9. A correct state-reuse decision therefore does not generally justify answer-plane reuse.

The base normalization fixture was also checked exactly: old raw error 4/33 becomes 72/319 after normalization, larger than the original certificate. For any nonzero-mass raw vector with target mass one, `||x/sum(x)-x||_1=|1-sum(x)|` when x is nonnegative; together with the original error this gives the stated general 2E bound. Output normalization needs the mass and any additional pass, not merely a formatting flag. Warm starts do not inherit the base's zero-start monotonicity/mass-deficit equality.

## State And Disk Audit

The three-array serial schedule is implementable at the algebraic level: load s; initialize a residual array from the matching old residual or old next-minus-current checkpoint stream; zero Delta_b; accumulate changed-row contributions into the latter two arrays; compute c'; stream old b to update the residual and emit b'. Counts and b can be streamed. This does not require keeping old next or b as additional full resident arrays. If they are retained in RAM for convenience, charge them.

| Selected component | Exact decimal bytes |
|---|---:|
| Three f64 feature arrays at F=1,000,000 | 24,000,000 |
| D16 named prepared portfolio | 36,032,000,000 |
| Reverse postings `4I+8(F+1)` | 8,008,000,008 |
| Combined named portfolio | 44,040,000,008 |
| Remaining under 50,000,000,000 | 5,959,999,992 |
| Old portfolio plus one new full score plane | 52,040,000,008 |
| Old portfolio plus one new full reverse index | 52,048,000,016 |

Thus **24MB selected mutable state is correct**, not 24MB whole-process or whole-machine RAM. Error intervals, row buffers, dictionary/count state, source/ID lookup, merge runs, runtime/native buffers, OS/file residency, concurrent readers and refresh all require the remaining physical 4,000,000,000-byte envelope. The 50GB limit here is prepared disk, not historical GDS projected RAM. Source exports and build scratch still need separately declared peak-device space; if the entire device is capped at 50GB, all simultaneous bytes must share it.

The favorable changed-row count is conservative, not a hidden error: initial departing set size 2,000 and new arriving set size 2,001 both include the changed entity, so their union is at most 4,000 when otherwise disjoint. D17's "about 4,001" and 8,002 memberships are safe upper allowances. Roughly 250,000 fewer row memberships is a selected work-count comparison, not a measured refresh ratio. Postings access, row-image reads, F-wide reduction, validation and block publication can dominate it.

First-answer cost still includes raw acquisition, bounded external build, optional reverse-index construction, first feature solve and complete result delivery. A failed refresh gate includes discovery already spent, further full row passes, and regenerated output; it is not just the rerun kernel. At N=1B, an all-entity ID-plus-f64-score export is 16GB before framing even when unchanged score blocks are shared. Source consistency and publication must remain atomic through crashes and reader-pinned generations. These are admission conditions, not properties established by the arithmetic fixtures.

## Conditional Raw-Score Reuse

D17 lines 95-97 are algebraically correct: fixed c, s, p and an unchanged B_i, d_i, w_i give the same raw reconstructed score. The old score plane must actually have been produced from that identical retained state and numeric/output contract. A plane produced from the old stationary solution cannot be paired with a different approximate checkpoint. Snapshot and state provenance are therefore part of reuse eligibility, not just metadata bookkeeping.

The condition is sufficient, not necessary; some scores might coincidentally remain equal after a parameter change, but that is not a license to reuse all other blocks. A new global normalization mass, advanced s, changed c, score sorting/top-k, original-ID remapping or lossy storage can invalidate larger output artifacts. Unchanged mathematical values also do not prove byte-identical independently recomputed floating results. Reusing a previously stored block requires a compatible encoding/provenance contract and a charged physical sharing mechanism. Full exported results remain full-sized.

## Coverage And Check Record

| Source | Exact fully consumed spans | Unique observation |
|---|---|---|
| Feature-State-PageRank.md | 1-140 and 141-267; all 267 lines, nontruncated | Singleton deletion erases membership information needed by a general refresh; raw finite reconstruction and its normalization contract are essential. Norm contraction enables newer-state output without another certificate-only scan. |
| Feature-Refresh-Certificates.md | 1-152; all 152 lines, nontruncated | Complete w-affected rows and the global c term make the update exact; compact reverse-index replacement can fail the retention cap even when the residual gate passes. |

Read versions (hashes are identity checks, not reading evidence): D17 `ffc49033f800cb071155032b0f9e4b1328d2825b449076432618bc538150faa9`; D16 `97b414f2f3d583f564de911d661908c4ec83aa1547e3e32f2f24ef79f6fb8958`.

Executed exact arithmetic assertions: omitted-global-term false zero; correct warm residual/new reconstruction error; s_next and beta bound; old-plane error; degree-closure b differences; normalization error growth; selected RAM/disk byte sums. All passed. The singleton indistinguishability and block-retention examples are direct mathematical/storage counterexamples, not performance tests. The lead's 110,592 f64 checks were read as reported evidence and were not rerun or relabeled as independent results.

Review complete: 419/419 source lines read, no unread span in these two documents. No linked papers were fetched, no worldwide novelty assessment was performed, and no large test or production implementation was created. Promotion remains conditional on the already requested numerical, source-consistency and lifecycle experiments, with F1/F2 made explicit. This completes the single focused review; it does not reopen the earlier corpus or claim the overall nine-folder goal complete.

## Coordinator Resolution

After this review, the lead amended D17 to require matching-version raw singleton/cardinality provenance, distinguish effective from raw counts, and charge a possible sidecar. It also added a separate physical storage gate, versioned posting/row/score overlays, explicit publication and compaction lifetimes, and fallback/declined-refresh behavior. The new verification list includes both findings' counterexamples. These are documentation resolutions; the review's source hashes and line counts identify the earlier reviewed versions, and no follow-up implementation or production verification is claimed.
