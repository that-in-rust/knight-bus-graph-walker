# Mojo Rust Duck Debug

**Method:** rubber duck debugging. Instead of a council-of-experts synthesis, this
document explains the Rust+Mojo proposal to a duck, one assumption at a time, out
loud, until each assumption either survives the explanation or breaks mid-sentence.
Rubber ducking works because the act of stating an assumption plainly — with no
audience that can fill in gaps for you — forces the hidden step where the bug lives
to become visible. The "bugs" here are architectural, not code.

The program being debugged: *"Knight Bus should be Rust-first with an optional Mojo
compute lane, bounded by an immutable artifact contract, targeting Mac Mini / Apple
Silicon appliances."*

---

## Session transcript

### Duck question 1: "What problem does Mojo solve for you? Say it in one sentence."

> "Mojo lets us write SIMD/GPU kernels in one language so graph compute gets
> faster on Apple Silicon."

**...wait.** I said "graph compute gets faster" but I didn't say *which* compute,
*for whom*, and *when they're waiting*. Let me re-say it.

Our own corrected cost model (innovation-mega-arch §0) splits buyer-felt time into
`T_open`, `T_materialize`, and query-time gathers; the heavy iterative work
(PageRank power iterations, Louvain passes) runs at **build time**, inside sealed
generations, amortized across every subsequent read. So the honest one-sentence
version is:

> "Mojo may shrink the **build window**, letting a bigger graph fit the same
> refresh cadence on the same Mac Mini."

That's a real benefit, but a different product claim than "faster graph compute."
Nobody is staring at a spinner during the build. **Bug found and fixed:** the
proposal's implicit claim was query-side; the defensible claim is build-side.

### Duck question 2: "Why do you believe a GPU helps the build?"

> "Because PageRank-style iteration is a big regular parallel scan and GPUs love
> that shape."

Say the memory access pattern out loud: for each vertex, gather ranks from
in-neighbors at **irregular offsets** determined by graph topology. That's the
gather. Our whole mega-arch document exists because the gather — random reads,
skewed degrees — dominates cost. The literature the proposal itself cites
(Gunrock, HyTGraph) says GPUs only win on graphs after heroic frontier
engineering, and the RTNS'25 result (Mojo 4–8× slower than Rust on branch-heavy
KMP) is the CPU-side echo of the same fact.

**Surviving version:** the GPU bet is only plausible *because we already reorder*.
ARCH-II degree-rank layout, Hilbert Fold, and Candle File all make the gather less
random. A GPU kernel is a **downstream consumer of the layout innovations**, not a
substitute for them. If Y1–Y3 measurements kill the layout designs, they kill the
GPU lane too. The dependency order is: layout first, Mojo maybe after.

### Duck question 3: "You keep saying '16 GB GPU memory' — is that even a thing on the target hardware?"

> "The doc says a 50 GB graph can't fit in 16 GB of GPU memory and transfers
> become the bottleneck—"

**Stop.** The target is a Mac Mini. Apple Silicon has **unified memory**. There is
no discrete VRAM, no PCIe transfer in the discrete-GPU sense. Half of the
proposal's GPU-tax section is written in a discrete-GPU mental model that doesn't
apply to the stated hardware.

Explaining it correctly to the duck:

- **Good news:** the mmap'd CSR artifact and the GPU can address the same
  physical memory. The host→device copy tax largely disappears (Metal still wants
  its own resource wiring, but not a bulk relocation of 50 GB).
- **Bad news:** GPU working set and CPU page cache now compete for **the same
  16–64 GB**. There is no "extra" memory pool the GPU brings.
- **Consequence:** GPU residency must be **priced by the Budget Machine** against
  the same RAM budget it already guards. A GPU run isn't free capacity; it's a
  tenant in the budget. The receipt gains a line ("peak unified-memory footprint,
  engine=mojo-gpu"), and admission control gains a rule — which is actually a
  *feature*: no competitor's GPU story includes pre-run pricing and refusal.

**Bug found:** discrete-GPU reasoning on unified-memory hardware. Fixed framing:
less transfer tax than the doc fears, tighter capacity contention than it admits.

### Duck question 4: "What exactly crosses the language boundary? Point at each byte."

> "Rust writes an immutable artifact: manifest.json, forward/reverse offsets,
> neighbor arrays, degrees, checksums. Mojo mmaps it read-only, runs a kernel,
> returns a result buffer plus metrics."

Duck asks: "And what does Mojo *never* touch?" — The write path. The journal.
The overlay. Admission decisions. Receipts. The Cypher/Bolt surface. Good.

Duck asks: "Is this a new interface?" — **No, and this is the proposal's best
property.** The sealed-generation snapshot format already *is* an immutable,
checksummed, versioned artifact with a manifest. A Mojo worker is just another
reader of a format that already exists for other reasons. The marginal
architecture cost of the experiment is close to zero, which is exactly what makes
it a cheap option rather than a rewrite. The proposal's line — *"FFI is an
optimization. The artifact contract is the architecture."* — survives the duck
unchanged. It is the best sentence in the document.

Duck asks: "And the C-ABI shared-view phase 2?" — Deferred until a Mojo kernel has
*won* an end-to-end benchmark through the file boundary. On unified memory the
file boundary is already near-zero-copy (mmap of the same pages), so phase 2 may
never be needed at all. Another quiet simplification the discrete-GPU framing hid.

### Duck question 5: "How would you know if the two engines disagree?"

> "Golden fixtures, Rust as correctness oracle, output hash in the receipt."

Say the failure mode slowly: two engines, floating-point accumulation in different
orders, results drift at the 12th decimal, hashes differ, alarm fires — on a
*correct* kernel. Exact output-hash equality is the wrong contract for
float-valued algorithms.

**Fix, from machinery we already have:** reuse the equivalence-tier ladder from
the differential-testing spec — deterministic equality for integer-valued results
(WCC labels, triangle counts, component sizes), tolerance-banded equivalence for
float ranks (canonicalized ordering, epsilon per tier), declared-approximate for
the sketch rung. The Mojo oracle harness is the *same harness* we run against
Neo4j, pointed at a second engine. No new verification machinery; one more column.

### Duck question 6: "Who maintains the second toolchain in year 3?"

> "Mojo requires macOS 15+, Xcode/Metal tooling; Modular controls the compiler
> and its licensing has shifted before—"

Explaining an appliance to a duck: an appliance's promise is *it keeps working*.
Every dependency is a promise we're making on someone else's behalf. Rust's
toolchain promise is a decade of stability governance. Mojo's is a startup's
roadmap. For a *cloud* service that's a manageable risk (we redeploy); for an
appliance in a customer's office it's a supply-chain liability the receipt can't
price.

**Surviving policy:** Mojo kernels are treated like a vendored dependency with an
exit plan — pinned versions, the Rust baseline permanently maintained for every
kernel, a documented kill switch, and the enable/disable decision recorded in the
receipt. The proposal already says "Mojo is never the only path"; the duck adds
"and the Rust path is never allowed to rot, which means CI runs both, forever" —
a real, permanent carrying cost that belongs in the decision, not the footnotes.

### Duck question 7: "Why now?"

> "Because—"

...I don't have a good answer. The repo just landed Cypher/Bolt compatibility
work; the standing queue is the Y1–Y3/Z1–Z3 measurement sprint, the T_open
regression (Hot Boot), and the WCC wedge. Every one of those has higher expected
value per week than a second language, and question 2 established that the Mojo
lane's upside is *gated behind* the layout measurements anyway.

**Surviving schedule:** the Mojo experiment enters the queue only after (a) the
measurement sprint reprices the layout portfolio and (b) one algorithm family
ships with receipts. Then it's the proposal's own bounded one-week trial: same
sealed artifact, PageRank build-side kernel, Rust-vs-Mojo-CPU-vs-Mojo-GPU,
measured on parity tier + end-to-end build wall time + peak unified memory +
cost-per-job. Keep on a customer-visible win (bigger admissible graph, or same
graph on a cheaper Mini); kill otherwise.

---

## What the duck changed

| # | Before ducking | After ducking |
|---|---|---|
| 1 | "Mojo makes graph compute faster" | Mojo is a **build-window** optimization; query path untouched |
| 2 | GPU as independent bet | GPU is **downstream of layout** (ARCH-II / Hilbert / Candle); gated on Y1–Y3 |
| 3 | Discrete-GPU transfer tax + VRAM ceiling | **Unified memory**: little transfer tax, but GPU is a tenant of the same RAM budget → priced by the Budget Machine |
| 4 | New Rust↔Mojo artifact interface | **Existing** sealed-generation format; marginal cost ≈ 0; phase-2 C-ABI likely unnecessary |
| 5 | Exact output-hash parity | **Equivalence tiers** (exact / tolerance / declared-approximate), reusing the Neo4j differential harness |
| 6 | "Optional backend" as a slogan | Vendored-dependency policy: pinned versions, Rust baseline in CI forever, kill switch in the receipt |
| 7 | Implicit "start soon" | Explicitly **after** measurement sprint + one shipped algorithm family; then a one-week keep/kill trial |

## Verdict (unchanged in direction, sharpened in claim)

Adopt: Rust-first, artifact-contract boundary, Rust as oracle, Mojo as optional
backend, end-to-end benchmark contract. Re-scope the pitch from "GPU-accelerated
graph compute" to "**build-window acceleration on known Apple Silicon, priced and
admitted by the same Budget Machine as everything else**." Sequence it behind the
measurements that decide whether its prerequisite (layout regularization) is even
real.

One sentence for the interview, if asked about Mojo:

> "Knight Bus is a Rust appliance; Mojo is a candidate accelerator for the build
> window that has to beat the Rust baseline end-to-end on a fixed Mac Mini —
> and its runs get priced, admitted, and receipted like every other workload."

---

*Caveats: the RTNS'25 KMP result and the unified-memory behavior of Metal
resource wiring are cited from the source proposal and general platform knowledge
respectively; both should be independently re-verified before the trial week.
All speedup expectations here are modeled, not measured.*
