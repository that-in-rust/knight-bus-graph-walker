# Community And Triangle Probe Evidence

Date: 2026-09-20. Research evidence, not production code/scaffolding. The source retains the original algebra probes and adds actual cached community procedures and virtual-tape capacity-forced triangle execution. It does not demonstrate physical 4 GB execution or a real disk/storage lifecycle.

## Reproduce

Node.js v24.9.0 is the execution runtime used here. From the repository root, run the single JavaScript fence without creating another file:

```sh
awk '/^```javascript$/{active=1;next} /^```$/{if(active){active=0;exit}} active' research_algorithms_20260920/community-triangles-Probe-Evidence.md | node
```

The program throws on its first mismatch. Original result groups `triangles`, `communities`, and `weightedCommunities` are unchanged. New groups are `cachedCommunities`, `integerPrefixes`, and `capacityJoin`. Counts from different tiers are not interchangeable.

| Tier | Actually executed | Deliberately outside this test |
| --- | --- | --- |
| Original triangle algebra | Full local identity and sparse `p` edge stream versus triples / explicit `K E^2` | Bounded external execution |
| Original community algebra | Expanded scores, binary prefix search, final/multisweep equality | Compressed cache/interval data structures |
| Revised community procedure | Sparse type-community counts, one indexed max-heap per type, two key updates per scalar/event, streamed extrema oracle, BigInt division, interval splits/merges and refusal-before-mutation | External index pages, crash atomicity, production width/admission |
| Revised triangle R procedure | Bounded run buffers, insertion-sort runs, two-way merge passes, oversized moment tiling, sorted `(u,type,p)` join, streaming destination reduction | Actual files, OS caches, byte packing, source interpretation |
| Revised complete small triangle procedure | Type-once `G`, support-only `J`, signed degree/ID forward intersections, bulk/exception merge | Type-matrix tiles and external residual counters |

The virtual-tape machine stores backing records in JS arrays but accesses them through counted tape reads/writes. Its transient record arena is deliberately capped at 5, 7 or 11 slots. A run uses `cap-1` records plus one insertion temporary; a merge uses two current input records and one output record. Moment tiles reserve their target accumulators plus fixed cursor/output slots. Tape contents, oracle graphs/vectors, template storage and JS runtime are **not** charged to this logical arena. Sort runs use two ping-pong tapes and arithmetic run offsets, not an unbounded in-memory run catalog. `peakLive` counts external logical records, not bytes/RSS; record types have different packed widths in the algorithm note. Fixture and final oracle expansion loops are outside candidate counters.

In revised communities, expanded adjacency is used only by the independent BigInt objective oracle. Candidate scores come from sparse counts; every scalar decision, serial gain receipt, aggregate gain and event boundary is compared, not only the final sweep. Validation scans and oracle expansions are not counted as candidate heap operations. The interval model uses arrays with `O(z)` replacement/search, so it tests semantics but does not establish the paper's indexed-interval complexity. Small triangle Number arithmetic remains exact at the tested magnitudes; wide community cases use BigInt.

## Executable Source

```javascript
"use strict";

function create_zero_matrix_array(n, m = n) {
  return Array.from({ length: n }, () => Array(m).fill(0));
}

function assert_exact_equal_values(actual, expected, context) {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(JSON.stringify({ context, actual, expected }));
  }
}

function verify_triangle_stencil_identity() {
  const n = 5, q = 3, cls = [0, 0, 1, 1, 2], sizes = [2, 2, 1];
  const typePairs = [], edgePairs = [];
  for (let a = 0; a < q; a++) for (let b = a; b < q; b++) typePairs.push([a, b]);
  for (let i = 0; i < n; i++) for (let j = i + 1; j < n; j++) edgePairs.push([i, j]);
  let cases = 0, localChecks = 0, pChecks = 0, degreeChecks = 0, signChecks = 0, templateChecks = 0;
  for (let wm = 0; wm < 64; wm++) {
    const W = create_zero_matrix_array(q), K = create_zero_matrix_array(n);
    typePairs.forEach(([a, b], p) => { W[a][b] = W[b][a] = (wm >> p) & 1; });
    const diag = W.map((row, a) => row[a]), B = create_zero_matrix_array(q);
    for (let i = 0; i < n; i++) for (let j = 0; j < n; j++) {
      K[i][j] = i === j ? 0 : W[cls[i]][cls[j]];
    }
    for (let a = 0; a < q; a++) for (let b = 0; b < q; b++) {
      for (let c = 0; c < q; c++) B[a][b] += W[a][c] * sizes[c] * W[c][b];
    }
    const base = sizes.map((_, a) => {
      let h3 = 0, middle = 0;
      for (let b = 0; b < q; b++) {
        middle += sizes[b] * diag[b] * W[a][b] * W[b][a];
        for (let c = 0; c < q; c++) {
          h3 += W[a][b] * sizes[b] * W[b][c] * sizes[c] * W[c][a];
        }
      }
      return (h3 - 2 * diag[a] * B[a][a] - middle + 2 * diag[a]) / 2;
    });
    const baseOracle = Array(n).fill(0);
    for (let i = 0; i < n; i++) for (let j = i + 1; j < n; j++) {
      for (let k = j + 1; k < n; k++) {
        const value = K[i][j] * K[j][k] * K[k][i];
        for (const v of [i, j, k]) baseOracle[v] += value;
      }
    }
    for (let i = 0; i < n; i++) {
      assert_exact_equal_values(base[cls[i]], baseOracle[i], { wm, i, check: "template" });
      templateChecks++;
    }
    for (let am = 0; am < 1024; am++) {
      const A = create_zero_matrix_array(n), E = create_zero_matrix_array(n);
      const H = create_zero_matrix_array(n, q), F = create_zero_matrix_array(q);
      const D = Array.from({ length: n }, () => new Set());
      const defectRows = Array.from({ length: n }, () => []);
      edgePairs.forEach(([i, j], p) => { A[i][j] = A[j][i] = (am >> p) & 1; });
      for (let i = 0; i < n; i++) for (let j = 0; j < n; j++) {
        E[i][j] = A[i][j] - K[i][j];
        H[i][cls[j]] += E[i][j];
        F[cls[i]][cls[j]] += E[i][j];
        if (E[i][j]) { D[i].add(cls[j]); defectRows[i].push([j, E[i][j]]); }
      }
      const exact = Array(n).fill(0), signedResidual = Array(n).fill(0);
      for (let i = 0; i < n; i++) for (let j = i + 1; j < n; j++) {
        for (let k = j + 1; k < n; k++) {
          const value = A[i][j] * A[j][k] * A[k][i];
          const signed = E[i][j] * E[j][k] * E[k][i];
          for (const v of [i, j, k]) { exact[v] += value; signedResidual[v] += signed; }
        }
      }
      // Candidate: type moments once per needed target type, then stream edges.
      const R_batched = Array(n).fill(0);
      for (let u = 0; u < n; u++) {
        const p = new Map();
        for (const a of D[u]) {
          let value = 0;
          for (const b of D[u]) value += W[a][b] * H[u][b];
          p.set(a, value);
        }
        for (const [v, sign] of defectRows[u]) R_batched[v] += sign * p.get(cls[v]);
      }
      // Independent R oracle: explicitly multiply E by E, then take diag(K E^2).
      const E2 = create_zero_matrix_array(n);
      for (let i = 0; i < n; i++) for (let j = 0; j < n; j++) {
        for (let k = 0; k < n; k++) E2[i][j] += E[i][k] * E[k][j];
      }
      for (let v = 0; v < n; v++) {
        const a = cls[v];
        let L = 0, G = 0, J = 0, z = 0, defectDegree = 0, R_oracle = 0;
        for (let u = 0; u < n; u++) {
          defectDegree += E[v][u] * E[v][u];
          J -= diag[cls[u]] * E[v][u] * E[v][u];
          R_oracle += K[v][u] * E2[u][v];
        }
        assert_exact_equal_values(defectDegree, H[v].reduce((sum, h) => sum + Math.abs(h), 0), { wm, am, v, check: "type-sign" });
        signChecks++;
        R_batched[v] -= diag[a] * defectDegree;
        assert_exact_equal_values(R_batched[v], R_oracle, { wm, am, v, check: "optimized-p" });
        pChecks++;
        for (let b = 0; b < q; b++) {
          L += H[v][b] * (B[a][b] - W[a][b] * (diag[a] + diag[b]));
          z += H[v][b] * W[a][b];
          for (let c = 0; c < q; c++) {
            G += W[a][b] * F[b][c] * W[c][a];
            J += H[v][b] * W[b][c] * H[v][c];
          }
        }
        const doubled = 2 * base[a] + G + 2 * L - 2 * diag[a] * z
          + 2 * R_batched[v] + J + 2 * signedResidual[v];
        assert_exact_equal_values(doubled, 2 * exact[v], { wm, am, v, check: "local" });
        localChecks++;
        let degree = -diag[a];
        for (let b = 0; b < q; b++) degree += sizes[b] * W[a][b] + H[v][b];
        assert_exact_equal_values(degree, A[v].reduce((x, y) => x + y, 0), { wm, am, v, check: "degree" });
        degreeChecks++;
      }
      cases++;
    }
  }
  return { cases, localChecks, optimizedPChecks: pChecks, degreeChecks, signChecks, templateChecks, mismatches: 0 };
}

function compute_full_modularity_numerator(A, labels, p, q) {
  const volumes = new Map();
  let M = 0, internal = 0;
  for (let i = 0; i < A.length; i++) {
    let degree = 0;
    for (let j = 0; j < A.length; j++) {
      degree += A[i][j];
      if (labels[i] === labels[j]) internal += A[i][j];
    }
    M += degree;
    volumes.set(labels[i], (volumes.get(labels[i]) || 0) + degree);
  }
  return q * M * internal - p * [...volumes.values()].reduce((s, k) => s + k * k, 0);
}

function execute_scalar_reference_sweep(A, initial, p, q, next) {
  const labels = [...initial];
  let moves = 0;
  for (let i = 0; i < A.length; i++) {
    const source = labels[i], ids = [...new Set(labels)].sort((a, b) => a - b);
    ids.push(next);
    let best = compute_full_modularity_numerator(A, labels, p, q), target = source;
    for (const c of ids) {
      if (c === source) continue;
      labels[i] = c;
      const value = compute_full_modularity_numerator(A, labels, p, q);
      if (value > best) { best = value; target = c; }
    }
    labels[i] = target;
    if (target !== source) { moves++; if (target === next) next++; }
  }
  return { labels, next, moves };
}

function execute_certified_cohort_sweep(A, cls, initial, p, q, next) {
  const n = A.length, labels = [...initial], degrees = A.map(row => row.reduce((x, y) => x + y, 0));
  const M = degrees.reduce((x, y) => x + y, 0);
  let i = 0, moves = 0, events = 0, skipped = 0, maxJump = 0;
  while (i < n) {
    const source = labels[i], ids = [...new Set(labels)].sort((a, b) => a - b);
    ids.push(next);
    let run = 1;
    while (i + run < n && cls[i + run] === cls[i] && labels[i + run] === source) run++;
    const volume = {}, affinity = {}, score = {};
    for (const c of ids) { volume[c] = 0; affinity[c] = 0; }
    for (let u = 0; u < n; u++) {
      volume[labels[u]] += degrees[u];
      if (u !== i) affinity[labels[u]] += A[i][u];
    }
    volume[source] -= degrees[i];
    for (const c of ids) score[c] = q * M * affinity[c] - p * degrees[i] * volume[c];
    let target = source;
    for (const c of ids) {
      if (score[c] > score[target]
          || (score[c] === score[target] && score[c] > score[source] && c < target)) target = c;
    }
    if (target === source) { skipped += run; i += run; continue; }
    const internalWeight = run > 1 ? A[i][i + 1] : 0;
    const beta = q * M * internalWeight - p * degrees[i] * degrees[i];
    const feasible = r => {
      const b = score[target] + r * beta, a = score[source] - r * beta;
      if (b <= a) return false;
      for (const c of ids) {
        if (c !== source && c !== target && (b < score[c] || (b === score[c] && c < target))) return false;
      }
      if (target === next && b < 0) return false;
      return true;
    };
    let lo = 0, hi = run - 1;
    while (lo < hi) {
      const middle = Math.ceil((lo + hi) / 2);
      if (feasible(middle)) lo = middle; else hi = middle - 1;
    }
    const length = lo + 1;
    for (let j = 0; j < length; j++) labels[i + j] = target;
    if (target === next) next++;
    moves += length; events++; maxJump = Math.max(maxJump, length); i += length;
  }
  return { labels, next, moves, events, skipped, maxJump };
}

function enumerate_vertex_partition_lists(n) {
  const result = [];
  const visit = (prefix, maximum) => {
    if (prefix.length === n) { result.push(prefix); return; }
    for (let c = 0; c <= maximum + 1; c++) visit([...prefix, c], Math.max(c, maximum));
  };
  visit([0], 0);
  return result;
}

function verify_community_sweep_equivalence() {
  const n = 5, cls = [0, 0, 1, 1, 2], pairs = [], partitions = enumerate_vertex_partition_lists(n);
  for (let a = 0; a < 3; a++) for (let b = a; b < 3; b++) pairs.push([a, b]);
  let cases = 0, moves = 0, events = 0, skipped = 0, maxJump = 0;
  for (let mask = 0; mask < 64; mask++) {
    const W = create_zero_matrix_array(3);
    pairs.forEach(([a, b], j) => { W[a][b] = W[b][a] = (mask >> j) & 1; });
    const A = Array.from({ length: n }, (_, i) => Array.from({ length: n }, (_, j) => i === j ? 0 : W[cls[i]][cls[j]]));
    if (A.flat().every(x => x === 0)) continue;
    for (const initial of partitions) for (const [p, q] of [[1, 2], [1, 1], [2, 1]]) {
      const scalar = execute_scalar_reference_sweep(A, initial, p, q, n);
      const fast = execute_certified_cohort_sweep(A, cls, initial, p, q, n);
      assert_exact_equal_values([fast.labels, fast.next, fast.moves], [scalar.labels, scalar.next, scalar.moves], { mask, initial, p, q });
      cases++; moves += fast.moves; events += fast.events; skipped += fast.skipped; maxJump = Math.max(maxJump, fast.maxJump);
    }
  }
  return { cases, partitions: partitions.length, acceptedMoves: moves, movingEvents: events, skippedVertices: skipped, maxJump, mismatches: 0 };
}

function verify_weighted_multisweep_equivalence() {
  const n = 9, cls = [0, 0, 0, 1, 1, 1, 2, 2, 2];
  let state = 20260920, sweeps = 0, moves = 0, events = 0, maxJump = 0, maxSweeps = 0;
  const random = () => { state = (Math.imul(state, 1664525) + 1013904223) >>> 0; return state >>> 16; };
  for (let test = 0; test < 2000; test++) {
    const W = create_zero_matrix_array(3);
    for (let a = 0; a < 3; a++) for (let b = a; b < 3; b++) W[a][b] = W[b][a] = random() % 4;
    const A = Array.from({ length: n }, (_, i) => Array.from({ length: n }, (_, j) => i === j ? 0 : W[cls[i]][cls[j]]));
    const initial = Array.from({ length: n }, (_, i) => test % 4 === 0 ? i : test % 4 === 1 ? cls[i] : test % 4 === 2 ? 0 : random() % 4);
    const [p, q] = [[1, 2], [1, 1], [2, 1], [3, 2]][Math.floor(test / 4) % 4];
    let scalar = { labels: initial, next: n }, fast = { labels: initial, next: n }, finished = false;
    for (let iter = 0; iter < 100; iter++) {
      scalar = execute_scalar_reference_sweep(A, scalar.labels, p, q, scalar.next);
      fast = execute_certified_cohort_sweep(A, cls, fast.labels, p, q, fast.next);
      assert_exact_equal_values([fast.labels, fast.next, fast.moves], [scalar.labels, scalar.next, scalar.moves], { test, iter, W, initial, p, q });
      sweeps++; moves += fast.moves; events += fast.events; maxJump = Math.max(maxJump, fast.maxJump);
      if (scalar.moves === 0) { finished = true; maxSweeps = Math.max(maxSweeps, iter + 1); break; }
    }
    assert_exact_equal_values(finished, true, { test, check: "100-sweep-cap" });
  }
  return { seed: 20260920, cases: 2000, sweeps, acceptedMoves: moves, movingEvents: events, maxJump, maxSweeps, mismatches: 0 };
}

// Revision probes: candidate state below does not consult expanded adjacency.
function compare_ranked_score_entries(a, b) {
  return a[1] > b[1] || (a[1] === b[1] && a[0] < b[0]);
}

function select_streamed_candidate_extrema(entries, source, next) {
  let first = [next, 0n], second = null;
  for (const entry of entries) {
    if (entry[0] === source) continue;
    if (compare_ranked_score_entries(entry, first)) { second = first; first = entry; }
    else if (second === null || compare_ranked_score_entries(entry, second)) second = entry;
  }
  // When fresh wins, its replacement is an unchanged zero-score candidate.
  if (first[0] === next) {
    const replacement = [next + 1, 0n];
    if (second === null || compare_ranked_score_entries(replacement, second)) second = replacement;
  }
  return [first, second];
}

function create_indexed_score_heap(entries, stats) {
  const heap = [...entries], positions = new Map();
  const better = (a, b) => { stats.heapComparisons++; return compare_ranked_score_entries(a, b); };
  const swap = (i, j) => {
    [heap[i], heap[j]] = [heap[j], heap[i]];
    positions.set(heap[i][0], i); positions.set(heap[j][0], j);
  };
  const down = start => {
    let i = start;
    for (;;) {
      let j = 2 * i + 1;
      if (j >= heap.length) return;
      if (j + 1 < heap.length && better(heap[j + 1], heap[j])) j++;
      if (!better(heap[j], heap[i])) return;
      swap(i, j); i = j;
    }
  };
  const repair = start => {
    let i = start;
    while (i > 0 && better(heap[i], heap[Math.floor((i - 1) / 2)])) {
      const parent = Math.floor((i - 1) / 2); swap(i, parent); i = parent;
    }
    down(i);
  };
  heap.forEach((entry, i) => positions.set(entry[0], i));
  for (let i = Math.floor(heap.length / 2) - 1; i >= 0; i--) down(i);
  return {
    replace(token, value) {
      stats.scoreUpdates++;
      let i = positions.get(token);
      if (value === null) {
        if (i === undefined) throw Error("missing score deletion");
        positions.delete(token);
        const tail = heap.pop();
        if (i < heap.length) { heap[i] = tail; positions.set(tail[0], i); repair(i); }
      } else if (i === undefined) {
        i = heap.length; heap.push([token, value]); positions.set(token, i); repair(i);
      } else { heap[i] = [token, value]; repair(i); }
    },
    topThree() {
      const frontier = heap.length ? [0] : [], result = [];
      while (frontier.length && result.length < 3) {
        let best = 0;
        for (let j = 1; j < frontier.length; j++) {
          if (better(heap[frontier[j]], heap[frontier[best]])) best = j;
        }
        const i = frontier.splice(best, 1)[0]; result.push(heap[i]);
        for (const child of [2 * i + 1, 2 * i + 2]) if (child < heap.length) frontier.push(child);
        if (frontier.length > 4) throw Error("unbounded extrema frontier");
      }
      stats.extremaRecords += result.length;
      return result;
    }
  };
}

function compute_integer_prefix_length(run, g, h, beta, epsilon) {
  if (g < 1n || h < epsilon) throw Error("invalid first move");
  if (beta >= 0n) return run;
  const bounds = [BigInt(run), 1n + (g - 1n) / (-2n * beta),
    1n + (h - epsilon) / (-beta)];
  return Number(bounds.reduce((a, b) => a < b ? a : b));
}

function construct_ranked_label_intervals(cls, labels) {
  const result = [];
  for (let i = 0; i < labels.length; i++) {
    const tail = result.at(-1);
    if (tail && tail[2] === cls[i] && tail[3] === labels[i]) tail[1]++;
    else result.push([i, i + 1, cls[i], labels[i]]);
  }
  return result;
}

function replace_ranked_interval_prefix(intervals, lo, hi, target, cap) {
  const result = [];
  const append = row => {
    if (row[0] === row[1]) return;
    const tail = result.at(-1);
    if (tail && tail[1] === row[0] && tail[2] === row[2] && tail[3] === row[3]) tail[1] = row[1];
    else result.push(row);
  };
  for (const [l, r, a, c] of intervals) {
    if (r <= lo || l >= hi) append([l, r, a, c]);
    else {
      append([l, Math.max(l, lo), a, c]);
      append([Math.max(l, lo), Math.min(r, hi), a, target]);
      append([Math.min(r, hi), r, a, c]);
    }
  }
  if (result.length > cap) throw Error("interval admission refused");
  return result;
}

function expand_ranked_interval_labels(intervals) {
  const labels = [];
  for (const [l, r, , c] of intervals) for (let i = l; i < r; i++) labels[i] = c;
  return labels;
}

function compute_bigint_objective_oracle(A, labels, p, q) {
  const volumes = new Map(); let M = 0n, internal = 0n;
  for (let i = 0; i < A.length; i++) {
    let k = 0n;
    for (let j = 0; j < A.length; j++) {
      k += A[i][j]; if (labels[i] === labels[j]) internal += A[i][j];
    }
    M += k; volumes.set(labels[i], (volumes.get(labels[i]) || 0n) + k);
  }
  return q * M * internal - p * [...volumes.values()].reduce((s, k) => s + k * k, 0n);
}

function execute_bigint_oracle_visit(A, state, i, p, q) {
  const source = state.labels[i], before = compute_bigint_objective_oracle(A, state.labels, p, q);
  let best = before, target = source;
  for (const c of [...new Set(state.labels), state.next].sort((a, b) => a - b)) {
    if (c === source) continue;
    state.labels[i] = c;
    const value = compute_bigint_objective_oracle(A, state.labels, p, q);
    if (value > best) { best = value; target = c; }
  }
  state.labels[i] = target;
  if (target !== source) { state.moves++; if (target === state.next) state.next++; }
  return { target, gain: best - before };
}

function execute_cached_interval_sweep(Winput, cls, initial, p0, q0, jump, limit = cls.length) {
  const W = Winput.map(row => row.map(BigInt)), p = BigInt(p0), q = BigInt(q0);
  const sizes = W.map((_, a) => cls.filter(b => b === a).length);
  const degrees = W.map((row, a) => row.reduce((s, w, b) => s + w * BigInt(sizes[b]), 0n) - row[a]);
  const M = degrees.reduce((s, k, a) => s + k * BigInt(sizes[a]), 0n);
  const Aoracle = cls.map((a, i) => cls.map((b, j) => i === j ? 0n : W[a][b]));
  const oracle = { labels: [...initial], next: Math.max(...initial) + 1, moves: 0 };
  let next = oracle.next, cursor = 0, currentType = -1, cache, heap;
  let intervals = construct_ranked_label_intervals(cls, initial);
  const counts = new Map(), volumes = new Map();
  for (const [l, r, a, c] of intervals) {
    if (!counts.has(c)) counts.set(c, new Map());
    const row = counts.get(c); row.set(a, (row.get(a) || 0) + r - l);
    volumes.set(c, (volumes.get(c) || 0n) + BigInt(r - l) * degrees[a]);
  }
  const stats = { moves: 0, movingEvents: 0, stayEvents: 0, maxJump: 0,
    cacheBuilds: 0, countBuildReads: 0, scoreUpdates: 0, heapComparisons: 0,
    extremaRecords: 0, extremaChecks: 0, boundaries: 0, receiptChecks: 0,
    divisions: 0, intervalCommits: 0, splits: 0, admissionChecks: 0 };
  const snapshots = [];
  while (cursor < Math.min(cls.length, limit)) {
    const interval = intervals.find(([l, r]) => l <= cursor && cursor < r);
    const [, end, a, source] = interval, k = degrees[a];
    const beta = q * M * W[a][a] - p * k * k;
    if (a !== currentType) {
      currentType = a; cache = new Map(); stats.cacheBuilds++;
      for (const [c, row] of counts) {
        let affinity = 0n;
        for (const [b, count] of row) { affinity += W[a][b] * BigInt(count); stats.countBuildReads++; }
        cache.set(c, q * M * affinity - p * k * volumes.get(c));
      }
      heap = create_indexed_score_heap(cache, stats);
    }
    const choices = select_streamed_candidate_extrema(heap.topThree(), source, next);
    const scan = select_streamed_candidate_extrema(stats.boundaries % 2 ? [...cache].reverse() : cache, source, next);
    const normalized = x => x.map(y => y && [y[0], y[1].toString()]);
    assert_exact_equal_values(normalized(choices), normalized(scan), "heap vs streamed extrema");
    stats.extremaChecks++;
    const [best, runner] = choices, sourceScore = cache.get(source) - beta;
    const moving = best[1] > sourceScore;
    const target = moving ? best[0] : source;
    const run = Math.min(end, limit) - cursor;
    let length = jump ? run : 1;
    const g = best[1] - sourceScore;
    if (jump && moving) {
      const epsilon = runner[0] < target ? 1n : 0n;
      length = compute_integer_prefix_length(run, g, best[1] - runner[1], beta, epsilon);
      if (beta < 0n) stats.divisions += 2;
    }
    // Oracle-only expanded member loops: the candidate commit edits intervals.
    let oracleGain = 0n;
    for (let r = 0; r < length; r++) {
      const step = execute_bigint_oracle_visit(Aoracle, oracle, cursor + r, p, q);
      assert_exact_equal_values(step.target, target, "scalar destination within event");
      const expected = moving ? 2n * (g + 2n * BigInt(r) * beta) : 0n;
      assert_exact_equal_values(step.gain.toString(), expected.toString(), "serial gain receipt");
      oracleGain += step.gain; stats.receiptChecks++;
    }
    const receipt = moving ? 2n * (BigInt(length) * g + beta * BigInt(length) * BigInt(length - 1)) : 0n;
    assert_exact_equal_values(oracleGain.toString(), receipt.toString(), "aggregate receipt");
    if (moving) {
      const planned = replace_ranked_interval_prefix(intervals, cursor, cursor + length, target, cls.length);
      // Refuse the same transaction before changing any live state.
      const before = JSON.stringify([intervals, [...counts].map(([c, row]) => [c, [...row]]), next, cursor]);
      let refused = false;
      try { replace_ranked_interval_prefix(intervals, cursor, cursor + length, target, planned.length - 1); }
      catch (error) { refused = error.message === "interval admission refused"; }
      assert_exact_equal_values(refused, true, "forced interval refusal");
      assert_exact_equal_values(JSON.stringify([intervals, [...counts].map(([c, row]) => [c, [...row]]), next, cursor]), before, "unchanged refused state");
      stats.admissionChecks++;
      stats.splits += Math.max(0, planned.length - intervals.length);
      intervals = planned; stats.intervalCommits++;
      const src = counts.get(source), remaining = src.get(a) - length;
      if (remaining) src.set(a, remaining); else src.delete(a);
      volumes.set(source, volumes.get(source) - BigInt(length) * k);
      if (!counts.has(target)) { counts.set(target, new Map()); volumes.set(target, 0n); }
      const dst = counts.get(target); dst.set(a, (dst.get(a) || 0) + length);
      volumes.set(target, volumes.get(target) + BigInt(length) * k);
      const destinationScore = (cache.get(target) || 0n) + BigInt(length) * beta;
      if (src.size === 0) { counts.delete(source); volumes.delete(source); cache.delete(source); heap.replace(source, null); }
      else { cache.set(source, cache.get(source) - BigInt(length) * beta); heap.replace(source, cache.get(source)); }
      cache.set(target, destinationScore); heap.replace(target, destinationScore);
      if (target === next) next++;
      stats.moves += length; stats.movingEvents++; stats.maxJump = Math.max(stats.maxJump, length);
    } else stats.stayEvents++;
    cursor += length; stats.boundaries++;
    const expanded = expand_ranked_interval_labels(intervals);
    assert_exact_equal_values([expanded, next, stats.moves], [oracle.labels, oracle.next, oracle.moves], "event boundary");
    for (const [c, row] of counts) {
      for (let b = 0; b < W.length; b++) {
        assert_exact_equal_values(row.get(b) || 0, expanded.filter((token, i) => token === c && cls[i] === b).length, "compressed count");
      }
      const volume = expanded.reduce((sum, token, i) => sum + (token === c ? degrees[cls[i]] : 0n), 0n);
      const affinity = [...row].reduce((sum, [b, count]) => sum + W[a][b] * BigInt(count), 0n);
      assert_exact_equal_values([volumes.get(c).toString(), cache.get(c).toString()],
        [volume.toString(), (q * M * affinity - p * k * volume).toString()], "volume and cache");
    }
    snapshots.push([cursor, target, length]);
  }
  assert_exact_equal_values(stats.scoreUpdates, 2 * stats.movingEvents, "two score updates per move event");
  return { labels: expand_ranked_interval_labels(intervals), next, cursor, stats, snapshots };
}

function verify_cached_execution_procedures() {
  const cls = [0, 0, 1, 1, 2], pairs = [], partitions = enumerate_vertex_partition_lists(5);
  for (let a = 0; a < 3; a++) for (let b = a; b < 3; b++) pairs.push([a, b]);
  let cases = 0, boundaries = 0, receipts = 0, scalarUpdates = 0, jumpUpdates = 0, splits = 0, divisions = 0;
  for (let mask = 0; mask < 64; mask++) {
    const W = create_zero_matrix_array(3);
    pairs.forEach(([a, b], j) => { W[a][b] = W[b][a] = (mask >> j) & 1; });
    for (const partition of partitions) for (const [p, q] of [[1, 2], [1, 1], [2, 1]]) {
      // Arbitrary, non-dense, non-monotone community tokens.
      const initial = partition.map(c => [31, 7, 203, 55, 101][c]);
      const scalar = execute_cached_interval_sweep(W, cls, initial, p, q, false);
      const fast = execute_cached_interval_sweep(W, cls, initial, p, q, true);
      assert_exact_equal_values([fast.labels, fast.next, fast.stats.moves], [scalar.labels, scalar.next, scalar.stats.moves], "matched caches");
      for (const result of [scalar, fast]) { boundaries += result.stats.boundaries; receipts += result.stats.receiptChecks; }
      scalarUpdates += scalar.stats.scoreUpdates; jumpUpdates += fast.stats.scoreUpdates;
      splits += fast.stats.splits; divisions += fast.stats.divisions; cases++;
    }
  }
  const contrasts = {};
  for (const [name, initial] of [["default", Array.from({ length: 60 }, (_, i) => i)],
    ["warm", Array.from({ length: 60 }, (_, i) => i < 30 ? 7 : 31)]]) {
    contrasts[name] = {};
    for (const jump of [false, true]) {
      const result = execute_cached_interval_sweep([[1]], Array(60).fill(0), initial, 1, 1, jump);
      contrasts[name][jump ? "jump" : "scalar"] = result.stats;
    }
  }
  const truncated = execute_cached_interval_sweep([[1]], Array(60).fill(0), Array.from({ length: 60 }, (_, i) => i < 30 ? 7 : 31), 1, 1, true, 17);
  assert_exact_equal_values([truncated.cursor, truncated.stats.moves, truncated.stats.movingEvents], [17, 17, 1], "logical visit budget");
  const isolate = execute_cached_interval_sweep([[1, 0], [0, 0]], [0, 0, 1], [0, 0, 1], 3, 1, true);
  assert_exact_equal_values(isolate.snapshots[0][1], 1, "old zero-volume token beats fresh");
  const huge = execute_cached_interval_sweep([[2n ** 80n]], Array(8).fill(0), [7, 7, 7, 7, 31, 31, 31, 31], 3, 2, true);
  const negative = execute_cached_interval_sweep([[0, 1], [1, 0]],
    [...Array(40).fill(0), ...Array(40).fill(1)],
    [...Array(40).fill(31), ...Array(30).fill(7), ...Array(10).fill(55)], 1, 1, true);
  assert_exact_equal_values(negative.snapshots[0], [21, 7, 21], "negative beta graph crossover at tie");
  return { cases, boundaries, receiptChecks: receipts, scalarScoreUpdates: scalarUpdates,
    jumpScoreUpdates: jumpUpdates, jumpSplits: splits, jumpDivisions: divisions,
    logicalBudgetMoves: truncated.stats.moves, largeIntegerBoundaries: huge.stats.boundaries,
    negativeBetaFirstJump: negative.snapshots[0][2],
    K60: contrasts, mismatches: 0 };
}

function verify_division_boundary_cases() {
  let checks = 0;
  for (let A = -6; A <= 6; A++) for (let B = -6; B <= 6; B++) for (let R = -6; R <= 6; R++)
  for (let beta = -4; beta <= 4; beta++) for (let earlier = 0; earlier <= 1; earlier++) for (let run = 1; run <= 20; run++) {
    if (B <= A || B - R < earlier) continue;
    let scalar = 0;
    for (let r = 0; r < run; r++) {
      if (B - A + 2 * r * beta <= 0 || B - R + r * beta < earlier) break;
      scalar++;
    }
    const direct = compute_integer_prefix_length(run, BigInt(B - A), BigInt(B - R), BigInt(beta), BigInt(earlier));
    assert_exact_equal_values(direct, scalar, "integer crossover"); checks++;
  }
  // A long strictly decreasing gap stops at the zero-gain visit, not after it.
  assert_exact_equal_values(compute_integer_prefix_length(1000, 1001n, 10000n, -1n, 0n), 501, "long negative beta");
  const scale = 2n ** 100n;
  assert_exact_equal_values(compute_integer_prefix_length(1000, 1001n * scale, 10000n * scale, -scale, 0n), 501, "wide division");
  return { exhaustive: checks, longAndWide: 2, mismatches: 0 };
}

// Virtual tapes model external records; JS backing arrays are NOT physical-RAM evidence.
function create_capacity_tape_machine(cap) {
  const stats = { capacity: cap, maxBuffer: 0, reads: 0, writes: 0, live: 0, peakLive: 0, sorts: 0, mergePasses: 0, comparisons: 0 };
  const hold = count => { stats.maxBuffer = Math.max(stats.maxBuffer, count); if (count > cap) throw Error("arena exceeded"); };
  const tape = () => ({ rows: [], alive: true });
  const write = (file, record) => {
    if (!file.alive) throw Error("write deleted tape");
    file.rows.push(record); stats.writes++; stats.live++; stats.peakLive = Math.max(stats.peakLive, stats.live);
  };
  const read = (file, i) => { if (!file.alive || i >= file.rows.length) throw Error("invalid tape read"); stats.reads++; return file.rows[i]; };
  const drop = file => { stats.live -= file.rows.length; file.alive = false; file.rows = []; };
  const sort = (input, compare) => {
    stats.sorts++; const size = input.rows.length, run = cap - 1;
    let output = tape();
    for (let start = 0; start < size; start += run) {
      const buffer = [];
      for (let i = start; i < Math.min(start + run, size); i++) buffer.push(read(input, i));
      hold(buffer.length + 1);
      // In-place insertion sort has one temporary record, no hidden sort workspace.
      for (let i = 1; i < buffer.length; i++) {
        const value = buffer[i]; let j = i;
        while (j && (stats.comparisons++, compare(value, buffer[j - 1]) < 0)) { buffer[j] = buffer[j - 1]; j--; }
        buffer[j] = value;
      }
      for (const record of buffer) write(output, record);
    }
    drop(input); input = output;
    for (let width = run; width < size; width *= 2) {
      stats.mergePasses++; output = tape();
      for (let start = 0; start < size; start += 2 * width) {
        let i = start, j = Math.min(start + width, size);
        const mid = j, end = Math.min(start + 2 * width, size);
        let left = i < mid ? read(input, i) : null, right = j < end ? read(input, j) : null;
        hold(3);
        while (left || right) {
          if (!right || (left && (stats.comparisons++, compare(left, right) <= 0))) {
            write(output, left); i++; left = i < mid ? read(input, i) : null;
          } else { write(output, right); j++; right = j < end ? read(input, j) : null; }
        }
      }
      drop(input); input = output;
    }
    return input;
  };
  return { stats, hold, tape, write, read, drop, sort };
}

function execute_capacity_keyed_join(cls, W, defects, cap, tile) {
  const disk = create_capacity_tape_machine(cap), { read, write, tape, drop, hold } = disk;
  const keyCompare = (x, y) => x[0] - y[0] || x[1] - y[1] || (x[2] || 0) - (y[2] || 0);
  let edges = tape();
  for (const [u, v, sign] of defects) {
    if (u === v || sign !== 1 - 2 * W[cls[u]][cls[v]]) throw Error("invalid Boolean defect");
    write(edges, [u, cls[v], v, sign]); write(edges, [v, cls[u], u, sign]);
  }
  edges = disk.sort(edges, keyCompare);
  const moments = tape(), offsets = tape();
  let previous = null, sum = 0, rowStart = 0, rowVertex = -1;
  const flush = () => {
    if (!previous) return;
    if (rowVertex !== previous[0]) {
      if (rowVertex !== -1) write(offsets, [rowVertex, rowStart, moments.rows.length - rowStart]);
      rowVertex = previous[0]; rowStart = moments.rows.length;
    }
    write(moments, [previous[0], previous[1], sum]);
  };
  for (let i = 0; i < edges.rows.length; i++) {
    const edge = read(edges, i); hold(4);
    if (!previous || edge[0] !== previous[0] || edge[1] !== previous[1]) { flush(); previous = edge; sum = 0; }
    sum += edge[3];
  }
  flush(); if (rowVertex !== -1) write(offsets, [rowVertex, rowStart, moments.rows.length - rowStart]);
  let payload = tape(), R2 = 0, momentVisits = 0, forcedRows = 0, maxRow = 0;
  for (let i = 0; i < offsets.rows.length; i++) {
    const [u, start, length] = read(offsets, i);
    R2 += length * length; maxRow = Math.max(maxRow, length);
    if (length > tile) forcedRows++;
    // Reverse tile order forces the subsequent payload sort to do real work.
    for (let end = length; end > 0; end -= tile) {
      const targets = [];
      for (let j = Math.max(0, end - tile); j < end; j++) {
        const record = read(moments, start + j); targets.push([record[1], 0]); momentVisits++;
      }
      hold(targets.length + 3);
      for (let j = 0; j < length; j++) {
        const [, b, h] = read(moments, start + j); momentVisits++;
        for (const target of targets) target[1] += W[target[0]][b] * h;
      }
      for (const [a, p] of targets) write(payload, [u, a, p]);
    }
  }
  const H = moments.rows.length;
  payload = disk.sort(payload, (x, y) => x[0] - y[0] || x[1] - y[1]);
  let contributions = tape(), edgeCursor = 0, edge = edges.rows.length ? read(edges, 0) : null;
  let joinReads = edge ? 1 : 0;
  for (let i = 0; i < payload.rows.length; i++) {
    const [u, a, p] = read(payload, i); hold(3);
    if (!edge || edge[0] !== u || edge[1] !== a) throw Error("unmatched payload");
    while (edge && edge[0] === u && edge[1] === a) {
      write(contributions, [edge[2], edge[3] * p]); edgeCursor++;
      edge = edgeCursor < edges.rows.length ? read(edges, edgeCursor) : null;
      if (edge) joinReads++;
    }
  }
  if (edgeCursor !== 2 * defects.length) throw Error("unmatched keyed edge");
  drop(payload); drop(edges);
  contributions = disk.sort(contributions, (x, y) => x[0] - y[0]);
  const resultTape = tape();
  let position = 0, contribution = contributions.rows.length ? read(contributions, 0) : null;
  for (let i = 0; i < offsets.rows.length; i++) {
    const [v, start, length] = read(offsets, i); let raw = 0, degree = 0;
    hold(5);
    while (contribution && contribution[0] === v) {
      raw += contribution[1]; position++;
      contribution = position < contributions.rows.length ? read(contributions, position) : null;
    }
    for (let j = 0; j < length; j++) degree += Math.abs(read(moments, start + j)[2]);
    write(resultTape, [v, raw - W[cls[v]][cls[v]] * degree]);
  }
  assert_exact_equal_values(position, contributions.rows.length, "streamed destination reduction");
  drop(contributions); drop(moments); drop(offsets);
  // Only the test harness expands the completed sparse output tape.
  const answer = Array(cls.length).fill(0);
  for (let i = 0; i < resultTape.rows.length; i++) {
    const [v, value] = read(resultTape, i); answer[v] = value;
  }
  drop(resultTape);
  assert_exact_equal_values([joinReads, disk.stats.live], [2 * defects.length, 0], "edge-once join and scratch retirement");
  return { answer, stats: { f: defects.length, H, R2, maxRow, forcedRows, tile,
    momentVisits, joinEdgeReads: joinReads, ...disk.stats } };
}

function verify_sparse_complete_stencil(W, cls, defects, R) {
  const n = cls.length, q = W.length, sizes = W.map((_, a) => cls.filter(b => b === a).length);
  const h = Array.from({ length: n }, () => new Map()), F = create_zero_matrix_array(q);
  const rows = Array.from({ length: n }, () => []), e = Array(n).fill(0);
  for (const [u, v, sign] of defects) {
    for (const [x, y] of [[u, v], [v, u]]) {
      h[x].set(cls[y], (h[x].get(cls[y]) || 0) + sign);
      F[cls[x]][cls[y]] += sign; rows[x].push([y, sign]); e[x]++;
    }
  }
  const less = (u, v) => e[u] < e[v] || (e[u] === e[v] && u < v);
  const forward = rows.map((row, u) => row.filter(([v]) => less(u, v)).sort(([v], [w]) => v - w));
  const signed = Array(n).fill(0); let residualTriangles = 0;
  for (let u = 0; u < n; u++) for (const [v, uv] of forward[u]) {
    let i = 0, j = 0;
    while (i < forward[u].length && j < forward[v].length) {
      const [x, ux] = forward[u][i], [y, vy] = forward[v][j];
      if (x < y) i++;
      else if (x > y) j++;
      else {
        const value = uv * ux * vy;
        signed[u] += value; signed[v] += value; signed[x] += value;
        residualTriangles++; i++; j++;
      }
    }
  }
  const B = create_zero_matrix_array(q), G = Array(q).fill(0), base = Array(q).fill(0);
  for (let a = 0; a < q; a++) for (let b = 0; b < q; b++) for (let c = 0; c < q; c++) {
    B[a][b] += W[a][c] * sizes[c] * W[c][b];
    G[a] += W[a][b] * F[b][c] * W[c][a];
    base[a] += W[a][b] * sizes[b] * W[b][c] * sizes[c] * W[c][a];
  }
  for (let a = 0; a < q; a++) {
    base[a] -= 2 * W[a][a] * B[a][a]; base[a] += 2 * W[a][a];
    for (let b = 0; b < q; b++) base[a] -= sizes[b] * W[b][b] * W[a][b] * W[a][b];
  }
  const bulk = base.map((value, a) => value + G[a]), exceptions = new Map();
  for (let v = 0; v < n; v++) if (h[v].size) {
    const a = cls[v]; let L = 0, J = 0, z = 0;
    for (const [b, hb] of h[v]) {
      L += hb * (B[a][b] - W[a][b] * (W[a][a] + W[b][b]));
      z += hb * W[a][b]; J -= W[b][b] * Math.abs(hb);
      for (const [c, hc] of h[v]) J += hb * W[b][c] * hc;
    }
    exceptions.set(v, 2 * L - 2 * W[a][a] * z + 2 * R[v] + J + 2 * signed[v]);
  }
  const result = cls.map((a, v) => (bulk[a] + (exceptions.get(v) || 0)) / 2);
  const A = cls.map((a, u) => cls.map((b, v) => u === v ? 0 : W[a][b]));
  const E = create_zero_matrix_array(n);
  for (const [u, v, sign] of defects) { A[u][v] += sign; A[v][u] += sign; E[u][v] = E[v][u] = sign; }
  const exact = Array(n).fill(0), signedOracle = Array(n).fill(0);
  for (let u = 0; u < n; u++) for (let v = u + 1; v < n; v++) for (let w = v + 1; w < n; w++) {
    for (const x of [u, v, w]) {
      exact[x] += A[u][v] * A[v][w] * A[w][u];
      signedOracle[x] += E[u][v] * E[v][w] * E[w][u];
    }
  }
  assert_exact_equal_values(signed, signedOracle, "signed forward intersections");
  assert_exact_equal_values(result, exact, "type-once G sparse J and bulk/exception merge");
  return { local: result, residualTriangles };
}

function verify_capacity_join_procedures() {
  const skew = [];
  for (const [r, L, cap, tile] of [[12, 37, 7, 3], [32, 1024, 11, 8]]) {
    const cls = [0], defects = [];
    for (let a = 1; a <= r; a++) for (let j = 0; j < L; j++) {
      cls.push(a); defects.push([0, cls.length - 1, -1]);
    }
    const W = Array.from({ length: r + 1 }, () => Array(r + 1).fill(1));
    const result = execute_capacity_keyed_join(cls, W, defects, cap, tile);
    assert_exact_equal_values(result.answer, [0, ...Array(r * L).fill(r * L - 1)], "clique-star independent R oracle");
    assert_exact_equal_values(result.stats.R2, r * r + L * r, "skew R2");
    skew.push(result.stats);
  }
  let smallChecks = 0, fullLocalChecks = 0, residualTriangles = 0;
  for (let test = 0; test < 64; test++) {
    const cls = [0, 0, 1, 1, 2, 2], W = create_zero_matrix_array(3), pairs = [];
    for (let a = 0; a < 3; a++) for (let b = a; b < 3; b++) pairs.push([a, b]);
    pairs.forEach(([a, b], i) => { W[a][b] = W[b][a] = (test >> i) & 1; });
    const E = create_zero_matrix_array(6), K = create_zero_matrix_array(6), defects = [];
    for (let u = 0; u < 6; u++) for (let v = u + 1; v < 6; v++) {
      K[u][v] = K[v][u] = W[cls[u]][cls[v]];
      if ((u * 7 + v * 3 + test) % 4 !== 0) {
        const sign = 1 - 2 * K[u][v]; defects.push([u, v, sign]); E[u][v] = E[v][u] = sign;
      }
    }
    const result = execute_capacity_keyed_join(cls, W, defects, 5, 2), oracle = Array(6).fill(0);
    for (let v = 0; v < 6; v++) for (let u = 0; u < 6; u++) for (let z = 0; z < 6; z++) oracle[v] += K[v][z] * E[z][u] * E[u][v];
    assert_exact_equal_values(result.answer, oracle, "mixed-sign capacity join vs direct KE2"); smallChecks += 6;
    residualTriangles += verify_sparse_complete_stencil(W, cls, defects, result.answer).residualTriangles;
    fullLocalChecks += 6;
  }
  const witnesses = [];
  for (const edges of [[[0, 1], [1, 2], [2, 3], [3, 4], [4, 5], [0, 5]],
    [[0, 1], [1, 2], [0, 2], [3, 4], [4, 5], [3, 5]]]) {
    const defects = edges.map(([u, v]) => [u, v, -1]), cls = Array(6).fill(0);
    const joined = execute_capacity_keyed_join(cls, [[1]], defects, 5, 2);
    witnesses.push(verify_sparse_complete_stencil([[1]], cls, defects, joined.answer).local);
  }
  assert_exact_equal_values(witnesses, [Array(6).fill(1), Array(6).fill(0)], "same moments different topology");
  return { skew, smallCases: 64, smallVertexChecks: smallChecks, fullLocalChecks,
    signedResidualTriangles: residualTriangles, witnesses, mismatches: 0 };
}

console.log(JSON.stringify({
  triangles: verify_triangle_stencil_identity(),
  communities: verify_community_sweep_equivalence(),
  weightedCommunities: verify_weighted_multisweep_equivalence(),
  cachedCommunities: verify_cached_execution_procedures(),
  integerPrefixes: verify_division_boundary_cases(),
  capacityJoin: verify_capacity_join_procedures()
}, null, 2));
```

## Recorded Rerun

The saved source ran successfully with exit code zero on 2026-09-20, Node.js v24.9.0. Output:

```json
{
  "triangles": {
    "cases": 65536,
    "localChecks": 327680,
    "optimizedPChecks": 327680,
    "degreeChecks": 327680,
    "signChecks": 327680,
    "templateChecks": 320,
    "mismatches": 0
  },
  "communities": {
    "cases": 9672,
    "partitions": 52,
    "acceptedMoves": 16902,
    "movingEvents": 16146,
    "skippedVertices": 31458,
    "maxJump": 2,
    "mismatches": 0
  },
  "weightedCommunities": {
    "seed": 20260920,
    "cases": 2000,
    "sweeps": 3877,
    "acceptedMoves": 8952,
    "movingEvents": 7966,
    "maxJump": 3,
    "maxSweeps": 5,
    "mismatches": 0
  },
  "cachedCommunities": {
    "cases": 9984,
    "boundaries": 95636,
    "receiptChecks": 99840,
    "scalarScoreUpdates": 33816,
    "jumpScoreUpdates": 32304,
    "jumpSplits": 1556,
    "jumpDivisions": 18338,
    "logicalBudgetMoves": 17,
    "largeIntegerBoundaries": 8,
    "negativeBetaFirstJump": 21,
    "K60": {
      "default": {
        "scalar": {
          "moves": 59,
          "movingEvents": 59,
          "stayEvents": 1,
          "maxJump": 1,
          "cacheBuilds": 1,
          "countBuildReads": 60,
          "scoreUpdates": 118,
          "heapComparisons": 716,
          "extremaRecords": 179,
          "extremaChecks": 60,
          "boundaries": 60,
          "receiptChecks": 60,
          "divisions": 0,
          "intervalCommits": 59,
          "splits": 0,
          "admissionChecks": 59
        },
        "jump": {
          "moves": 59,
          "movingEvents": 59,
          "stayEvents": 1,
          "maxJump": 1,
          "cacheBuilds": 1,
          "countBuildReads": 60,
          "scoreUpdates": 118,
          "heapComparisons": 716,
          "extremaRecords": 179,
          "extremaChecks": 60,
          "boundaries": 60,
          "receiptChecks": 60,
          "divisions": 0,
          "intervalCommits": 59,
          "splits": 0,
          "admissionChecks": 59
        }
      },
      "warm": {
        "scalar": {
          "moves": 30,
          "movingEvents": 30,
          "stayEvents": 30,
          "maxJump": 1,
          "cacheBuilds": 1,
          "countBuildReads": 2,
          "scoreUpdates": 60,
          "heapComparisons": 59,
          "extremaRecords": 90,
          "extremaChecks": 60,
          "boundaries": 60,
          "receiptChecks": 60,
          "divisions": 0,
          "intervalCommits": 30,
          "splits": 1,
          "admissionChecks": 30
        },
        "jump": {
          "moves": 30,
          "movingEvents": 1,
          "stayEvents": 1,
          "maxJump": 30,
          "cacheBuilds": 1,
          "countBuildReads": 2,
          "scoreUpdates": 2,
          "heapComparisons": 1,
          "extremaRecords": 3,
          "extremaChecks": 2,
          "boundaries": 2,
          "receiptChecks": 60,
          "divisions": 0,
          "intervalCommits": 1,
          "splits": 0,
          "admissionChecks": 1
        }
      }
    },
    "mismatches": 0
  },
  "integerPrefixes": {
    "exhaustive": 248040,
    "longAndWide": 2,
    "mismatches": 0
  },
  "capacityJoin": {
    "skew": [
      {
        "f": 444,
        "H": 456,
        "R2": 588,
        "maxRow": 12,
        "forcedRows": 1,
        "tile": 3,
        "momentVisits": 948,
        "joinEdgeReads": 888,
        "capacity": 7,
        "maxBuffer": 7,
        "reads": 25491,
        "writes": 23210,
        "live": 0,
        "peakLive": 3133,
        "sorts": 3,
        "mergePasses": 23,
        "comparisons": 13276
      },
      {
        "f": 32768,
        "H": 32800,
        "R2": 33792,
        "maxRow": 32,
        "forcedRows": 1,
        "tile": 8,
        "momentVisits": 65696,
        "joinEdgeReads": 65536,
        "capacity": 11,
        "maxBuffer": 11,
        "reads": 2687619,
        "writes": 2523618,
        "live": 0,
        "peakLive": 229441,
        "sorts": 3,
        "mergePasses": 38,
        "comparisons": 1509662
      }
    ],
    "smallCases": 64,
    "smallVertexChecks": 384,
    "fullLocalChecks": 384,
    "signedResidualTriangles": 448,
    "witnesses": [
      [
        1,
        1,
        1,
        1,
        1,
        1
      ],
      [
        0,
        0,
        0,
        0,
        0,
        0
      ]
    ],
    "mismatches": 0
  }
}
```

## Evidence Limits

The original ephemeral local-formula probe used the direct finite-sum `R`; a separate ephemeral probe checked optimized `p` against explicit `K E^2`. The retained consolidated source above goes further: it uses optimized `R_batched` inside the full local formula and also compares that `R_batched` independently to explicit multiplication. These are separate assertions, not one check relabeled twice.

The deterministic weighted test uses the upper 16 bits of an explicitly specified LCG, not an unseeded random source or a claim of representative graph sampling. This improves diversity over an earlier ephemeral low-bit LCG check; the retained test's results, not that earlier trial, are authoritative for reproducibility.

The revised matched-cache domain has 9,984 configurations including edgeless templates, 95,636 event boundaries across both engines and 99,840 individual gain receipts, with zero mismatches. Its moving-key updates fall from 33,816 to 32,304, about 4.47%; these are operation counts, not speedups. The old weighted multisweep result does not validate the revised cache across weighted multi-sweep persistence. The new wide-integer and negative-curvature graph cases validate specific additional paths, not all integer limits or graph profiles.

The negative-curvature graph is `K40,40`, with the first 40 vertices at token 31, the next 30 at token 7 and the final ten at token 55. `beta=-1600`; the first jump to token 7 includes the 21st member at equality with the later-token competitor and stops before the next member. Its complete sweep is checked against individual full-objective visits. The 501-member arithmetic case separately exercises the strict source/zero-gain boundary.

The large skew R test makes 65,536 join edge reads but **2,687,619 total tape reads and 2,523,618 writes**, including three sorts and 38 two-way merge passes. Omitting sorting would substantially misrepresent the procedure. Its maximum modeled live external set is 229,441 records, all retired after output validation; this is not a 229,441-byte bound. The small skew run peaks at 3,133 records. Fixed-capacity tests cover full row tiling, payload order, skewed one-to-many joins and streamed reduction, not just the algebraic `R` sum. The complete six-vertex sparse tests additionally process 448 residual-support triangles and compare 384 local counts plus both six-vertex witnesses.

Remaining untested systems stages: real file/page I/O, type-matrix tiling, external score/count/interval trees, external residual counters, crash-atomic event commits, projection/type validation, refresh, physical RAM and 50 GB lifecycle fit. BigInt arithmetic is tested, not production fixed-width overflow admission. No GDS/Leiden parity or general novelty result is implied. No standalone code was added.
