# PageRank Mathematical Probe Evidence

Date: 2026-09-20. Ephemeral JavaScript calculations, not a production implementation, benchmark or rigorous floating-point enclosure.

## Scope

The revised probe compares an independently expanded original adjacency operator with the reduced factor-state equation. The original matrix is constructed from the unpruned C/B inputs, so the test can detect an incorrect pruning transformation rather than silently changing the oracle too. Both solvers use f64 arithmetic. All graphs are small enough for a dense oracle; this does not implement the proposed disk schedule.

Coverage: 512 directed binary 3-by-3 matrices including loops, three personalizations, three damping values; plus 192 seeded weighted factored cases at four damping values. Total 5,376 cases. Original PageRank is iterated from p until successive L1 difference is below 1e-14, capped at 20,000 updates. The factor solver tests its residual identity at every iteration, verifies its error bound with a 1e-10 numerical allowance, and stops when its predicted bound is below 1e-10.

The resulting checks are correlated iterations on 5,376 cases, not 310,992 independent graphs. The allowance is deliberately much larger than observed residual identity roundoff; it cannot establish rigorously rounded certification near the target tolerance.

## Revised Results

```json
{
  "seed": 20260920,
  "cases": 5376,
  "residual_checks": 310992,
  "bound_checks": 310992,
  "max_identity_error": 1.1568555765032948e-15,
  "max_solution_error": 9.994219341002974e-11,
  "max_steps": 2899,
  "worst": {
    "n": 4,
    "f": 3,
    "alpha": 0.97,
    "max_q_over_d": 3,
    "C": [
      [
        3,
        2,
        3
      ],
      [
        0,
        0,
        1
      ],
      [
        2,
        0,
        0
      ],
      [
        1,
        0,
        3
      ]
    ],
    "B": [
      [
        0,
        3,
        0
      ],
      [
        0,
        0,
        0
      ],
      [
        0,
        0,
        2
      ],
      [
        0,
        1,
        0
      ]
    ],
    "E": [
      [
        2,
        0,
        0,
        0
      ],
      [
        0,
        0,
        0,
        0
      ],
      [
        0,
        0,
        0,
        0
      ],
      [
        0,
        0,
        0,
        0
      ]
    ],
    "p": [
      0.3074324548412107,
      0.1672413241174222,
      0.16511960763668687,
      0.36020661340468024
    ]
  },
  "final_seed": 1609677230,
  "directed_sink": {
    "correct": [
      0.3508771929824561,
      0.6491228070175439
    ],
    "wrong_symmetric_c": [
      0.13043478260869565,
      0.24130434782608695
    ]
  }
}
```

## Reproduction

The following is the exact revised mathematical probe body run in the JavaScript orchestration environment, which supplies `text` and `store`. For a local JavaScript runtime those two reporting helpers can be replaced by printing JSON and discarding the stored copy. No external library, random network source or repository code is used. Initial seed 20260920 and final seed 1609677230 identify the generated sequence.

```javascript
let seed = 20260920;
function random_unit_value_next() {
  seed = (Math.imul(seed,1664525)+1013904223) >>> 0;
  return seed/4294967296;
}
function zeros_matrix_create_new(n,m) { return Array.from({length:n},()=>Array(m).fill(0)); }
function multiply_matrix_vector_values(A,x) { return A.map(row=>row.reduce((s,v,j)=>s+v*x[j],0)); }
function difference_vector_norm_compute(a,b) { return a.reduce((s,v,i)=>s+Math.abs(v-b[i]),0); }
function compile_factored_case_model(C,B,E,p,a,cover) {
  const n=p.length, f=C[0].length, columns=[], q=Array(n).fill(0);
  const originalC=C.map(row=>row.slice());
  const originalQ=C.map((row,i)=>row.reduce((s,v,h)=>s+v*B[i][h],0));
  C=C.map(row=>row.slice());
  for(let h=0;h<f;h++){const us=C.flatMap((row,i)=>row[h]?[i]:[]),vs=B.flatMap((row,i)=>row[h]?[i]:[]);if(us.length===1&&vs.length===1&&us[0]===vs[0])C[us[0]][h]=0;}
  for(let h=0;h<f;h++) columns.push([C.map(r=>r[h]),B.map(r=>r[h])]);
  for(let i=0;i<n;i++) q[i]=C[i].reduce((s,v,h)=>s+v*B[i][h],0);
  for(const [side,id] of cover) {
    let u=Array(n).fill(0),v=Array(n).fill(0);
    if(side==="s") { v[id]=1; for(let i=0;i<n;i++) if(!cover.some(([s,j])=>s==="d"&&j===i)) u[i]=E[i][id]; }
    else {u[id]=1; v=E[id].slice();}
    if(u.some(x=>x!==0)&&v.some(x=>x!==0))columns.push([u,v]);
  }
  const A=zeros_matrix_create_new(n,n);
  for(let i=0;i<n;i++) for(let j=0;j<n;j++) A[i][j]=originalC[i].reduce((s,v,h)=>s+v*B[j][h],0)-(i===j?originalQ[i]:0)+E[i][j];
  const d=Array.from({length:n},(_,j)=>A.reduce((s,row)=>s+row[j],0));
  const U=Array.from({length:n},(_,i)=>[...columns.map(([u])=>u[i]),p[i]]);
  const V=Array.from({length:n},(_,i)=>[...columns.map(([,v])=>d[i]?v[i]/d[i]:0),d[i]===0?1:0]);
  const H=d.map((v,i)=>v?v/(v+a*q[i]):1);
  const P=A.map(row=>row.map((v,j)=>d[j]?v/d[j]:p[A.indexOf(row)]));
  const weights=U[0].map((_,h)=>U.reduce((s,row)=>s+row[h],0));
  const reconstruct=h=>multiply_matrix_vector_values(U,h).map((v,i)=>H[i]*((1-a)*p[i]+a*v));
  const gather=x=>V[0].map((_,h)=>V.reduce((s,row,i)=>s+row[h]*x[i],0));
  return {A,d,U,V,H,P,weights,reconstruct,gather};
}
function greedy_defect_cover_build(E) {
  const n=E.length, cover=[],s=new Set(),d=new Set();
  for(let i=0;i<n;i++)for(let j=0;j<n;j++)if(E[i][j]&&!s.has(j)&&!d.has(i)){s.add(j);d.add(i);}
  for(const j of s)cover.push(["s",j]);for(const i of d)cover.push(["d",i]);
  return cover;
}
const stats={seed:20260920,cases:0,residual_checks:0,bound_checks:0,max_identity_error:0,max_solution_error:0,max_steps:0};
function verify_one_factored_case(C,B,E,p,a) {
 const M=compile_factored_case_model(C,B,E,p,a,greedy_defect_cover_build(E));
 let ref=p.slice();
 for(let t=0;t<20000;t++){const next=multiply_matrix_vector_values(M.P,ref).map((v,i)=>(1-a)*p[i]+a*v);if(difference_vector_norm_compute(next,ref)<1e-14){ref=next;break;}ref=next;}
 let h=M.gather(p),steps=0;
 for(;steps<20000;steps++){
  const x=M.reconstruct(h),next=M.gather(x),delta=next.map((v,j)=>v-h[j]);
  const actual=multiply_matrix_vector_values(M.P,x).map((v,i)=>(1-a)*p[i]+a*v-x[i]);
  const predicted=multiply_matrix_vector_values(M.U,delta).map(v=>a*v);
  const identity=difference_vector_norm_compute(actual,predicted);
  stats.max_identity_error=Math.max(stats.max_identity_error,identity);stats.residual_checks++;
  if(identity>1e-10)throw new Error("residual identity "+identity);
  const bound=a/(1-a)*delta.reduce((s,v,j)=>s+M.weights[j]*Math.abs(v),0);
  const error=difference_vector_norm_compute(x,ref);stats.bound_checks++;
  if(error>bound+1e-10)throw new Error("invalid bound "+error+" > "+bound);
  if(bound<1e-10){stats.max_solution_error=Math.max(stats.max_solution_error,error);break;}
  h=next;
 }
 if(steps===20000)throw new Error("not converged");
 if(steps>stats.max_steps) {stats.worst={n:p.length,f:C[0].length,alpha:a,max_q_over_d:Math.max(0,...M.d.map((d,i)=>d?C[i].reduce((z,v,k)=>z+v*B[i][k],0)/d:0)),C,B,E,p};} stats.max_steps=Math.max(stats.max_steps,steps);stats.cases++;
}
for(let mask=0;mask<512;mask++){
 const E=zeros_matrix_create_new(3,3);for(let i=0;i<3;i++)for(let j=0;j<3;j++)E[i][j]=(mask>>(i*3+j))&1;
 for(const p of [[1/3,1/3,1/3],[1,0,0],[0,0,1]])for(const a of [.1,.85,.97])verify_one_factored_case([[],[],[]],[[],[],[]],E,p,a);
}
for(let t=0;t<192;t++){
 const n=1+Math.floor(random_unit_value_next()*8),f=Math.floor(random_unit_value_next()*5);
 const C=zeros_matrix_create_new(n,f),B=zeros_matrix_create_new(n,f),E=zeros_matrix_create_new(n,n);
 for(let i=0;i<n;i++)for(let h=0;h<f;h++){C[i][h]=random_unit_value_next()<.55?0:1+Math.floor(random_unit_value_next()*3);B[i][h]=random_unit_value_next()<.55?0:1+Math.floor(random_unit_value_next()*3);}
 for(let i=0;i<n;i++)for(let j=0;j<n;j++)E[i][j]=random_unit_value_next()<.8?0:1+Math.floor(random_unit_value_next()*4);
 const raw=Array.from({length:n},()=>random_unit_value_next()),sum=raw.reduce((a,b)=>a+b,0),p=raw.map(v=>v/sum);
 for(const a of [0,.1,.85,.97])verify_one_factored_case(C,B,E,p,a);
}
stats.final_seed=seed;
stats.directed_sink={correct:[1/2.85,1.85/2.85],wrong_symmetric_c:[.15/(1-.85*.5)*.5,.15/(1-.85*.5)*.5*(1+.85)]};
text(stats);store("pagerank_probe_results",stats);
```

## First Version And Why It Changed

The first version did not prune canceled singleton factors and initialized h to the zero vector. It had the same 5,376 input/parameter cases, 1,612,996 residual checks, maximum residual-identity discrepancy 4.973452205625506e-15 and maximum 10,434 factor updates. The one-vertex worst case had `C=[[2,0,3,1]]`, `B=[[2,0,3,0]]`, `E=[[1]]`, p=[1], a=.97. Its base factors only introduced 13 units of diagonal mass canceled before PageRank.

The revised experiment prunes only fully canceled singleton base factors, initializes h from personalization, and preserves an unpruned expanded-matrix oracle. Combined maximum updates became 2,899. An ablation separating the two changes is still needed before crediting either with that full reduction.

## Optional Jacobi Comparison

The same revised probe was also run with two changes: return `diagonal[j]=a*sum_i U[i,j]*H_i*V[i,j]` from the model, and replace `h=next` by `h[j]=(next[j]-diagonal[j]*h[j])/(1-diagonal[j])`. The residual and error checks still use the original gather-minus-state difference, not the Jacobi update difference.

Observed: 5,376 cases; 666,596 residual and bound checks; maximum identity discrepancy 2.4424906541753444e-15; maximum final L1 error 9.999080730072052e-11; maximum 1,208 updates. The worst graph was the same four-vertex weighted case recorded above. This is a third evaluated schedule, not evidence that Jacobi always wins: aggregate iterations increased relative to the revised fixed point.

## Remaining Uncovered Risks

- No rigorous outward rounding or validated summation-error propagation.
- No disk-layout, row-chunk, allocation-lifetime or concurrent-reader implementation.
- No large-scale timing, RAM, source export, factor-discovery or cover-construction measurement.
- No differential run against GDS's actual finite-iteration/scaler semantics.
- Greedy cover in the oracle establishes factor identity, not optimal cover size or practical matching-builder memory.
- Full nearest-prior-art comparison and independent review are tracked separately.
