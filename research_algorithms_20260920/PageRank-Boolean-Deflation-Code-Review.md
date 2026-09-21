# Boolean PageRank Deflation Implementation Audit

Date: 2026-09-21. Bounded sidecar to the terminal mathematical review.
Ownership: this document only. No implementation, publisher, control, old
review, or other-file edits. Test fixtures are temporary data, removed on exit.
No literature search, repeated theorem review, commits, or timing benchmarks.

**Lead integration update, 2026-09-21:** the original finding and inspected
hashes below remain historical audit evidence. The lead reproduced the prefix
race, added a failing regression, then added initial/final descriptor
size/mtime/ctime checks. The embedded checker's mutation expectations are now
updated for that detector and its unchanged numerical probes are replayed on
the patched implementation. This detects the reproduced write; it does not
replace exclusive ownership of staged bytes or claim adversarial immutability.
The code patch and these checker adaptations are lead changes, not a second
independent audit. The separately implemented publisher documents the stable
private-file ownership boundary; it was outside this audit's scope.

## Findings

### P1: Matching Scan Hashes Do Not Freeze The Examined File

`boolean_rank_deflated_certificate.py:108` rewinds the same descriptor;
lines 120-138 consume and compare the second byte stream. The shared
`_finish_checked_output_pass` at `boolean_rank_output_certificate.py:96`
checks end-of-file, length and source snapshot, not that bytes already consumed
remain unchanged. A writer can modify a consumed prefix during the second
scan. Both hashes then match the old sequence, but not the descriptor's current
contents. This is possible without changing source rows, IDs, snapshot, file
length or inode, and before certification returns.

Independent reproduction: F=4, h=1, uniform personalization, six scores equal
to binary64 1/6, alpha=0.85 and epsilon=1e-10. Immediately after the second
scan consumes the first score, write 0.5 into that score's eight bytes using
another descriptor for the same inode. Initial result:

```text
accepted: True
l1_error_upper: 5.55111512312578270211921823469964185932400439835484282688492E-17
current-file exact L1 error, displayed as float: 0.33333333333333337
receipt hash matches pre-mutation bytes: True
receipt hash matches current-file bytes: False
live source iterators after return: 0
private scratch remaining: none
```

**Scope of the finding:** the numerical bound is valid for the hashed sequence
that both scans read. It is not valid for the changed file. Therefore this is
a P1 integration hazard if `accepted=True` is treated as authorization to
publish the current file without ensuring byte identity. It is not a false
residual bound for the receipt's identified bytes. The generic helper already
documents that a receipt does not attest a future pathname state; the same
limitation applies to in-place changes behind the read cursor, even during
the call. No claim about the new publisher is made.

Required boundary: exclusive ownership/immutability of the staged output
through checking and its eventual use, or an equivalent mechanism that binds
the consumed bytes to the bytes used. An additional hash or stat check alone
does not prove perpetual immutability. Metadata checks can detect more
accidental writes but do not remove the ownership requirement. The checker
below preserves this observation and separately tests a between-pass mutation
that the current code correctly rejects.

### Other Results

No additional implementation defect was identified by the scoped static
inspection. Independent numerical, malformed-input and lifecycle execution
results are recorded after the embedded checker. In particular, this is not
an assertion of safety for malicious prepared sources, mutable snapshot
providers, private-scratch tampering or a publisher not inspected here.

## Inspected Scope

Complete target modules at the initial inspected snapshot:

| File | SHA-256 |
| --- | --- |
| `experiments/boolean_rank_deflated_certificate.py` | `c76c9514cf042810b7a3a8fd0b1218d84a642a3dd9264362da1c6b05c29b4453` |
| `experiments/boolean_rank_spectral_source.py` | `989a8080555548c3c1e706d11b922a4ca5ae60df6227ad5ab31bf8b25b7832c6` |
| `experiments/boolean_rank_output_certificate.py` | `70e6a74021e486cd2a9a5bc04ba7444f389bf262e1451d57cd9712a9700ba1d3` |

The shared-helper audit covers `_enclose_exact_fraction_value`,
`_iterate_checked_class_records`, `_iterate_aligned_class_values`,
`_finish_checked_output_pass`, and their iterator-lifetime helper. The generic
verifier's main function was read as surrounding contract context, not
independently re-audited end-to-end. Also inspected:

- `stream_boolean_rank_solver.py:14` iterator-lifetime context manager only;
  no solver algorithm or output reconstruction audit.
- `boolean_rank_sqlite_source.py:280` bounded cursor lifetime and `:350`
  read transaction, cached metadata/properties, class and vertex iterator
  methods and source close. These establish consumed provider assumptions,
  not a fresh builder/schema/preparation audit.
- Existing `test_boolean_rank_deflated_certificate.py` and
  `test_boolean_rank_spectral_source.py` were read and replayed.

Skills/evidence: using-superpowers, verification-before-completion,
codebase-memory-mcp and systematic-debugging. The graph lookup found no nodes
for these new modules in the repository index; direct named-file inspection
was used without rebuilding or writing an index. Line references are to the
hashes above unless a later verification update explicitly says otherwise.

## Directed Arithmetic

The inspected implementation follows the approved arithmetic schedule:

- Lines 56-74 reject non-float/nonfinite alpha and epsilon, alpha outside
  [0,1), invalid precision and capacity, and a nonpositive denominator.
  `Fraction.from_float` and `Decimal.from_float` preserve the input float's
  value. Fresh FLOOR/CEILING contexts avoid ambient-context precision/traps.
- Lines 90-106 accumulate actual serialized score mass outward, form the
  absolute-value image of `1-s`, and round the nonnegative residual-mass
  product and square downward. `copy_abs()` avoids implicit ambient rounding;
  zero-crossing absolute intervals have zero lower endpoint.
- Lines 117-130 add the two factor intervals, subtract the duplicated class
  interval and subtract the current row's share. Lower endpoints subtract
  upper endpoints; upper endpoints subtract lower endpoints. Personalization
  and damping products have nonnegative factors as required by the source
  and score contracts. Negative lower incoming endpoints caused by interval
  width are retained rather than incorrectly clipped.
- Lines 131-143 square residual magnitude upper bounds and accumulate upward,
  multiply by exact Decimal(n), subtract the lower mass-square, reject negative
  variance, and divide by a downward denominator. The sqrt helper uses the
  same explicit context for nearest sqrt and its successor, with a zero case.
- Lines 144-147 compare the directed bound to the exact float epsilon and
  report the generic residual diagnostic separately. The generic number is
  not used to decide deflated acceptance.

Overflow/invalid/division exceptions abort rather than accept. For admitted
binary64 source/score data and the validated finite integer domain, MIN_EMIN
and MAX_EMAX leave ample decimal exponent range. Precision remains user-sized
and can consume resources; no hard memory bound follows. The independent
oracle checks exact mass, rho-square, residual L1, variance, sqrt and final
L1 enclosure against actual byte values, including very low precision and
extreme binary64 values. It imports neither the project's rational solver
nor the project's resident test fixture.

## Eligibility And Helper Assumptions

`boolean_rank_spectral_source.py:13-26` validates F/capacity before any scan,
positive integer counts, exactly P=F(F-1)/2 active/all classes, positive exact
total weight and zero isolate weight. Lines 30-53 stream every canonical
pair in order, require contiguous zero-based class IDs, equal positive h,
the exact positive d with h,d<=2^53, exact nonnegative class weights, total
weight agreement, n=Ph, and an unchanged snapshot token. No pair set/list is
materialized by this validator. Contiguous IDs are a deliberately stronger
provider convention than the mathematical theorem needs, not a defect for
the supplied builder.

Shared helpers enforce increasing class IDs, valid factor IDs, per-class
heights, finite nonnegative float weights, strictly increasing original IDs
within each class, exact ID alignment to each 16-byte output record, finite
nonnegative scores, row counts and exact total length. Negative zero is
numerically zero and is allowed; its distinct bytes remain in the hash.

The following are trusted-source premises, not newly verified here:

- Global original-ID uniqueness across classes, the correspondence between
  membership rows and class metadata, and original-weight sums matching the
  class and total Fractions. Helpers check within-class IDs and row lengths,
  not a global ID set or original-weight re-sum.
- The source actually stays pinned. A snapshot property merely returning the
  same token cannot authenticate hostile changes. The inspected SQLite
  provider opens `mode=ro`, begins a read transaction before reading its
  manifest, and retains that connection. Its UUID is explicitly a build ID,
  not a content hash. Database corruption/hostile-file validation is excluded.
- The two output scans use the same descriptor and unbuffered fixed-record
  reads. This avoids stale userspace output buffers and detects changed bytes
  observed by the two scans. It does not freeze the file or pathname.
- Class scratch is a private locally created temporary file. Its alignment
  and end are checked. Its contents are not a hostile-input format: record
  lengths, decimal string sizes, endpoint validity and cryptographic integrity
  are not independently defended against an attacker modifying that scratch.

## Lifetime And State

Every opened class/vertex iterator is under a lifetime context; early record
validation errors and BaseException cancellation unwind those contexts.
Output, scratch file and temporary directory are nested context managers.
The provider stays caller-owned and is reusable after a failed certificate.
Exceptions from provider iteration and I/O propagate rather than returning
an accepted result. Source methods that allocate and then raise before
returning an iterator remain responsible for their own cleanup.

| State/work | Actual scope of the accounting |
| --- | --- |
| Retained endpoint slots | Two Python lists with F references each, up to 2F nonzero Decimal endpoint values, plus constant scalar/context/string/hash state |
| Source scans | One eligibility scan plus two class scans: 3P class records; two vertex scans: 2n original rows |
| SQLite calls | Three class cursor opens, 2P vertex cursor opens and 2P height/signature metadata fetches; these are not all exposed in the certificate receipt |
| Scratch | P sequential JSONL records, not an in-memory P-vector; decimal precision and exponent spelling affect bytes; normal Python file buffering remains |
| Output | 16n-byte file, two descriptor reads totaling 32n payload bytes, plus EOF probes; returned hash identifies the read sequence |
| Exact arithmetic | P class-weight Fraction additions, per-original-row exact weight normalization on pass two, exact damping/gap fractions and precision-dependent Decimal operations |

`retained_decimal_values=2*F` names endpoint-array slots, not all live Decimal
objects and not a byte/RSS measurement. Temporary row/class Decimals, parsed
JSON/string objects, integers/Fractions, the source/cache/runtime, buffers,
output and scratch remain charged. The routine retains no changing P- or
n-vector in its own implementation; the test harness deliberately does.
No array reservation is a physical allocation guarantee. The implementation
does not claim a physical cap or universal performance win.

## Existing Test Replay

Python 3.11 with bytecode writes disabled; both commands exited 0:

```sh
/Users/amuldotexe/.local/bin/python3.11 -B -m unittest discover -s research_algorithms_20260920/experiments -p 'test_boolean_rank_deflated_certificate.py'
/Users/amuldotexe/.local/bin/python3.11 -B -m unittest discover -s research_algorithms_20260920/experiments -p 'test_boolean_rank_spectral_source.py'
```

Initial receipt: 10 certificate tests and 4 spectral-source tests passed.
Their displayed unittest durations are not benchmark evidence and are not
reproduced as performance results.

## Independent Runnable Checker

Run from repository root. The sole Python block constructs its own provider
and expanded exact linear-system oracle, writes only temporary input/output
data, and imports the modules under audit. The SQLite subprobes execute the
existing builder only to generate temporary trusted inputs; they do not
certify that builder. In-memory mocks inject I/O and cancellation faults;
no implementation files are patched. The original consumed-prefix observation
is preserved above. The updated checker requires rejection of that concrete
write after the lead patch.

```python
from contextlib import contextmanager
from decimal import Decimal, Inexact, localcontext
from fractions import Fraction as Q
from itertools import combinations
from pathlib import Path
from unittest.mock import patch
import builtins
import hashlib
import math
import os
import struct
import sys
import tempfile

sys.path.insert(0, 'research_algorithms_20260920/experiments')
import boolean_rank_deflated_certificate as cert
from boolean_rank_spectral_source import validate_boolean_spectral_source
from boolean_rank_sqlite_source import build_boolean_rank_source, SqliteBooleanRankSource


class IndependentPairSource:
    def __init__(self, factors=4, height=1, mode=0):
        self.factor_count = factors
        self.class_count = self.active_class_count = factors*(factors-1)//2
        self.vertex_count = height*self.class_count
        self.snapshot_id, self.isolate_weight = 'independent-frozen-source', Q(0)
        self.rows, self.classes, self.by_class = [], [], []
        self.class_scans = self.class_reads = self.vertex_reads = self.vertex_calls = 0
        self.live = self.peak = 0
        self.hook = lambda *args: None
        extremes = (math.ulp(0.0), float.fromhex('0x1.fffffffffffffp+1023'), 0.0, 1.0)
        for cid, pair in enumerate(combinations(range(factors), 2)):
            group_rows = []
            for offset in range(height):
                index = cid*height+offset
                weight = (1.0 if mode==0 else float((index*7+3)%11) if mode==1
                          else extremes[index%len(extremes)])
                original = 10+7*index
                self.rows.append((original,pair,weight))
                group_rows.append((original,weight))
            self.by_class.append(group_rows)
            self.classes.append((cid,pair,height,height*(2*factors-3)-1,
                                 sum((Q(weight) for _,weight in group_rows),Q(0))))
        self.total_weight = sum((Q(weight) for _,_,weight in self.rows),Q(0))

    def iterate_class_records(self):
        self.class_scans += 1
        self.live += 1
        self.peak = max(self.peak,self.live)
        try:
            self.hook('class_start',self,None,None)
            for index, row in enumerate(self.classes):
                self.hook('class_before',self,index,None)
                self.class_reads += 1
                yield row
            self.hook('class_end',self,None,None)
        finally:
            self.live -= 1

    def iterate_class_vertex_rows(self, cid):
        self.vertex_calls += 1
        self.live += 1
        self.peak = max(self.peak,self.live)
        try:
            for index, row in enumerate(self.by_class[cid]):
                self.hook('vertex_before',self,cid,index)
                self.vertex_reads += 1
                yield row
                self.hook('vertex_after',self,cid,index)
        finally:
            self.live -= 1


def solve_independent_exact_system(source, alpha):
    rows, n, a = source.rows, len(source.rows), Q(alpha)
    adjacency = [[int(i!=j and bool(set(left[1]) & set(right[1])))
                  for j,right in enumerate(rows)] for i,left in enumerate(rows)]
    degrees = list(map(sum,adjacency))
    p = [Q(weight)/source.total_weight for _,_,weight in rows]
    transition = [[Q(adjacency[i][j],degrees[j]) for j in range(n)] for i in range(n)]
    matrix = [[Q(int(i==j))-a*transition[i][j] for j in range(n)] for i in range(n)]
    rhs = [(1-a)*value for value in p]
    work = [row[:]+[value] for row,value in zip(matrix,rhs)]
    for column in range(n):
        pivot = next(row for row in range(column,n) if work[row][column])
        work[pivot],work[column] = work[column],work[pivot]
        divisor = work[column][column]
        work[column] = [value/divisor for value in work[column]]
        for row in range(n):
            if row!=column and work[row][column]:
                multiplier = work[row][column]
                work[row] = [value-multiplier*base for value,base in zip(work[row],work[column])]
    truth = [row[-1] for row in work]
    assert sum(truth)==1 and min(truth)>=0
    assert all(sum(coefficient*x for coefficient,x in zip(row,truth))==value
               for row,value in zip(matrix,rhs))
    return truth,p,transition


def encode_actual_score_bytes(source, scores):
    return b''.join(struct.pack('<Qd',row[0],score) for row,score in zip(source.rows,scores))


def verify_exact_receipt_enclosures(report, raw, truth, p, transition, alpha):
    x = [Q(score) for _,score in struct.iter_unpack('<Qd',raw)]
    a, n = Q(alpha),len(x)
    residual = [(1-a)*p[i]+a*sum(transition[i][j]*x[j] for j in range(n))-x[i]
                for i in range(n)]
    mass,rho = abs(sum(x)-1),sum(residual)
    variance = n*sum(value*value for value in residual)-rho*rho
    error = sum(abs(value-exact) for value,exact in zip(x,truth))
    assert variance>=0 and rho==(1-a)*(1-sum(x))
    assert Q(report['mass_error_upper'])>=mass
    assert Q(0)<=Q(report['rho_squared_lower'])<=rho*rho
    assert Q(report['variance_upper'])>=variance
    assert Q(report['root_upper'])**2>=Q(report['variance_upper'])
    assert Q(report['residual_l1_upper'])>=sum(map(abs,residual))
    assert Q(report['generic_l1_error_upper'])>=sum(map(abs,residual))/(1-a)
    assert Q(0)<Q(report['spectral_denominator_lower'])<=1-a*Q(report['spectral_upper'])
    assert Q(report['l1_error_upper'])>=error
    assert report['accepted']==(Q(report['l1_error_upper'])<=Q(1e-10))
    if report['accepted']:
        assert error<=Q(1e-10)
    assert report['output_sha256']==hashlib.sha256(raw).hexdigest()


class FaultedFileHandle:
    def __init__(self, handle, method):
        self.handle,self.method = handle,method
    def __getattr__(self, name):
        if name==self.method:
            def raise_injected_io_failure(*args,**kwargs):
                raise OSError('injected '+name+' failure')
            return raise_injected_io_failure
        return getattr(self.handle,name)
    def __enter__(self):
        return self
    def __exit__(self,*args):
        self.handle.close()


@contextmanager
def track_certificate_handle_lifetime(output_fault=None, scratch_fault=None):
    handles = []
    builtin_open,path_open = builtins.open,Path.open
    def track_output_file_open(*args,**kwargs):
        handle = builtin_open(*args,**kwargs)
        handles.append(handle)
        return FaultedFileHandle(handle,output_fault) if output_fault else handle
    def track_scratch_file_open(path,*args,**kwargs):
        handle = path_open(path,*args,**kwargs)
        handles.append(handle)
        return FaultedFileHandle(handle,scratch_fault) if scratch_fault else handle
    try:
        with patch.object(cert,'open',track_output_file_open,create=True), patch.object(Path,'open',track_scratch_file_open):
            yield
    finally:
        assert all(handle.closed for handle in handles)


def assert_private_scratch_removed(root):
    assert not list(root.glob('deflated-rank-certificate-*'))


def run_expected_failure_probe(source, path, root, expected=ValueError, **kwargs):
    try:
        with track_certificate_handle_lifetime(kwargs.pop('output_fault',None),kwargs.pop('scratch_fault',None)):
            cert.certify_boolean_deflated_output(source,path,alpha=0.85,epsilon=1e-10,
                max_factor_slots=source.factor_count,scratch_dir=root,**kwargs)
    except expected:
        pass
    else:
        raise AssertionError('expected failure was not raised')
    assert source.live==0
    assert_private_scratch_removed(root)


numeric = accepted = refused = metadata = malformed = lifecycle = 0
with tempfile.TemporaryDirectory(prefix='independent-deflation-audit-') as directory:
    root,path = Path(directory),Path(directory)/'scores.bin'
    for factors,height in ((4,1),(4,2),(5,1)):
        for mode in (0,1,2):
            for alpha in (0.0,math.ulp(0.0),0.5,0.85,math.nextafter(1.0,0.0)):
                template = IndependentPairSource(factors,height,mode)
                truth,p,transition = solve_independent_exact_system(template,alpha)
                rounded = [float(value) for value in truth]
                variants = [rounded,[value/2 for value in rounded],[0.0]*len(rounded),
                            [value+(0.0625 if index==0 else 0.0) for index,value in enumerate(rounded)]]
                for scores in variants:
                    raw = encode_actual_score_bytes(template,scores)
                    path.write_bytes(raw)
                    for precision in (1,7,34,60):
                        source = IndependentPairSource(factors,height,mode)
                        with track_certificate_handle_lifetime():
                            report = cert.certify_boolean_deflated_output(source,path,alpha=alpha,
                                epsilon=1e-10,precision=precision,max_factor_slots=factors,scratch_dir=root)
                        verify_exact_receipt_enclosures(report,raw,truth,p,transition,alpha)
                        assert source.live==0 and source.peak==2
                        assert (source.class_scans,source.class_reads,source.vertex_reads,source.vertex_calls)==(
                            3,3*source.class_count,2*source.vertex_count,2*source.class_count)
                        assert report['nrows']==source.vertex_count
                        assert report['validation_class_rows']==source.class_count
                        assert report['retained_decimal_values']==2*factors
                        assert report['scratch_records']==source.class_count
                        assert report['output_read_bytes']==32*source.vertex_count
                        assert path.read_bytes()==raw
                        assert_private_scratch_removed(root)
                        numeric += 1
                        accepted += int(report['accepted'])
                        refused += int(not report['accepted'])

    source = IndependentPairSource()
    truth,p,transition = solve_independent_exact_system(source,0.85)
    for scores in ([math.ulp(0.0)]*6, [float.fromhex('0x1.fffffffffffffp+1023')]*6, [-0.0]*6):
        raw = encode_actual_score_bytes(source,scores)
        path.write_bytes(raw)
        report = cert.certify_boolean_deflated_output(source,path,alpha=0.85,epsilon=1e-10,
                    max_factor_slots=4,scratch_dir=root)
        verify_exact_receipt_enclosures(report,raw,truth,p,transition,0.85)
        numeric += 1
        accepted += int(report['accepted'])
        refused += int(not report['accepted'])

    source = IndependentPairSource()
    raw = encode_actual_score_bytes(source,[1/6]*6)
    path.write_bytes(raw)
    baseline = cert.certify_boolean_deflated_output(source,path,alpha=0.85,epsilon=1e-10,max_factor_slots=4,scratch_dir=root)
    with localcontext() as context:
        context.prec=1
        context.traps[Inexact]=True
        hostile = cert.certify_boolean_deflated_output(source,path,alpha=0.85,epsilon=1e-10,max_factor_slots=4,scratch_dir=root)
    assert hostile['l1_error_upper']==baseline['l1_error_upper']

    invalid_bytes = [b'',raw[:-1],raw[:-16],raw+b'x',raw+raw[:16],
                     struct.pack('<Qd',999999,1/6)+raw[16:],raw[16:32]+raw[:16]+raw[32:],
                     raw[:16]+raw[:16]+raw[32:]]
    invalid_bytes += [struct.pack('<Qd',10,value)+raw[16:] for value in (float('nan'),float('inf'),-float('inf'),-1.0)]
    for data in invalid_bytes:
        path.write_bytes(data)
        for _ in range(2):
            source = IndependentPairSource()
            run_expected_failure_probe(source,path,root)
            malformed += 1

    path.write_bytes(raw)
    for kind in ('missing','extra','repeat','reverse','group','height','degree','class_weight','count','total','isolate','id'):
        source = IndependentPairSource()
        if kind=='missing': source.classes.pop()
        elif kind=='extra': source.classes.append(source.classes[-1])
        elif kind=='repeat': source.classes[1]=source.classes[0]
        elif kind=='reverse': source.classes.reverse()
        elif kind=='count': source.vertex_count+=1
        elif kind=='total': source.total_weight+=1
        elif kind=='isolate': source.isolate_weight=Q(1)
        else:
            row=list(source.classes[0])
            position={'group':1,'height':2,'degree':3,'class_weight':4,'id':0}[kind]
            row[position]={'group':(0,0),'height':2,'degree':5,'class_weight':Q(-1),'id':1}[kind]
            source.classes[0]=tuple(row)
        run_expected_failure_probe(source,path,root)
        assert source.vertex_calls==0
        metadata += 1
    for capacity in (3,-1,True,4.0):
        source = IndependentPairSource()
        try: validate_boolean_spectral_source(source,max_factor_slots=capacity)
        except ValueError: pass
        else: raise AssertionError('invalid reservation admitted')
        assert source.class_scans==0 and source.live==0
        metadata += 1

    for event,scan,error_type in (('class_start',1,OSError),('class_before',2,OSError),
                                ('vertex_before',2,OSError),('vertex_before',3,OSError),
                                ('vertex_after',2,KeyboardInterrupt),('vertex_after',3,KeyboardInterrupt)):
        source = IndependentPairSource(height=2)
        path.write_bytes(encode_actual_score_bytes(source,[1/12]*12))
        def inject_source_iteration_failure(observed,current,cid,index):
            if observed==event and current.class_scans==scan:
                raise error_type('injected source failure')
        source.hook=inject_source_iteration_failure
        run_expected_failure_probe(source,path,root,expected=error_type)
        lifecycle += 1
    for scan in (1,3):
        source = IndependentPairSource()
        path.write_bytes(raw)
        def change_reported_snapshot_identity(event,current,cid,index):
            if event=='class_end' and current.class_scans==scan:
                current.snapshot_id='changed'
        source.hook=change_reported_snapshot_identity
        run_expected_failure_probe(source,path,root)
        lifecycle += 1
    for output_fault,scratch_fault in (('read',None),(None,'write'),(None,'readline')):
        path.write_bytes(raw)
        run_expected_failure_probe(IndependentPairSource(),path,root,expected=OSError,
                                   output_fault=output_fault,scratch_fault=scratch_fault)
        lifecycle += 1

    # Same inode: an observed between-pass modification must be rejected.
    path.write_bytes(raw)
    source=IndependentPairSource()
    def mutate_before_second_scan(event,current,cid,index):
        if event=='class_start' and current.class_scans==3:
            with path.open('r+b',buffering=0) as writer:
                writer.seek(8); writer.write(struct.pack('<d',0.5))
    source.hook=mutate_before_second_scan
    run_expected_failure_probe(source,path,root)

    # Same inode: an already-consumed prefix is outside the two-hash observation.
    path.write_bytes(raw)
    source=IndependentPairSource()
    original_inode=path.stat().st_ino
    def mutate_consumed_output_prefix(event,current,cid,index):
        if event=='vertex_after' and current.class_scans==3 and cid==0 and index==0:
            with path.open('r+b',buffering=0) as writer:
                writer.seek(8); writer.write(struct.pack('<d',0.5))
    source.hook=mutate_consumed_output_prefix
    run_expected_failure_probe(source,path,root)
    changed=path.read_bytes()
    current_error=sum(abs(Q(value)-Q(1,6)) for _,value in struct.iter_unpack('<Qd',changed))
    assert path.stat().st_ino==original_inode
    assert current_error>Q(1e-10)
    assert source.live==0
    assert_private_scratch_removed(root)

    # Replacing the pathname must not redirect the retained descriptor.
    path.write_bytes(raw)
    source=IndependentPairSource()
    replacement=root/'replacement.bin'
    def replace_output_pathname_only(event,current,cid,index):
        if event=='class_start' and current.class_scans==3:
            replacement.write_bytes(encode_actual_score_bytes(current,[0.5]*6))
            os.replace(replacement,path)
    source.hook=replace_output_pathname_only
    try:
        detached=cert.certify_boolean_deflated_output(source,path,alpha=0.85,epsilon=1e-10,max_factor_slots=4,scratch_dir=root)
    except ValueError as error:
        assert 'changed' in str(error)
    else:
        assert detached['accepted'] and detached['output_sha256']==hashlib.sha256(raw).hexdigest()
        assert detached['output_sha256']!=hashlib.sha256(path.read_bytes()).hexdigest()
    assert source.live==0

    # Real SQLite provider: failure/cancellation closes both cursors and permits reuse.
    template=IndependentPairSource(height=2,mode=1)
    database=root/'source.sqlite'
    build_boolean_rank_source(database,template.rows,factor_count=4)
    truth,p,transition=solve_independent_exact_system(template,0.85)
    good=encode_actual_score_bytes(template,[float(value) for value in truth])
    sqlite_failures=0
    with SqliteBooleanRankSource(database) as provider:
        for data in (good[:-1],struct.pack('<Qd',999999,0.1)+good[16:],
                     struct.pack('<Qd',template.rows[0][0],float('nan'))+good[16:]):
            for _ in range(3):
                path.write_bytes(data)
                try:
                    cert.certify_boolean_deflated_output(provider,path,alpha=0.85,epsilon=1e-10,
                        max_factor_slots=4,scratch_dir=root)
                except ValueError: pass
                else: raise AssertionError('invalid SQLite-aligned output admitted')
                assert provider.events['active_cursors']==0
                assert_private_scratch_removed(root)
                sqlite_failures+=1
        path.write_bytes(good)
        original_method=provider.iterate_class_vertex_rows
        def interrupt_sqlite_vertex_iteration(cid):
            rows=original_method(cid)
            try:
                yield next(rows)
                raise KeyboardInterrupt('cancel SQLite certificate')
            finally:
                rows.close()
        with patch.object(provider,'iterate_class_vertex_rows',interrupt_sqlite_vertex_iteration):
            try:
                cert.certify_boolean_deflated_output(provider,path,alpha=0.85,epsilon=1e-10,
                    max_factor_slots=4,scratch_dir=root)
            except KeyboardInterrupt: pass
            else: raise AssertionError('cancellation swallowed')
        assert provider.events['active_cursors']==0
        assert_private_scratch_removed(root)
        recovered=cert.certify_boolean_deflated_output(provider,path,alpha=0.85,epsilon=1e-10,
            max_factor_slots=4,scratch_dir=root)
        verify_exact_receipt_enclosures(recovered,good,truth,p,transition,0.85)
        assert recovered['accepted'] and provider.events['active_cursors']==0
        assert provider.events['peak_cursors']<=2
    assert_private_scratch_removed(root)

print('Independent exact/actual-byte enclosures:',numeric,'accepted:',accepted,'refused:',refused)
print('Malformed-byte refusals:',malformed,'eligibility/reservation refusals:',metadata)
print('Injected source/snapshot/I/O/cancellation cleanup probes:',lifecycle)
print('SQLite malformed-byte refusals:',sqlite_failures,'cancellation/reuse: PASS')
print('Ambient Decimal isolation, iterator/scan accounting, handle closure, scratch removal: PASS')
print('Between-pass same-inode mutation: REJECTED')
print('Consumed-prefix same-inode mutation: REJECTED; changed-file error:',float(current_error))
print('Path replacement: rejected on metadata change or receipt bound only to old descriptor')
print('Diagnostic complete; no code changes or timing benchmarks; temporary data removed.')
```

Reproduction:

```sh
awk '/^```python$/{block=1;next} /^```$/{if(block)exit} block' research_algorithms_20260920/PageRank-Boolean-Deflation-Code-Review.md | /Users/amuldotexe/.local/bin/python3.11 -B
```

## Explicit Exclusions

Not audited: new publisher/solver/certificate option integration; publication
rename, commit or rollback mechanics; uniform-pair exact-target control owned
by the other agent; solver numerical methods; source builder/schema correctness
or hostile SQLite validation; malicious scratch modification; process-crash,
power-loss, disk-full or filesystem durability guarantees; nonlocal filesystems;
parallel source consumers; cryptographic collision attacks; native allocator,
RSS or hard byte limits; timing/performance; mathematics or literature already
closed in the earlier review. Mocked I/O exceptions test Python cleanup, not
every filesystem's failure semantics.

## Independent Execution Receipt

The complete embedded checker ran under Python 3.11 with `-B` and exited 0:

```text
Independent exact/actual-byte enclosures: 723 accepted: 90 refused: 633
Malformed-byte refusals: 24 eligibility/reservation refusals: 16
Injected source/snapshot/I/O/cancellation cleanup probes: 11
SQLite malformed-byte refusals: 9 cancellation/reuse: PASS
Ambient Decimal isolation, iterator/scan accounting, handle closure, scratch removal: PASS
Between-pass same-inode mutation: REJECTED
Consumed-prefix same-inode mutation: ACCEPTED OLD HASH; current error: 0.33333333333333337
Consumed-prefix reported bound: 5.55111512312578270211921823469964185932400439835484282688492E-17
Path replacement: receipt remains bound to old descriptor, not replacement path
Diagnostic complete; no code changes or timing benchmarks; temporary data removed.
```

The 723 numerical checks are 720 combinations of three all-pair sizes, three
weight patterns, five binary64 damping values, four candidate modes and four
precisions, plus three extreme-score checks. Ninety accepted bounds and all
633 refused bounds enclose the independent exact error. Acceptance/refusal
counts are correctness fixtures, not measured operational admission rates.
The 24 malformed-byte probes repeat 12 shapes; they are not 24 unique bug
classes. The 11 injected cleanup probes cover provider errors in both passes,
cancellation in both passes, changed snapshot tokens, output read failure,
scratch write failure and scratch read failure. The SQLite provider then
survives nine malformed-output attempts and cancellation and certifies a valid
answer again with no open cursors. No timing measurement was collected.

## Terminal Disposition

**This bounded implementation audit is terminal.** One concrete consumed-prefix
mutation finding is retained, conditional on treating the receipt as authority
over mutable current-file contents. Within the explicitly trusted/pinned-source,
private-scratch and stable-output assumptions, no additional directed-arithmetic,
eligibility, iteration, cleanup or declared-state-accounting defect was found
by this inspection and these finite probes.

The finding is not a demand to redesign the publisher inside this task: the
lead owns that separate integration, and no publisher code was inspected.
No code fix, default-enablement approval, benchmark claim, or physical resource
guarantee is supplied. The earlier mathematical review remains terminal and
unchanged. Only this owned review Markdown was authored.
