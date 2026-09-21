"""Stationary/clique controls versus the previously best applicable CG plan.

Uses frozen native inputs; fresh serial workers and full original-ID output.
Refusals stay in the record. No physical RAM cap or general Neo4j claim.
"""

import argparse
from fractions import Fraction
import json
from pathlib import Path
import platform
import subprocess
import sys
import time

from boolean_rank_native_direct import publish_native_direct_result, NativeDirectCertificateRefusal
from boolean_rank_native_pipeline import publish_native_rank_result, NativeRankCertificateRefusal
from boolean_rank_sqlite_source import SqliteBooleanRankSource
from measure_boolean_rank_native import summarize_native_control_bracket
from measure_boolean_rank_workflow import (
    audit_boolean_output_pair, capture_process_peak_memory, hash_measurement_file_bytes,
)


def run_direct_measurement_query(source_root, output_root, case, mode, trial):
    output=output_root/f"{case['name']}-{trial}-{mode}.ranks"
    result=dict(case=case["name"],mode=mode,trial=trial,output=output.name,accepted=False)
    started=time.perf_counter()
    with SqliteBooleanRankSource(source_root/f"{case['name']}.sqlite",cache_kib=256) as source:
        try:
            arguments=dict(alpha=case["alpha"],epsilon=case["epsilon"],max_factor_slots=source.factor_count,precision=60)
            if mode in ("stationary","clique-direct"):
                receipt=publish_native_direct_result(source,output,method=mode,**arguments)
            elif mode in ("factor-generic","factor-native"):
                receipt=publish_native_rank_result(source,output,method="factor-cg",
                    certificate_method="generic" if mode=="factor-generic" else "native-gap",
                    max_class_slots=source.active_class_count,tolerance=1e-13,maximum_iterations=1000,**arguments)
            else:
                raise ValueError("unknown direct comparison mode")
            result.update(pipeline=receipt,accepted=True)
        except (ValueError,ArithmeticError,OSError) as error:
            result.update(failure_type=type(error).__name__,failure=str(error))
            if isinstance(error,(NativeDirectCertificateRefusal,NativeRankCertificateRefusal)):
                result["pipeline"]=error.receipt
        result["source_events"]=dict(source.events)
        if source.events["active_cursors"]:
            raise AssertionError("direct comparison leaked source cursor")
    result.update(query_seconds=time.perf_counter()-started,process_maxrss_bytes=capture_process_peak_memory())
    if result["accepted"]:
        result["postprocess_output_sha256"]=hash_measurement_file_bytes(output)
        if result["postprocess_output_sha256"]!=result["pipeline"]["output_sha256"]:
            raise AssertionError("direct comparison output hash changed")
    elif output.exists():
        raise AssertionError("refused direct comparison published output")
    return result


def execute_direct_measurement_study(source_root, output_root):
    script=Path(__file__).resolve()
    prior_path=source_root/"receipt.json"
    prior=json.loads(prior_path.read_text())
    files=tuple(prior["source_hashes"])+("boolean_rank_native_direct.py","measure_boolean_rank_direct.py")
    for name,expected in prior["source_hashes"].items():
        if hash_measurement_file_bytes(script.parent/name)!=expected:
            raise ValueError(f"frozen native implementation changed: {name}")
    for entry in prior["preparations"]:
        if hash_measurement_file_bytes(source_root/entry["database"])!=entry["database_sha256"]:
            raise ValueError("frozen prepared source changed")
    output_root.mkdir(parents=True,exist_ok=False)
    manifest=dict(schema="boolean-rank-direct-v1",platform=platform.platform(),python=sys.version,
        prior_receipt=str(prior_path),prior_receipt_sha256=hash_measurement_file_bytes(prior_path),
        reused_preparations=prior["preparations"],queries=[],brackets=[],
        source_hashes={name:hash_measurement_file_bytes(script.parent/name) for name in files},
        design="three CG/direct/CG brackets per case; stationary all ten cases; clique only two stars",
        control_policy="factor-generic at 0.85; factor-native at near one",
        stability_screen="control max/min <=1.5, all observations retained",
        scope="same frozen native sources; prep reused and separately reported; observed RSS; no cold cache or physical cap")
    with (output_root/"events.jsonl").open("x") as events:
        def launch_direct_measurement_worker(case,mode,trial):
            started=time.perf_counter()
            process=subprocess.run([sys.executable,"-B",str(script),"--worker",
                "--source-directory",str(source_root),"--directory",str(output_root),
                "--case-json",json.dumps(case),"--mode",mode,"--trial",str(trial)],
                capture_output=True,text=True,check=True)
            result=json.loads(process.stdout)
            result["process_wall_seconds"]=time.perf_counter()-started
            events.write(json.dumps(result,sort_keys=True)+"\n")
            events.flush()
            manifest["queries"].append(result)
            return result

        for prepared in prior["preparations"]:
            case=prepared["case"]
            control="factor-generic" if case["alpha"]==.85 else "factor-native"
            candidates=("stationary","clique-direct") if case["kind"]=="star" else ("stationary",)
            for candidate_mode in candidates:
                for repeat in range(3):
                    trial=len(manifest["queries"])
                    first,candidate,last=[launch_direct_measurement_worker(case,mode,trial+offset)
                        for offset,mode in enumerate((control,candidate_mode,control))]
                    bracket=dict(case=case["name"],candidate=candidate_mode,control=control,repeat=repeat,
                        trials=[first["trial"],candidate["trial"],last["trial"]],
                        **summarize_native_control_bracket(first,candidate,last))
                    if bracket["all_accepted"]:
                        for label,other in (("before",first),("after",last)):
                            bound=sum(Fraction(q["pipeline"]["certificate"]["l1_error_upper"]) for q in (candidate,other))
                            bracket["audit_"+label]=audit_boolean_output_pair(
                                output_root/candidate["output"],output_root/other["output"],bound)
                    manifest["brackets"].append(bracket)
            queries=[q for q in manifest["queries"] if q["case"]==case["name"]]
            print(f"{case['name']}: {sum(q['accepted'] for q in queries)}/{len(queries)} admitted",flush=True)
    for name,expected in manifest["source_hashes"].items():
        if hash_measurement_file_bytes(script.parent/name)!=expected:
            raise AssertionError("implementation changed during direct study")
    for entry in manifest["reused_preparations"]:
        if hash_measurement_file_bytes(source_root/entry["database"])!=entry["database_sha256"]:
            raise AssertionError("prepared source changed during direct study")
    (output_root/"receipt.json").write_text(json.dumps(manifest,indent=2,sort_keys=True)+"\n")
    print(f"Saved {len(manifest['queries'])} attempts and {len(manifest['brackets'])} brackets",flush=True)


def parse_direct_measurement_arguments():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-directory",type=Path,required=True)
    parser.add_argument("--directory",type=Path,required=True)
    parser.add_argument("--worker",action="store_true")
    parser.add_argument("--case-json")
    parser.add_argument("--mode",choices=("stationary","clique-direct","factor-generic","factor-native"))
    parser.add_argument("--trial",type=int,default=0)
    return parser.parse_args()


if __name__=="__main__":
    args=parse_direct_measurement_arguments()
    if args.worker:
        print(json.dumps(run_direct_measurement_query(args.source_directory,args.directory,
            json.loads(args.case_json),args.mode,args.trial),sort_keys=True))
    else:
        execute_direct_measurement_study(args.source_directory.resolve(),args.directory.resolve())
