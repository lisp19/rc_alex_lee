#!/usr/bin/env python3
"""Run the committed case design exclusively against the deployed real system."""
import argparse
from collections import Counter
import copy
import datetime
import itertools
import json
import subprocess
import time
import traceback

from cases import CASES, L8, L9
from harness import Harness, ROOT


def validate_design():
    assert len({c["id"] for c in CASES})==len(CASES)==51
    coverage={}
    for name,rows,levels,multiplicity in (("O1",L9,3,1),("O2",L8,2,2)):
        details=[]
        for a,b in itertools.combinations(range(4),2):
            counts=Counter((row[a],row[b]) for row in rows)
            assert len(counts)==levels**2 and set(counts.values())=={multiplicity}
            details.append({"columns":[a,b],"combinations":len(counts),"occurrences_each":multiplicity})
        coverage[name]=details
    return coverage


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list",action="store_true",help="list case definitions only; do not touch deployment")
    parser.add_argument("--priority",choices=["P0","P1","P2"])
    parser.add_argument("--cases",help="comma-separated IDs or prefixes, e.g. P0-02,P2-01,O1-")
    parser.add_argument("--fail-fast",action="store_true")
    args=parser.parse_args()
    coverage=validate_design()
    selected=[c for c in CASES if (not args.priority or c["priority"]==args.priority) and (not args.cases or any(c["id"].startswith(v) for v in args.cases.split(",")))]
    selected.sort(key=lambda c:c["priority"])
    if not selected:parser.error("no matching cases")
    if args.list:
        print(json.dumps({"cases":[{k:v for k,v in c.items() if k!="run"} for c in selected],"priority_counts":dict(Counter(c["priority"] for c in selected)),"orthogonal_coverage":coverage},indent=2))
        return 0
    h=Harness()
    report={"run_id":h.run_id,"started_at":datetime.datetime.now(datetime.timezone.utc).isoformat(),"case_commit":subprocess.check_output(["git","rev-parse","HEAD"],cwd=ROOT,text=True).strip(),"working_tree_dirty":bool(subprocess.check_output(["git","status","--porcelain"],cwd=ROOT,text=True)),"selected":[c["id"] for c in selected],"orthogonal_coverage":coverage,"results":h.results,"environment_restored":False}
    suite_failed=False
    print("Runtime evidence:",h.directory,flush=True)
    try:
        h.save("original-management.json",h.original)
        (h.directory/"original-process.json").write_bytes(h.original_process)
        h.setup()
        for c in selected:
            h.case_name=c["id"];start=time.monotonic()
            result={k:v for k,v in c.items() if k!="run"}
            before=copy.deepcopy(h.document)
            overlay=copy.deepcopy(h.overlay_data)
            try:
                evidence=c["run"](h)
                result.update(status="PASS",evidence=evidence)
            except Exception as err:
                suite_failed=True
                result.update(status="FAIL",error=str(err).replace(h.secret,"[REDACTED]")[:3000],traceback=traceback.format_exc().replace(h.secret,"[REDACTED]")[-8000:])
                # Cases own their ordinary teardown. This recovery path is for
                # a failed assertion, not a way to manufacture a PASS outcome.
                try:
                    for service in ("mariadb","redis","rabbitmq","notifier","notifier-2"):h.start(service)
                    h.overlay_data=overlay;h.overlay_up("notifier","notifier-2");h.healthy();h.config(before)
                except Exception as restore_error:
                    result["recovery_error"]=str(restore_error)[:1000]
            result["seconds"]=round(time.monotonic()-start,3)
            h.results.append(result);h.save("results.json",report)
            print(result["status"],c["id"],c["title"],f"({result['seconds']}s)",flush=True)
            if result["status"]=="FAIL":print(result["error"][:800],flush=True)
            if args.fail_fast and result["status"]=="FAIL":break
        try:h.capture()
        except Exception as err:report["capture_error"]=str(err)[:1000]
    except Exception as err:
        suite_failed=True;report["setup_or_runner_error"]=str(err)[:3000];report["traceback"]=traceback.format_exc()[-8000:]
        print("RUNNER ERROR:",str(err)[:1000],flush=True)
    finally:
        try:
            h.cleanup();report["environment_restored"]=True
        except Exception as err:
            suite_failed=True;report["cleanup_error"]=str(err)[:2000]
            print("CLEANUP ERROR:",str(err)[:1000],flush=True)
        report["finished_at"]=datetime.datetime.now(datetime.timezone.utc).isoformat()
        report["counts"]=dict(Counter(r["status"] for r in h.results))
        report["unexecuted"]=sorted(set(report["selected"])-{r["id"] for r in h.results})
        report["passed"]=not suite_failed and not report["unexecuted"]
        h.save("results.json",report)
        print(json.dumps({k:report[k] for k in ("run_id","counts","unexecuted","environment_restored","passed")}),flush=True)
    return 0 if report["passed"] else 1


if __name__=="__main__":
    raise SystemExit(main())
