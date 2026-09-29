"""Run on VM1; coordinate VM2 over existing passwordless SSH."""
import argparse
import base64
import csv
import json
from pathlib import Path
import shlex
import statistics
import subprocess
import sys
import time
from datetime import datetime,timezone


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--suite",default="suite.json")
    p.add_argument("--only-small",action="store_true",help="Only the remaining small-file transfer trials; not the full assignment.")
    p.add_argument("--check-only",action="store_true")
    p.add_argument("--host-free-gib",type=float,help="Current free GiB on the Mac volume storing both VM disks; required for full generation.")
    a=p.parse_args()
    settings=json.loads(Path(a.suite).read_text())
    local=Path(settings["local_project"]).expanduser().resolve()
    remote=settings["remote"]
    host=remote["ssh_host"];user=remote["ssh_user"];port=str(remote.get("ssh_port",22))
    project=remote["project"]
    ssh=["ssh","-o","BatchMode=yes","-o","ConnectTimeout=10","-p",port,f"{user}@{host}"]
    if remote.get("ssh_identity"):
        ssh[1:1]=["-i",str(Path(remote["ssh_identity"]).expanduser())]
    node=Path(__file__).resolve().with_name("auto_node.py")
    target_file=local/"auto_node.py"
    if node!=target_file:target_file.write_bytes(node.read_bytes())
    # Only the new automation worker is copied. Existing application/config files are preserved.
    subprocess.run(ssh+["cd "+shlex.quote(project)+" && cat > auto_node.py"],
                   input=node.read_bytes(),check=True)
    run_id=datetime.now(timezone.utc).strftime("auto_%Y%m%dT%H%M%S_%fZ")
    dest=local/"results"/run_id
    dest.mkdir(parents=True)
    peers=[];all_results=[];pair_rows=[]
    def launch(which,job):
        job=dict(job,config=settings.get("local_config","config.json") if which==0 else remote.get("config","config.json"),run_id=run_id)
        encoded=json.dumps(job)
        if which==0:
            cmd=[sys.executable,str(target_file),encoded]
        else:
            cmd=ssh+["cd "+shlex.quote(project)+" && python3 auto_node.py "+shlex.quote(encoded)]
        return subprocess.Popen(cmd,cwd=local,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=None,text=True)
    def read_message(proc):
        line=proc.stdout.readline()
        if not line:raise RuntimeError("Worker stopped unexpectedly; check SSH/service errors.")
        data=json.loads(line)
        if "error" in data:raise RuntimeError(data["error"])
        return data
    def call(which,job):
        proc=launch(which,job)
        try:
            data=read_message(proc)
            proc.stdin.close()
            if proc.wait()!=0:raise RuntimeError("Worker failed")
            return data["result"]
        finally:
            if proc.poll() is None:proc.terminate()
    sizes=["1k"] if a.only_small else ["1k","1m","1g"]
    for i in range(2):
        t0=time.time();info=call(i,{"mode":"ping"});t1=time.time()
        if not t0-0.25 <= info["epoch"] <= t1+0.25:
            raise RuntimeError("VM clocks disagree; synchronize clocks before timing.")
        peers.append(info)
        print(f"Connected: {info['peer']['id']} ({info['shared']})",flush=True)
    if peers[0]["peer"]["id"]==peers[1]["peer"]["id"]:
        raise RuntimeError("Both endpoints identify as the same peer; check suite/config files.")
    if peers[0]["index"]!=peers[1]["index"]:
        raise RuntimeError("Peer configurations use different index servers.")
    (dest/"environment.json").write_text(json.dumps(peers,indent=2))
    reports=[call(i,{"mode":"capacity","sizes":sizes}) for i in range(2)]
    (dest/"capacity.json").write_text(json.dumps(reports,indent=2))
    for r in reports:print(json.dumps(r,indent=2),flush=True)
    if a.check_only:
        print("Capacity check complete. No datasets or benchmarks were run.")
        return
    if a.only_small and any(r["missing"]["1k"] for r in reports):
        raise RuntimeError("Small-only mode requires the existing million-file datasets on both peers.")
    if not all(r["enough"] for r in reports):
        raise RuntimeError("Insufficient guest space/inodes. Expand storage, then rerun. No datasets generated.")
    host_need=sum(r["required_free_gib_conservative"] for r in reports)+5
    if not a.only_small and (a.host_free_gib is None or a.host_free_gib<host_need):
        raise RuntimeError(f"Full suite requires a conservative host reserve of {host_need:.1f} GiB. Check the Mac disk containing the VM images, then supply --host-free-gib. No datasets generated.")
    try:
        if not a.only_small:
            for i in range(2):
                print(f"Preparing all datasets on {peers[i]['peer']['id']}...",flush=True)
                call(i,{"mode":"prepare","sizes":sizes})
        for i in range(2):
            print(f"Registering {peers[i]['peer']['id']} (no replication)...",flush=True)
            result=call(i,{"mode":"register"})
            print(result,flush=True)
        cases=[] if a.only_small else [("search","1k",10000)]
        cases += [("transfer",tag,{"1k":10000,"1m":1000,"1g":8}[tag]) for tag in sizes]
        for mode,tag,total in cases:
            for clients in (1,2):
                # Small-only mode reuses the already completed one-peer pilot as baseline.
                if a.only_small and clients==1:continue
                for trial in range(1,4):
                    label=f"{mode}_{tag}_{clients}peers_r{trial}"
                    count=total if mode=="search" else total//clients
                    active=list(range(clients))
                    jobs=[{"mode":mode,"tag":tag,"count":count,
                           "target":peers[1-i]["peer"]["id"],"label":label,"seed":550+trial}
                          for i in active]
                    processes=[launch(i,job) for i,job in zip(active,jobs)]
                    results=[]
                    try:
                        for proc in processes:
                            if not read_message(proc).get("ready"):raise RuntimeError("Worker was not ready")
                        # Persistent SSH channels act as a barrier; no manual START_AT variables.
                        release=utc()
                        for proc in processes:proc.stdin.write("GO\n");proc.stdin.flush()
                        for proc in processes:
                            results.append(read_message(proc)["result"])
                            proc.stdin.close()
                            if proc.wait()!=0:raise RuntimeError("Benchmark worker failed")
                    finally:
                        for proc in processes:
                            if proc.poll() is None:proc.terminate()
                    all_results.extend(results)
                    (dest/"all_results.json").write_text(json.dumps(all_results,indent=2))
                    if any(r["failures"] for r in results):
                        raise RuntimeError(f"{label} has failed requests; stopped.")
                    starts=[datetime.fromisoformat(r["start_utc"]) for r in results]
                    ends=[datetime.fromisoformat(r["end_utc"]) for r in results]
                    if clients==2 and min(ends)<=max(starts):raise RuntimeError("Concurrent runs did not overlap.")
                    span=(max(ends)-min(starts)).total_seconds()
                    pair_rows.append({"case":f"{mode}_{tag}","clients":clients,"trial":trial,
                                      "elapsed_s":span,"start_skew_s":(max(starts)-min(starts)).total_seconds(),
                                      "requests":sum(r["requests"] for r in results),"release_utc":release})
                    with (dest/"trial_summary.csv").open("w",newline="") as f:
                        w=csv.DictWriter(f,fieldnames=list(pair_rows[0]));w.writeheader();w.writerows(pair_rows)
                    print(f"{label}: {span:.3f}s; failures=0; start skew={pair_rows[-1]['start_skew_s']:.3f}s",flush=True)
                    # Register only after both timed workers have finished.
                    if mode=="transfer":
                        for i in active:call(i,{"mode":"register"})
        aggregates=[]
        for case in sorted({r["case"] for r in pair_rows}):
            for clients in (1,2):
                values=[r["elapsed_s"] for r in pair_rows if r["case"]==case and r["clients"]==clients]
                if values:aggregates.append({"case":case,"clients":clients,"trials":len(values),
                                              "mean_s":statistics.mean(values),"sample_sd_s":statistics.stdev(values) if len(values)>1 else ""})
        with (dest/"aggregate.csv").open("w",newline="") as f:
            w=csv.DictWriter(f,fieldnames=["case","clients","trials","mean_s","sample_sd_s"]);w.writeheader();w.writerows(aggregates)
        (dest/"COMPLETE.txt").write_text("Completed selected experiment scope. See README for methodology and remaining submission tasks.\n")
        print("Selected tests completed.",flush=True)
    finally:
        try:
            payload=call(1,{"mode":"collect"})
            (dest/"vm2_raw_results.tar.gz").write_bytes(base64.b64decode(payload["archive_base64"]))
        except Exception as exc:print(f"Remote collection failed; VM2 files remain in results/{run_id}: {exc}")
        import tarfile
        bundle=local/f"{run_id}.tar.gz"
        with tarfile.open(bundle,"w:gz") as tar:tar.add(dest,arcname=run_id)
        print(f"Results archive: {bundle}",flush=True)


def utc():
    return datetime.now(timezone.utc).isoformat()


if __name__=="__main__":
    try:main()
    except (OSError,ValueError,KeyError,RuntimeError,subprocess.SubprocessError) as exc:
        print(f"STOPPED: {exc}",file=sys.stderr)
        raise SystemExit(1)
