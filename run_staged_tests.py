"""Reduced sequential medium/large experiment suite. Run on VM1."""
import argparse
import base64
import csv
import json
from pathlib import Path
import shlex
import statistics
import subprocess
import sys
import tarfile
import time
from datetime import datetime,timezone


NODE_SOURCE = '"""Worker for run_all_tests.py; standard library and existing P2P modules only."""\nimport base64\nimport contextlib\nimport csv\nimport hashlib\nimport io\nimport json\nimport os\nfrom pathlib import Path\nimport random\nimport socket\nimport sys\nimport tarfile\nimport tempfile\nimport time\nfrom datetime import datetime, timezone\n\nfrom download import download_file\nfrom register import send_request\n\nDATA = {"1m": (800, 1024**2), "1g": (8, 1024**3)}\nPREFIX = ""\n\n\ndef utc():\n    return datetime.now(timezone.utc).isoformat()\n\n\ndef config(path):\n    p = Path(path).resolve()\n    c = json.loads(p.read_text())\n    peer, index = c["peer"], c["index"]\n    shared = (p.parent / peer["shared_dir"]).resolve()\n    shared.mkdir(parents=True, exist_ok=True)\n    return peer, index, shared\n\n\ndef request(index, payload):\n    return send_request(index["host"], index["port"], payload)\n\n\ndef name(peer, tag, number):\n    return f"{PREFIX}_{peer}_{tag}_{number:07d}.bin"\n\n\ndef register(peer, index, files):\n    return request(index, {"action": "registry", "peer_id": peer["id"],\n                          "host": peer["host"], "port": peer["port"], "files": files})\n\n\ndef sources(peer, index, filename):\n    return request(index, {"action": "search", "peer_id": peer["id"],\n                          "file_name": filename})["peers"]\n\n\ndef prepare(peer, shared, sizes, check_only):\n    """Check existing generator targets, capacity, and optionally create missing files."""\n    missing = {tag: [] for tag in sizes}\n    for tag in sizes:\n        count, size = DATA[tag]\n        for i in range(count):\n            p = shared / name(peer["id"], tag, i)\n            if p.is_symlink():\n                raise ValueError(f"Refusing symlink: {p}")\n            if p.exists():\n                if not p.is_file() or p.stat().st_size != size:\n                    raise ValueError(f"Unexpected existing size/type: {p}")\n            else:\n                missing[tag].append(i)\n    st = os.statvfs(shared)\n    unit = st.f_frsize or st.f_bsize\n    missing_bytes = sum(len(v) * ((DATA[k][1] + unit - 1)//unit*unit) for k,v in missing.items())\n    # Reserve space for every source\'s full benchmark download set, plus replacement temp file.\n    incoming = 0\n    reserve = max(DATA[k][1] for k in sizes) + 1024**3\n    need = missing_bytes + incoming + reserve\n    free = st.f_bavail * unit\n    need_inodes = sum(map(len, missing.values())) + 1 + 10000\n    report = {"peer": peer["id"], "free_gib": free/1024**3,\n              "required_free_gib_conservative": need/1024**3,\n              "free_inodes": st.f_favail, "required_inodes_conservative":need_inodes,\n              "missing": {k:len(v) for k,v in missing.items()},\n              "enough": free >= need and (not st.f_files or st.f_favail >= need_inodes)}\n    if check_only:\n        return report\n    if not report["enough"]:\n        raise ValueError("Insufficient capacity: " + json.dumps(report))\n    for tag, numbers in missing.items():\n        size = DATA[tag][1]\n        for i in numbers:\n            p = shared / name(peer["id"],tag,i)\n            seed = f"{peer[\'id\']}:{i}".encode()\n            if tag in ("1k","1m"):\n                block = (hashlib.sha256(seed).hexdigest()[:63] + "\\n").encode()*16384\n            else:\n                block = hashlib.shake_256(seed + b":large").digest(1024**2)\n            tmp = None\n            try:\n                with tempfile.NamedTemporaryFile(dir=shared,prefix=".auto-",suffix=".part",delete=False) as f:\n                    tmp=Path(f.name)\n                    left=size\n                    while left:\n                        part=block[:min(left,len(block))]\n                        f.write(part);left-=len(part)\n                tmp.replace(p);tmp=None\n            finally:\n                if tmp is not None:tmp.unlink(missing_ok=True)\n    # Sample text validation for pre-existing small/medium files.\n    for tag in ("1k","1m"):\n        if tag in sizes:\n            for i in (0, DATA[tag][0]//2, DATA[tag][0]-1):\n                (shared/name(peer["id"],tag,i)).read_bytes().decode("ascii")\n    return report\n\n\ndef main(job):\n    global PREFIX\n    PREFIX = job["run_id"]\n    peer,index,shared=config(job["config"])\n    mode=job["mode"]\n    if mode=="ping":\n        index_status=request(index,{"action":"list_peers"})\n        # A listening peer service is required, not merely an index.\n        with socket.create_connection((peer["host"],peer["port"]),timeout=5):pass\n        return {"peer":peer,"index":index,"utc":utc(),"epoch":time.time(),"shared":str(shared),"reported_replication_factor":index_status.get("replication_factor"),"host_share_free_gib": (os.statvfs("/media/sf_Downloads").f_bavail*os.statvfs("/media/sf_Downloads").f_frsize/1024**3) if Path("/media/sf_Downloads").is_dir() else None}\n    if mode in ("capacity","prepare"):\n        return prepare(peer,shared,job["sizes"],mode=="capacity")\n    if mode=="cleanup":\n        removed = 0\n        for owner in job["owners"]:\n            for i in range(DATA[job["tag"]][0]):\n                target = shared/name(owner,job["tag"],i)\n                if target.is_symlink():\n                    raise ValueError(f"Refusing cleanup symlink: {target}")\n                if target.is_file():\n                    target.unlink();removed += 1\n        return {"removed_stage_files":removed}\n    if mode=="register_stage":\n        files=[name(peer["id"],job["tag"],i) for i in range(DATA[job["tag"]][0])]\n        register(peer,index,files)\n        return {"registered":len(files)}\n    if mode=="register":\n        batch=[];count=0\n        with os.scandir(shared) as entries:\n            for entry in entries:\n                if entry.is_file(follow_symlinks=False) and not entry.name.endswith(".part"):\n                    batch.append(entry.name)\n                    if len(batch)==1000:\n                        register(peer,index,batch);count+=len(batch);batch=[]\n        if batch:register(peer,index,batch);count+=len(batch)\n        return {"registered_files":count,"peer":peer["id"],"replication_requested":False}\n    run_dir=Path("results")/job["run_id"]\n    run_dir.mkdir(parents=True,exist_ok=True)\n    if mode=="collect":\n        buffer=io.BytesIO()\n        with tarfile.open(fileobj=buffer,mode="w:gz") as tar:\n            tar.add(run_dir,arcname=run_dir.name)\n        return {"archive_base64":base64.b64encode(buffer.getvalue()).decode()}\n    tag=job["tag"]\n    target=job["target"]\n    count=job["count"]\n    seed=job.get("seed",550)\n    if mode=="search":\n        rng=random.Random(seed)\n        indices=[rng.randrange(DATA["1k"][0]) for _ in range(count)]\n    elif mode=="transfer":\n        indices=list(range(job.get("first",0),job.get("first",0)+count))\n    else:raise ValueError("Unknown worker mode")\n    filenames=[name(target,tag,i) for i in indices]\n    # Verify content against this worker\'s deterministic generator.\n    expected = {}\n    for i, filename in zip(indices, filenames):\n        seed=f"{target}:{i}".encode()\n        block=((hashlib.sha256(seed).hexdigest()[:63]+"\\n").encode()*16384\n               if tag=="1m" else hashlib.shake_256(seed+b":large").digest(1024**2))\n        h=hashlib.sha256()\n        remaining=DATA[tag][1]\n        while remaining:\n            part=block[:min(remaining,len(block))];h.update(part);remaining-=len(part)\n        expected[filename]=h.digest()\n    # Preflight lookups are outside timing; there is no full-file warmup.\n    for filename in (filenames[0],filenames[-1]):\n        if not any(p["peer_id"]==target for p in sources(peer,index,filename)):\n            raise ValueError(f"Expected owner missing: {filename}")\n    if mode=="search":\n        for filename in filenames[:100]:sources(peer,index,filename)\n    print(json.dumps({"ready":True,"epoch":time.time()}),flush=True)\n    if sys.stdin.readline().strip()!="GO":\n        raise ValueError("Coordinator did not release this trial.")\n    rows=[];good=[];latencies=[];captured=io.StringIO()\n    start_utc=utc();started=time.perf_counter()\n    for seq,filename in enumerate(filenames,1):\n        tick=time.perf_counter();error=""\n        try:\n            matches=sources(peer,index,filename)\n            source=next((p for p in matches if p["peer_id"]==target),None)\n            if source is None:raise ValueError("Expected source missing")\n            if mode=="transfer":\n                with contextlib.redirect_stdout(captured):\n                    ok=download_file(filename,source["host"],source["port"],shared)\n                if not ok:raise OSError("Download failed; see output log")\n                if (shared/filename).stat().st_size!=DATA[tag][1]:\n                    raise ValueError("Incorrect downloaded size")\n                received=shared/filename\n                h=hashlib.sha256()\n                with received.open("rb") as f:\n                    for chunk in iter(lambda:f.read(1024**2),b""):\n                        h.update(chunk)\n                if h.digest()!=expected[filename]:\n                    raise ValueError("SHA-256 verification failed")\n                received.unlink()\n        except (OSError,ValueError,KeyError,TypeError) as e:error=str(e)\n        ms=(time.perf_counter()-tick)*1000\n        rows.append([seq,filename,ms,int(not error),error])\n        if not error:good.append(filename);latencies.append(ms)\n    elapsed=time.perf_counter()-started;end_utc=utc()\n    label=job["label"]\n    with (run_dir/f"{label}_{peer[\'id\']}_requests.csv").open("w",newline="") as f:\n        w=csv.writer(f);w.writerow(["request","file_name","latency_ms","success","error"]);w.writerows(rows)\n    (run_dir/f"{label}_{peer[\'id\']}_output.txt").write_text(captured.getvalue())\n    result={"label":label,"mode":mode,"size":tag,"peer":peer["id"],"requests":count,\n            "successes":len(good),"failures":count-len(good),"elapsed_s":elapsed,\n            "start_utc":start_utc,"end_utc":end_utc,\n            "mean_latency_ms":sum(latencies)/len(latencies) if latencies else None,\n            "timing":"search+download+SHA256 verification+received-copy deletion; no registration",\n            "dataset_scope":"reduced, staged; not full assignment dataset"}\n    # No registration here: the faster peer must not disturb the slower timed peer.\n    (run_dir/f"{label}_{peer[\'id\']}_summary.json").write_text(json.dumps(result,indent=2))\n    return result\n\n\nif __name__=="__main__":\n    try:\n        job=json.loads(sys.argv[1])\n        print(json.dumps({"result":main(job)}),flush=True)\n    except Exception as e:\n        print(json.dumps({"error":str(e)}),flush=True)\n        raise SystemExit(1)\n'

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite",default="suite.json")
    parser.add_argument("--host-free-gib",type=float,
                        help="Measured free GiB on actual host volume holding both VM images.")
    args=parser.parse_args()
    settings=json.loads(Path(args.suite).read_text())
    local=Path(settings["local_project"]).expanduser().resolve()
    remote=settings["remote"]
    ssh=["ssh","-o","BatchMode=yes","-o","ConnectTimeout=10"]
    if remote.get("ssh_identity"):ssh+=["-i",str(Path(remote["ssh_identity"]).expanduser())]
    ssh+=["-p",str(remote.get("ssh_port",22)),f"{remote['ssh_user']}@{remote['ssh_host']}"]
    project=remote["project"]
    run_id=datetime.now(timezone.utc).strftime("staged_%Y%m%dT%H%M%S_%fZ")
    worker_name=f"worker_{run_id}.py"
    worker=local/worker_name
    worker.write_text(NODE_SOURCE)
    subprocess.run(ssh+["cd "+shlex.quote(project)+" && cat > "+shlex.quote(worker_name)],
                   input=NODE_SOURCE,text=True,check=True)
    dest=local/"results"/run_id;dest.mkdir(parents=True)
    records=[];trials=[];peers=[];host_consumed=0.0
    def launch(which,job):
        job=dict(job,config=settings.get("local_config","config.json") if which==0 else remote.get("config","config.json"),run_id=run_id)
        encoded=json.dumps(job)
        command=([sys.executable,str(worker),encoded] if which==0 else
                 ssh+["cd "+shlex.quote(project)+" && python3 "+shlex.quote(worker_name)+" "+shlex.quote(encoded)])
        return subprocess.Popen(command,cwd=local,stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True)
    def read(proc):
        line=proc.stdout.readline()
        if not line:raise RuntimeError("Worker exited unexpectedly.")
        msg=json.loads(line)
        if "error" in msg:raise RuntimeError(msg["error"])
        return msg
    def call(which,job):
        proc=launch(which,job)
        try:
            result=read(proc)["result"]
            proc.stdin.close()
            if proc.wait()!=0:raise RuntimeError("Worker failed")
            return result
        finally:
            if proc.poll() is None:proc.terminate()
    try:
        for i in range(2):
            start=time.time();info=call(i,{"mode":"ping"});end=time.time()
            if not start-.25 <= info["epoch"] <= end+.25:
                raise RuntimeError("Synchronize VM clocks before running.")
            peers.append(info)
        if peers[0]["peer"]["id"]==peers[1]["peer"]["id"]:
            raise RuntimeError("Both endpoints identify as the same peer.")
        if peers[0]["index"]!=peers[1]["index"]:
            raise RuntimeError("Peers use different index endpoints.")
        (dest/"environment.json").write_text(json.dumps(peers,indent=2))
        owners=[p["peer"]["id"] for p in peers]
        for tag,total in [("1m",800),("1g",8)]:
            print(f"=== Starting {tag}: {total} source files per peer ===",flush=True)
            caps=[call(i,{"mode":"capacity","sizes":[tag]}) for i in range(2)]
            (dest/f"{tag}_capacity.json").write_text(json.dumps(caps,indent=2))
            for cap in caps:print(json.dumps(cap),flush=True)
            if not all(c["enough"] for c in caps):
                raise RuntimeError(f"{tag}: insufficient guest disk/inodes. Earlier results preserved.")
            host_need=2*total*({"1m":1024**2,"1g":1024**3}[tag])/1024**3+2*({"1m":1024**2,"1g":1024**3}[tag])/1024**3+.5
            # Existing shared-host-volume free space is a conservative proxy, not proof
            # of where VM disk images reside. Supply actual host volume space if different.
            probes=[call(i,{"mode":"ping"}) for i in range(2)]
            shares=[p["host_share_free_gib"] for p in probes if p["host_share_free_gib"] is not None]
            host_free=min(shares) if shares else args.host_free_gib
            if args.host_free_gib is not None:
                # Explicit override is for VM images stored on a different host volume.
                host_free=args.host_free_gib-host_consumed
            if host_free is None or host_free<host_need:
                raise RuntimeError(f"{tag}: need approximately {host_need:.2f} GiB host headroom; observed/declared {host_free}. Free host space or use --host-free-gib with actual measured VM-image volume capacity. Earlier results preserved.")
            host_consumed += host_need - 0.5
            for i in range(2):
                print(f"Preparing {owners[i]} {tag}...",flush=True)
                call(i,{"mode":"prepare","sizes":[tag]})
                call(i,{"mode":"register_stage","tag":tag})
            for clients in (1,2):
                for repeat in range(1,4):
                    label=f"reduced_{tag}_{clients}peers_r{repeat}"
                    procs=[launch(i,{"mode":"transfer","tag":tag,"target":owners[1-i],
                                     "count":total//clients,"label":label}) for i in range(clients)]
                    results=[]
                    try:
                        for proc in procs:
                            if not read(proc).get("ready"):raise RuntimeError("Worker not ready")
                        for proc in procs:proc.stdin.write("GO\n");proc.stdin.flush()
                        for proc in procs:
                            results.append(read(proc)["result"])
                            proc.stdin.close()
                            if proc.wait()!=0:raise RuntimeError("Timed worker failed")
                    finally:
                        for proc in procs:
                            if proc.poll() is None:proc.terminate()
                    records.extend(results)
                    (dest/"all_results.json").write_text(json.dumps(records,indent=2))
                    if any(r["failures"] for r in results):raise RuntimeError("Failed requests; stopping to preserve evidence.")
                    starts=[datetime.fromisoformat(r["start_utc"]) for r in results]
                    ends=[datetime.fromisoformat(r["end_utc"]) for r in results]
                    if clients==2 and min(ends)<=max(starts):raise RuntimeError("No overlap in concurrent trial")
                    elapsed=(max(ends)-min(starts)).total_seconds()
                    trials.append({"size":tag,"clients":clients,"trial":repeat,"total_files":total,
                                   "elapsed_s":elapsed,"start_skew_s":(max(starts)-min(starts)).total_seconds()})
                    with (dest/"trials.csv").open("w",newline="") as f:
                        w=csv.DictWriter(f,fieldnames=list(trials[0]));w.writeheader();w.writerows(trials)
                    print(f"{label}: {elapsed:.3f}s, failures=0",flush=True)
            # Only exact unique run-prefixed filenames from this stage are removed.
            for i in range(2):
                result=call(i,{"mode":"cleanup","tag":tag,"owners":owners})
                print(f"Cleanup {owners[i]} {tag}: {result}",flush=True)
            (dest/f"{tag}_COMPLETE.txt").write_text("All six trials passed; stage files cleaned.\n")
        (dest/"COMPLETE.txt").write_text("Reduced staged tests complete; not full assignment compliance.\n")
    finally:
        if trials:
            with (dest/"aggregate.csv").open("w",newline="") as f:
                w=csv.writer(f);w.writerow(["size","clients","trials","mean_s","sample_sd_s"])
                for tag in ["1m","1g"]:
                    for clients in [1,2]:
                        vals=[r["elapsed_s"] for r in trials if r["size"]==tag and r["clients"]==clients]
                        if vals:w.writerow([tag,clients,len(vals),statistics.mean(vals),statistics.stdev(vals) if len(vals)>1 else ""])
        try:
            result=call(1,{"mode":"collect"})
            (dest/"vm2_raw_results.tar.gz").write_bytes(base64.b64decode(result["archive_base64"]))
        except Exception as e:print(f"Collection failed; VM2 retains logs: {e}")
        bundle=local/f"{run_id}.tar.gz"
        with tarfile.open(bundle,"w:gz") as tar:tar.add(dest,arcname=run_id)
        print(f"Results: {bundle}",flush=True)
        print("Deleted source files leave stale index records. Before further experiments, restart VM3 index and re-register surviving files with --no-replicate.",flush=True)


if __name__=="__main__":
    try:main()
    except (OSError,ValueError,KeyError,RuntimeError,subprocess.SubprocessError) as e:
        print(f"STOPPED: {e}",file=sys.stderr)
        raise SystemExit(1)
