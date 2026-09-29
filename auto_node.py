"""Worker for run_all_tests.py; standard library and existing P2P modules only."""
import base64
import contextlib
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import random
import socket
import sys
import tarfile
import tempfile
import time
from datetime import datetime, timezone

from download import download_file
from register import send_request

DATA = {"1k": (1000000, 1024), "1m": (1000, 1024**2), "1g": (10, 1024**3)}


def utc():
    return datetime.now(timezone.utc).isoformat()


def config(path):
    p = Path(path).resolve()
    c = json.loads(p.read_text())
    peer, index = c["peer"], c["index"]
    shared = (p.parent / peer["shared_dir"]).resolve()
    shared.mkdir(parents=True, exist_ok=True)
    return peer, index, shared


def request(index, payload):
    return send_request(index["host"], index["port"], payload)


def name(peer, tag, number):
    return f"bench_{peer}_{tag}_{number:07d}.bin"


def register(peer, index, files):
    return request(index, {"action": "registry", "peer_id": peer["id"],
                          "host": peer["host"], "port": peer["port"], "files": files})


def sources(peer, index, filename):
    return request(index, {"action": "search", "peer_id": peer["id"],
                          "file_name": filename})["peers"]


def prepare(peer, shared, sizes, check_only):
    """Check existing generator targets, capacity, and optionally create missing files."""
    missing = {tag: [] for tag in sizes}
    for tag in sizes:
        count, size = DATA[tag]
        for i in range(count):
            p = shared / name(peer["id"], tag, i)
            if p.is_symlink():
                raise ValueError(f"Refusing symlink: {p}")
            if p.exists():
                if not p.is_file() or p.stat().st_size != size:
                    raise ValueError(f"Unexpected existing size/type: {p}")
            else:
                missing[tag].append(i)
    st = os.statvfs(shared)
    unit = st.f_frsize or st.f_bsize
    missing_bytes = sum(len(v) * ((DATA[k][1] + unit - 1)//unit*unit) for k,v in missing.items())
    # Reserve space for every source's full benchmark download set, plus replacement temp file.
    incoming = sum({"1k": 10000, "1m": 1000, "1g": 8}[k] * DATA[k][1] for k in sizes)
    reserve = max(DATA[k][1] for k in sizes) + 1024**3
    need = missing_bytes + incoming + reserve
    free = st.f_bavail * unit
    need_inodes = sum(map(len, missing.values())) + sum({"1k":10000,"1m":1000,"1g":8}[k] for k in sizes) + 10000
    report = {"peer": peer["id"], "free_gib": free/1024**3,
              "required_free_gib_conservative": need/1024**3,
              "free_inodes": st.f_favail, "required_inodes_conservative":need_inodes,
              "missing": {k:len(v) for k,v in missing.items()},
              "enough": free >= need and (not st.f_files or st.f_favail >= need_inodes)}
    if check_only:
        return report
    if not report["enough"]:
        raise ValueError("Insufficient capacity: " + json.dumps(report))
    for tag, numbers in missing.items():
        size = DATA[tag][1]
        for i in numbers:
            p = shared / name(peer["id"],tag,i)
            seed = f"{peer['id']}:{i}".encode()
            if tag in ("1k","1m"):
                block = (hashlib.sha256(seed).hexdigest()[:63] + "\n").encode()*16384
            else:
                block = hashlib.shake_256(seed + b":large").digest(1024**2)
            tmp = None
            try:
                with tempfile.NamedTemporaryFile(dir=shared,prefix=".auto-",suffix=".part",delete=False) as f:
                    tmp=Path(f.name)
                    left=size
                    while left:
                        part=block[:min(left,len(block))]
                        f.write(part);left-=len(part)
                tmp.replace(p);tmp=None
            finally:
                if tmp is not None:tmp.unlink(missing_ok=True)
    # Sample text validation for pre-existing small/medium files.
    for tag in ("1k","1m"):
        if tag in sizes:
            for i in (0, DATA[tag][0]//2, DATA[tag][0]-1):
                (shared/name(peer["id"],tag,i)).read_bytes().decode("ascii")
    return report


def main(job):
    peer,index,shared=config(job["config"])
    mode=job["mode"]
    if mode=="ping":
        index_status=request(index,{"action":"list_peers"})
        # A listening peer service is required, not merely an index.
        with socket.create_connection((peer["host"],peer["port"]),timeout=5):pass
        return {"peer":peer,"index":index,"utc":utc(),"epoch":time.time(),"shared":str(shared),"reported_replication_factor":index_status.get("replication_factor")}
    if mode in ("capacity","prepare"):
        return prepare(peer,shared,job["sizes"],mode=="capacity")
    if mode=="register":
        batch=[];count=0
        with os.scandir(shared) as entries:
            for entry in entries:
                if entry.is_file(follow_symlinks=False) and not entry.name.endswith(".part"):
                    batch.append(entry.name)
                    if len(batch)==1000:
                        register(peer,index,batch);count+=len(batch);batch=[]
        if batch:register(peer,index,batch);count+=len(batch)
        return {"registered_files":count,"peer":peer["id"],"replication_requested":False}
    run_dir=Path("results")/job["run_id"]
    run_dir.mkdir(parents=True,exist_ok=True)
    if mode=="collect":
        buffer=io.BytesIO()
        with tarfile.open(fileobj=buffer,mode="w:gz") as tar:
            tar.add(run_dir,arcname=run_dir.name)
        return {"archive_base64":base64.b64encode(buffer.getvalue()).decode()}
    tag=job.get("tag","1k")
    target=job["target"]
    count=job["count"]
    seed=job.get("seed",550)
    if mode=="search":
        rng=random.Random(seed)
        indices=[rng.randrange(DATA["1k"][0]) for _ in range(count)]
    elif mode=="transfer":
        indices=list(range(job.get("first",0),job.get("first",0)+count))
    else:raise ValueError("Unknown worker mode")
    filenames=[name(target,tag,i) for i in indices]
    # Preflight lookups are outside timing; there is no full-file warmup.
    for filename in (filenames[0],filenames[-1]):
        if not any(p["peer_id"]==target for p in sources(peer,index,filename)):
            raise ValueError(f"Expected owner missing: {filename}")
    if mode=="search":
        for filename in filenames[:100]:sources(peer,index,filename)
    print(json.dumps({"ready":True,"epoch":time.time()}),flush=True)
    if sys.stdin.readline().strip()!="GO":
        raise ValueError("Coordinator did not release this trial.")
    rows=[];good=[];latencies=[];captured=io.StringIO()
    start_utc=utc();started=time.perf_counter()
    for seq,filename in enumerate(filenames,1):
        tick=time.perf_counter();error=""
        try:
            matches=sources(peer,index,filename)
            source=next((p for p in matches if p["peer_id"]==target),None)
            if source is None:raise ValueError("Expected source missing")
            if mode=="transfer":
                with contextlib.redirect_stdout(captured):
                    ok=download_file(filename,source["host"],source["port"],shared)
                if not ok:raise OSError("Download failed; see output log")
                if (shared/filename).stat().st_size!=DATA[tag][1]:
                    raise ValueError("Incorrect downloaded size")
        except (OSError,ValueError,KeyError,TypeError) as e:error=str(e)
        ms=(time.perf_counter()-tick)*1000
        rows.append([seq,filename,ms,int(not error),error])
        if not error:good.append(filename);latencies.append(ms)
    elapsed=time.perf_counter()-started;end_utc=utc()
    label=job["label"]
    with (run_dir/f"{label}_{peer['id']}_requests.csv").open("w",newline="") as f:
        w=csv.writer(f);w.writerow(["request","file_name","latency_ms","success","error"]);w.writerows(rows)
    (run_dir/f"{label}_{peer['id']}_output.txt").write_text(captured.getvalue())
    result={"label":label,"mode":mode,"size":tag,"peer":peer["id"],"requests":count,
            "successes":len(good),"failures":count-len(good),"elapsed_s":elapsed,
            "start_utc":start_utc,"end_utc":end_utc,
            "mean_latency_ms":sum(latencies)/len(latencies) if latencies else None}
    # No registration here: the faster peer must not disturb the slower timed peer.
    (run_dir/f"{label}_{peer['id']}_summary.json").write_text(json.dumps(result,indent=2))
    return result


if __name__=="__main__":
    try:
        job=json.loads(sys.argv[1])
        print(json.dumps({"result":main(job)}),flush=True)
    except Exception as e:
        print(json.dumps({"error":str(e)}),flush=True)
        raise SystemExit(1)
