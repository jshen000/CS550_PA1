"""Time searches plus actual downloads; register completed files after timing."""
import contextlib
import csv
import io
import json
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path
from config import parse_config_args
from download import download_file
from register import index_request, register_batch


def options(p):
    p.add_argument("--target-peer", required=True)
    p.add_argument("--files", type=int, default=10000)
    p.add_argument("--first", type=int, default=0)
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--label", required=True)
    p.add_argument("--start-at", type=float)
    p.add_argument("--output-dir", default="results")


def main():
    a = parse_config_args("peer", options)
    if min(a.files, a.repeats) < 1 or a.first < 0:
        raise ValueError("Invalid count.")
    if a.target_peer == a.peer_id:
        raise ValueError("Choose the other peer.")
    for value in (a.target_peer, a.peer_id, a.label):
        if not value or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for c in value):
            raise ValueError("Invalid ID or label.")
    if a.start_at is not None and a.repeats != 1:
        raise ValueError("Use --repeats 1 for synchronized trials.")
    names = [f"bench_{a.target_peer}_1k_{i:07d}.bin"
             for i in range(a.first, a.first + a.files)]
    out = Path(a.output_dir).resolve()
    out.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
    base = out / f"{a.label}_{a.peer_id}_{stamp}"
    Path(str(base) + "_metadata.json").write_text(json.dumps(
        dict(vars(a), expected_bytes=1024, warmup=0, fsync=False,
             timing="search + download + size check; registration excluded",
             cache_policy="OS-managed, no cache clearing"),
        default=str, indent=2) + "\n")

    def lookup(name):
        reply = index_request(a, {"action": "search", "peer_id": a.peer_id, "file_name": name})
        for peer in reply["peers"]:
            if peer["peer_id"] == a.target_peer:
                return peer
        raise ValueError(f"Requested source missing for {name}")

    lookup(names[0])
    if a.start_at is not None:
        if time.time() >= a.start_at:
            raise ValueError("Start time has passed.")
        print(f"Ready; waiting until Unix time {a.start_at}", flush=True)
        while time.time() < a.start_at:
            time.sleep(min(0.1, max(0, a.start_at - time.time())))
    durations = []
    failed = False
    raw_path = Path(str(base) + "_requests.csv")
    summary_path = Path(str(base) + "_summary.csv")
    with raw_path.open("x", newline="") as raw, summary_path.open("x", newline="") as summary, Path(str(base) + "_output.txt").open("x") as log:
        rw, sw = csv.writer(raw), csv.writer(summary)
        rw.writerow(["repeat", "request", "file_name", "latency_ms", "success", "error"])
        sw.writerow(["label", "peer_id", "repeat", "files", "successes", "failures",
                     "start_utc", "end_utc", "elapsed_s", "mean_success_latency_ms",
                     "sample_stddev_success_latency_ms", "registration_failures"])
        for repeat in range(1, a.repeats + 1):
            rows, good, latencies = [], [], []
            captured = io.StringIO()
            start_utc = datetime.now(timezone.utc).isoformat()
            start = time.perf_counter()
            for i, name in enumerate(names, 1):
                tick = time.perf_counter()
                error = ""
                try:
                    peer = lookup(name)
                    with contextlib.redirect_stdout(captured):
                        ok = download_file(name, peer["host"], peer["port"], a.shared_dir)
                    if not ok:
                        raise OSError("Download failed; see output log.")
                    if (Path(a.shared_dir) / name).stat().st_size != 1024:
                        raise ValueError("Expected exactly 1024 bytes.")
                except (OSError, ValueError, KeyError, TypeError) as exc:
                    error = str(exc)
                ms = (time.perf_counter() - tick) * 1000
                if not error:
                    good.append(name)
                    latencies.append(ms)
                rows.append([repeat, i, name, ms, int(not error), error])
            elapsed = time.perf_counter() - start
            end_utc = datetime.now(timezone.utc).isoformat()
            durations.append(elapsed)
            rw.writerows(rows)
            raw.flush()
            log.write(f"Trial {repeat}\n" + captured.getvalue())
            log.flush()
            # Registration is outside the measured search-and-transfer interval.
            reg_failed = 0
            for offset in range(0, len(good), 1000):
                batch = good[offset:offset + 1000]
                try:
                    register_batch(a, batch)
                except (OSError, ValueError, KeyError, TypeError) as exc:
                    reg_failed += len(batch)
                    log.write(f"Registration failed: {exc}\n")
            failures = a.files - len(good)
            sw.writerow([a.label, a.peer_id, repeat, a.files, len(good), failures,
                         start_utc, end_utc, elapsed,
                         statistics.mean(latencies) if latencies else "",
                         statistics.stdev(latencies) if len(latencies) > 1 else "",
                         reg_failed])
            summary.flush()
            print(f"Run {repeat}: {elapsed:.3f}s; successes={len(good)}; failures={failures}; registration_failures={reg_failed}", flush=True)
            if failures or reg_failed:
                failed = True
                break
    print(f"Mean run time: {statistics.mean(durations):.3f}s")
    if len(durations) > 1:
        print(f"Sample standard deviation of run time: {statistics.stdev(durations):.3f}s")
    print(f"Summary: {summary_path}")
    print(f"Raw requests: {raw_path}")
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"Benchmark failed: {error}")
        raise SystemExit(1)

