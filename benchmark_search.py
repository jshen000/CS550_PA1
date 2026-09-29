"""Measure sequential TCP search requests against the configured index."""
import csv
import json
import random
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path

from config import parse_config_args
from register import index_request


def options(parser):
    parser.add_argument("--target-peer", required=True)
    parser.add_argument("--requests", type=int, default=10000)
    parser.add_argument("--dataset-count", type=int, default=1000000)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=100)
    parser.add_argument("--seed", type=int, default=550)
    parser.add_argument("--label", required=True)
    parser.add_argument("--output-dir", default="results")
    parser.add_argument("--start-at", type=float,
                        help="Optional future Unix timestamp for a coordinated first run.")


def main():
    args = parse_config_args("peer", options)
    if min(args.requests, args.dataset_count, args.repeats) < 1 or args.warmup < 0:
        raise ValueError("Counts must be positive and warmup must be nonnegative.")
    if args.target_peer == args.peer_id:
        raise ValueError("Choose the other peer because searches exclude the requester.")
    for value in (args.label, args.peer_id, args.target_peer):
        if not value or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for c in value):
            raise ValueError("Labels and peer IDs must use letters, digits, underscores or hyphens.")
    root = Path(args.output_dir).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
    prefix = root / f"{args.label}_{args.peer_id}_{stamp}"
    raw_path = Path(str(prefix) + "_requests.csv")
    summary_path = Path(str(prefix) + "_summary.csv")
    metadata_path = Path(str(prefix) + "_metadata.json")
    metadata_path.write_text(json.dumps(vars(args), default=str, indent=2) + "\n")
    rng = random.Random(args.seed)

    def query(number):
        name = f"bench_{args.target_peer}_1k_{number:07d}.bin"
        response = index_request(args, {
            "action": "search", "peer_id": args.peer_id, "file_name": name,
        })
        if not any(p["peer_id"] == args.target_peer for p in response["peers"]):
            raise ValueError(f"Expected owner missing: {name}")
        return name

    print(f"Warmup: {args.warmup} requests", flush=True)
    for _ in range(args.warmup):
        query(rng.randrange(args.dataset_count))
    if args.start_at is not None:
        if time.time() >= args.start_at:
            raise ValueError("Scheduled start has passed. Choose a later timestamp.")
        print(f"Ready; waiting until Unix time {args.start_at}", flush=True)
        while time.time() < args.start_at:
            time.sleep(min(0.1, max(0, args.start_at - time.time())))

    elapsed_runs = []
    total_failures = 0
    fields = ["label", "peer_id", "repeat", "requests", "successes", "failures",
              "start_utc", "end_utc", "elapsed_s", "successful_requests_per_s",
              "mean_success_latency_ms", "sample_stddev_success_latency_ms"]
    with raw_path.open("x", newline="") as raw, summary_path.open("x", newline="") as summary:
        raw_writer = csv.writer(raw)
        raw_writer.writerow(["repeat", "request", "file_name", "latency_ms", "success", "error"])
        summary_writer = csv.DictWriter(summary, fieldnames=fields)
        summary_writer.writeheader()
        for repeat in range(1, args.repeats + 1):
            # Prepare inputs and defer CSV writes until after the measured interval.
            numbers = [rng.randrange(args.dataset_count) for _ in range(args.requests)]
            rows, latencies = [], []
            started_utc = datetime.now(timezone.utc).isoformat()
            started = time.perf_counter()
            for i, number in enumerate(numbers, 1):
                name = f"bench_{args.target_peer}_1k_{number:07d}.bin"
                tick = time.perf_counter()
                error = ""
                try:
                    query(number)
                except (OSError, ValueError, KeyError, TypeError) as exc:
                    error = str(exc)
                latency = (time.perf_counter() - tick) * 1000
                if not error:
                    latencies.append(latency)
                rows.append((repeat, i, name, latency, int(not error), error))
            elapsed = time.perf_counter() - started
            ended_utc = datetime.now(timezone.utc).isoformat()
            failures = args.requests - len(latencies)
            total_failures += failures
            elapsed_runs.append(elapsed)
            summary_writer.writerow({
                "label": args.label, "peer_id": args.peer_id, "repeat": repeat,
                "requests": args.requests, "successes": len(latencies), "failures": failures,
                "start_utc": started_utc, "end_utc": ended_utc, "elapsed_s": elapsed,
                "successful_requests_per_s": len(latencies) / elapsed,
                "mean_success_latency_ms": statistics.mean(latencies) if latencies else "",
                "sample_stddev_success_latency_ms": statistics.stdev(latencies) if len(latencies) > 1 else "",
            })
            raw_writer.writerows(rows)
            raw.flush()
            summary.flush()
            print(f"Run {repeat}: {elapsed:.3f}s; successes={len(latencies)}; failures={failures}", flush=True)
    print(f"Mean run time: {statistics.mean(elapsed_runs):.3f}s", flush=True)
    if len(elapsed_runs) > 1:
        print(f"Sample standard deviation of run time: {statistics.stdev(elapsed_runs):.3f}s", flush=True)
    print(f"Summary: {summary_path}")
    print(f"Raw requests: {raw_path}")
    print(f"Metadata: {metadata_path}")
    if total_failures:
        raise SystemExit(1)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"Benchmark failed: {error}")
        raise SystemExit(1)

