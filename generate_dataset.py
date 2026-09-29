"""Generate deterministic ASCII text 1 KiB files in the configured peer shared directory."""
import hashlib
import os
import time
from pathlib import Path
from config import parse_config_args

FILE_SIZE = 1024


def options(parser):
    parser.add_argument("--count", type=int, required=True,
                        help="Target total, not the number of additional files.")
    parser.add_argument("--rewrite-existing", action="store_true",
                        help="Replace generated targets with deterministic ASCII text.")
    parser.add_argument("--progress-every", type=int, default=10000)


def main():
    args = parse_config_args("peer", options)
    if not 1 <= args.count <= 1_000_000:
        raise ValueError("--count must be between 1 and 1000000")
    if args.progress_every < 1:
        raise ValueError("--progress-every must be positive")
    peer_id = args.peer_id
    if any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for c in peer_id):
        raise ValueError("Peer ID must contain only letters, digits, underscores or hyphens.")
    directory = Path(args.shared_dir)
    directory.mkdir(parents=True, exist_ok=True)
    prefix = f"bench_{peer_id}_1k_"
    # Check existing targets without retaining a million names in memory.
    existing = 0
    for number in range(args.count):
        path = directory / f"{prefix}{number:07d}.bin"
        if path.is_symlink():
            raise ValueError(f"Refusing symbolic link: {path}")
        if path.exists():
            if not path.is_file() or path.stat().st_size != FILE_SIZE:
                raise ValueError(f"Existing target is not a {FILE_SIZE}-byte file: {path}")
            existing += 1

    needed = args.count - existing
    stats = os.statvfs(directory)
    unit = stats.f_frsize or stats.f_bsize
    estimated = needed * ((FILE_SIZE + unit - 1) // unit * unit)
    free_bytes = stats.f_bavail * unit
    if estimated + 512 * 1024**2 > free_bytes:
        raise ValueError("Insufficient space, including a 512 MiB reserve.")
    if stats.f_files and needed + 10000 > stats.f_favail:
        raise ValueError("Insufficient inodes, including a 10000-inode reserve.")

    print(f"Peer: {peer_id}", flush=True)
    print(f"Directory: {directory}", flush=True)
    print(f"Target: {args.count}; existing: {existing}; new: {needed}", flush=True)
    print("File size: 1024 bytes (1 KiB). No registration or replication.", flush=True)
    started = time.monotonic()
    created = rewritten = 0
    for number in range(args.count):
        path = directory / f"{prefix}{number:07d}.bin"
        existed = path.exists()
        if existed and not args.rewrite_existing:
            continue
        # Each file has deterministic, peer-specific, non-sparse content.
        seed = f"{peer_id}:{number}".encode("utf-8")
        line = hashlib.sha256(seed).hexdigest().encode("ascii")[:63] + b"\n"
        payload = line * (FILE_SIZE // len(line))
        if existed:
            # Atomically replace only this generator-owned target.
            import tempfile
            temporary = None
            try:
                with tempfile.NamedTemporaryFile(mode="wb", dir=directory,
                        prefix=".text-repair-", suffix=".part", delete=False) as output:
                    temporary = Path(output.name)
                    output.write(payload)
                temporary.replace(path)
                temporary = None
            finally:
                if temporary is not None:
                    temporary.unlink(missing_ok=True)
            rewritten += 1
            if rewritten % args.progress_every == 0:
                print(f"Rewritten as text: {rewritten}/{existing}", flush=True)
            continue
        opened = False
        try:
            with path.open("xb") as output:
                opened = True
                output.write(payload)
        except BaseException:
            if opened:
                path.unlink(missing_ok=True)
            raise
        created += 1
        if created % args.progress_every == 0:
            print(f"Created {created}/{needed} new files", flush=True)
    elapsed = time.monotonic() - started
    print(f"Complete: created={created}, rewritten={rewritten}, kept={existing-rewritten}, target={args.count}", flush=True)
    print(f"Generation elapsed: {elapsed:.2f} seconds (not a P2P benchmark).", flush=True)
    print("No files deleted. Existing targets change only with --rewrite-existing.", flush=True)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"Generation failed: {error}")
        raise SystemExit(1)
    except KeyboardInterrupt:
        print("\nGeneration interrupted. Rerun with the same target count to resume.")
        raise SystemExit(130)

