"""Register local files and request missing replicas."""
import json
import socket
from pathlib import Path
from config import parse_config_args

BATCH_SIZE = 1000


def send_request(host, port, request, timeout=30):
    """Send one newline-delimited JSON request."""
    with socket.create_connection((host, port), timeout=timeout) as client:
        client.sendall((json.dumps(request) + "\n").encode("utf-8"))
        with client.makefile("rb") as reader:
            reply = reader.readline()
            if not reply:
                raise ConnectionError("Server closed without a reply.")
            response = json.loads(reply)
    if response["status"] != "ok":
        raise ValueError(response.get("message", "Request failed."))
    return response


def index_request(args, request):
    return send_request(args.index_host, args.index_port, request)


def scan_files(shared_dir):
    for path in shared_dir.iterdir():
        if not path.is_symlink() and path.is_file() and not path.name.endswith(".part"):
            yield path.name


def register_batch(args, files):
    """Add file ownership without initiating recursive replication."""
    return index_request(args, {
        "action": "registry", "peer_id": args.peer_id,
        "host": args.peer_host, "port": args.peer_port, "files": files,
    })


def get_owners(args, file_name):
    response = index_request(args, {"action": "search", "file_name": file_name})
    return {peer["peer_id"] for peer in response["peers"]}


def ensure_replicas(args, file_name, peers, factor):
    owners = get_owners(args, file_name)
    if len(owners) >= factor:
        return True
    for target in peers:
        if target["peer_id"] in owners:
            continue
        print(f"Replicating {file_name} to {target['peer_id']}...")
        try:
            send_request(target["host"], target["port"], {
                "action": "replicate", "file_name": file_name,
                "source": {"host": args.peer_host, "port": args.peer_port},
            }, timeout=args.replication_timeout)
        except (OSError, ValueError, KeyError, TypeError) as error:
            print(f"Replication request to {target['peer_id']} failed: {error}")
        owners = get_owners(args, file_name)
        if len(owners) >= factor:
            return True
    print(f"UNDER-REPLICATED: {file_name}; registered copies={len(owners)}, required={factor}")
    return False


def options(parser):
    parser.add_argument("--replication-timeout", type=float, default=600)
    parser.add_argument("--no-replicate", action="store_true",
                        help="Register files without requesting replicas.")


def main():
    args = parse_config_args("peer", options)
    shared_dir = Path(args.shared_dir)
    if not shared_dir.is_dir():
        raise ValueError(f"Directory does not exist: {shared_dir}")
    batch, total = [], 0
    for file_name in scan_files(shared_dir):
        batch.append(file_name)
        if len(batch) >= BATCH_SIZE:
            register_batch(args, batch)
            total += len(batch)
            print(f"Registered {total} files...")
            batch = []
    if batch or total == 0:
        register_batch(args, batch)
        total += len(batch)
    print(f"Registration complete: {total} files")
    if args.no_replicate:
        return
    response = index_request(args, {"action": "list_peers", "peer_id": args.peer_id})
    factor, peers = response["replication_factor"], response["peers"]
    print(f"Replication factor: {factor}")
    if factor == 1:
        print("No additional replicas required.")
        return
    checked = under_replicated = 0
    for file_name in scan_files(shared_dir):
        if not ensure_replicas(args, file_name, peers, factor):
            under_replicated += 1
        checked += 1
    print(f"Files checked: {checked}")
    print(f"Files below replication factor: {under_replicated}")
    if under_replicated:
        print("Start/register more peers, then rerun this command.")
        raise SystemExit(1)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"Registration or replication failed: {error}")
        raise SystemExit(1)

