"""Find a source peer, download a file, and register its new owner."""
import json
import socket
import tempfile
from pathlib import Path
from config import parse_config_args
from register import index_request, register_batch

CHUNK_SIZE = 64 * 1024
DEFAULT_DOWNLOAD_DIR = Path(__file__).resolve().parent / "peer_b" / "shared"


def download_file(file_name, peer_host, peer_port, download_dir=DEFAULT_DOWNLOAD_DIR):
    """Download atomically; also used by the peer replication service."""
    if (not isinstance(file_name, str) or not file_name
            or Path(file_name).name != file_name
            or file_name in (".", "..") or file_name.endswith(".part")):
        print("Invalid file name.")
        return False
    temporary = None
    try:
        download_dir = Path(download_dir).resolve()
        download_dir.mkdir(parents=True, exist_ok=True)
        destination = download_dir / file_name
        with socket.create_connection((peer_host, peer_port), timeout=30) as client:
            request = {"action": "obtain", "file_name": file_name}
            client.sendall((json.dumps(request) + "\n").encode("utf-8"))
            with client.makefile("rb") as reader:
                header = reader.readline()
                if not header:
                    raise ConnectionError("Peer closed without a reply.")
                response = json.loads(header)
                if response["status"] != "ok":
                    raise ValueError(response.get("message", "Unknown error"))
                file_size = response["file_size"]
                if type(file_size) is not int or file_size < 0:
                    raise ValueError("Invalid file size.")
                with tempfile.NamedTemporaryFile(
                    mode="wb", dir=download_dir, prefix=".download-",
                    suffix=".part", delete=False,
                ) as output:
                    temporary = Path(output.name)
                    remaining = file_size
                    while remaining > 0:
                        chunk = reader.read(min(CHUNK_SIZE, remaining))
                        if not chunk:
                            raise ConnectionError("Connection closed before the file was complete.")
                        output.write(chunk)
                        remaining -= len(chunk)
        temporary.replace(destination)
        temporary = None
        print(f"Downloaded {file_name}: {file_size} bytes")
        print(f"Saved to: {destination}")
        print(f"display file '{file_name}'")
        return True
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"Download failed: {error}")
        return False
    finally:
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError as error:
                print(f"Temporary file cleanup failed: {error}")


def options(parser):
    parser.add_argument("--file")
    parser.add_argument("--source-peer", help="Choose a registered source by peer ID.")


def main():
    args = parse_config_args("peer", options)
    file_name = args.file if args.file is not None else input("Enter file name to download: ").strip()
    peers = index_request(args, {
        "action": "search", "peer_id": args.peer_id, "file_name": file_name,
    })["peers"]
    if args.source_peer:
        peers = [peer for peer in peers if peer["peer_id"] == args.source_peer]
    if not peers:
        print(f"No matching source peer found for '{file_name}'.")
        raise SystemExit(1)
    for peer in peers:
        print(f"Downloading from {peer['peer_id']} at {peer['host']}:{peer['port']}")
        if download_file(file_name, peer["host"], peer["port"], args.shared_dir):
            break
    else:
        raise SystemExit(1)
    try:
        register_batch(args, [file_name])
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"Download succeeded, but registration failed: {error}")
        print("The file has been kept. Run register.py to retry registration.")
        raise SystemExit(2)
    print(f"Registered '{file_name}' as {args.peer_id} at {args.peer_host}:{args.peer_port}")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"Request failed: {error}")
        raise SystemExit(1)

