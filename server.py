import argparse
import json
import socket
import threading

# Map file names to the peers that hold them.
file_index = {}

# Track registered peers, including peers with no files.
peer_registry = {}

# Protect both shared dictionaries.
index_lock = threading.Lock()


def handle_client(conn, address, replication_factor):
    """Process one indexing request."""
    try:
        with conn:
            conn.settimeout(30)

            with conn.makefile("r", encoding="utf-8") as reader:
                message = reader.readline()

                if not message:
                    return

                request = json.loads(message)
                action = request["action"]

                if action == "registry":
                    peer_id = request["peer_id"]
                    files = request["files"]

                    peer_info = {
                        "peer_id": peer_id,
                        "host": request["host"],
                        "port": request["port"],
                    }

                    with index_lock:
                        # Keep one shared address record per peer.
                        if peer_id not in peer_registry:
                            peer_registry[peer_id] = {}

                        peer_registry[peer_id].update(peer_info)
                        record = peer_registry[peer_id]

                        for file_name in files:
                            if file_name not in file_index:
                                file_index[file_name] = {}

                            file_index[file_name][peer_id] = record

                        print(
                            f"Registered {len(files)} files from {peer_id}; "
                            f"indexed file names: {len(file_index)}"
                        )

                    response = {
                        "status": "ok",
                        "file_count": len(files),
                        "replication_factor": replication_factor,
                    }

                elif action == "search":
                    file_name = request["file_name"]
                    requester_id = request.get("peer_id")

                    with index_lock:
                        owners = file_index.get(file_name, {})

                        peers = [
                            info.copy()
                            for peer_id, info in owners.items()
                            if peer_id != requester_id
                        ]

                    response = {
                        "status": "ok",
                        "file_name": file_name,
                        "peers": peers,
                    }

                elif action == "list_peers":
                    requester_id = request.get("peer_id")

                    with index_lock:
                        peers = [
                            info.copy()
                            for peer_id, info in peer_registry.items()
                            if peer_id != requester_id
                        ]

                    response = {
                        "status": "ok",
                        "replication_factor": replication_factor,
                        "peers": peers,
                    }

                else:
                    response = {
                        "status": "error",
                        "message": "Unknown action",
                    }

                reply = json.dumps(response) + "\n"
                conn.sendall(reply.encode("utf-8"))

    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"Client error {address}: {error}")


def main():
    """Start the indexing server."""
    parser = argparse.ArgumentParser(
        description="Run the central indexing server."
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument(
        "--replication-factor",
        type=int,
        default=1,
    )
    args = parser.parse_args()

    if args.replication_factor < 1:
        parser.error("--replication-factor must be at least 1")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(
            socket.SOL_SOCKET, socket.SO_REUSEADDR, 1
        )
        server.bind((args.host, args.port))
        server.listen()

        print(f"Index server listening on {args.host}:{args.port}")
        print(f"Replication factor: {args.replication_factor}")

        while True:
            conn, address = server.accept()

            worker = threading.Thread(
                target=handle_client,
                args=(conn, address, args.replication_factor),
                daemon=True,
            )
            worker.start()


if __name__ == "__main__":
    main()
