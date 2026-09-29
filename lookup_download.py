import argparse
import json
import socket

from download import download_file


def send_request(request, index_host, index_port):
    """Send one request to the indexing server."""
    with socket.create_connection(
        (index_host, index_port), timeout=30
    ) as client:
        message = json.dumps(request) + "\n"
        client.sendall(message.encode("utf-8"))

        with client.makefile("rb") as reader:
            reply = reader.readline()

            if not reply:
                raise ConnectionError(
                    "Index server closed without a reply."
                )

            response = json.loads(reply)

    if response["status"] != "ok":
        raise ValueError(
            response.get("message", "Request failed.")
        )

    return response


def search_file(file_name, args):
    """Find other peers that hold the requested file."""
    request = {
        "action": "search",
        "peer_id": args.peer_id,
        "file_name": file_name,
    }

    response = send_request(
        request,
        args.index_host,
        args.index_port,
    )

    return response["peers"]


def register_download(file_name, args):
    """Register a successfully downloaded file."""
    request = {
        "action": "registry",
        "peer_id": args.peer_id,
        "host": args.peer_host,
        "port": args.peer_port,
        "files": [file_name],
    }

    send_request(
        request,
        args.index_host,
        args.index_port,
    )


def main():
    """Search, download, and register a file."""
    parser = argparse.ArgumentParser(
        description="Download a shared file and register it."
    )

    parser.add_argument("--peer-id", required=True)
    parser.add_argument("--peer-host", default="127.0.0.1")
    parser.add_argument("--peer-port", type=int, required=True)
    parser.add_argument("--shared-dir", required=True)
    parser.add_argument("--index-host", default="127.0.0.1")
    parser.add_argument("--index-port", type=int, default=9000)

    args = parser.parse_args()

    file_name = input("Enter file name: ").strip()

    peers = search_file(file_name, args)

    if not peers:
        print(f"No other peers found for '{file_name}'.")
        return

    print(f"Peers holding '{file_name}':")

    for number, peer in enumerate(peers, start=1):
        print(
            f"{number}. {peer['peer_id']} "
            f"at {peer['host']}:{peer['port']}"
        )

    choice = int(input("Choose peer number: "))

    if not 1 <= choice <= len(peers):
        print("Invalid peer number.")
        return

    selected = peers[choice - 1]

    success = download_file(
        file_name,
        selected["host"],
        selected["port"],
        args.shared_dir,
    )

    if not success:
        raise SystemExit(1)

    # Register only after the complete file has been saved.
    try:
        register_download(file_name, args)

    except (OSError, ValueError, KeyError, TypeError) as error:
        print("Download succeeded, but registration failed.")
        print(f"Registration error: {error}")
        print("The downloaded file is still saved locally.")
        print("Run register.py to register it later.")
        raise SystemExit(1)

    print(f"Registered '{file_name}' for {args.peer_id}.")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"Error: {error}")
        raise SystemExit(1)
