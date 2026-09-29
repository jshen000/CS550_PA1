from config import parse_config_args
import argparse

import json

import os

import socket

import threading

from pathlib import Path

from download import download_file

CHUNK_SIZE = 64 * 1024

# Limit replication downloads to one at a time on this peer.

replication_lock = threading.Lock()

def send_json(conn, response):

    """Send one newline-terminated JSON message."""

    message = json.dumps(response) + "\n"

    conn.sendall(message.encode("utf-8"))

def register_file(file_name, args):

    """Register a completed replica with the indexing server."""

    request = {

        "action": "registry",

        "peer_id": args.peer_id,

        "host": args.peer_host,

        "port": args.port,

        "files": [file_name],

    }

    with socket.create_connection(

        (args.index_host, args.index_port), timeout=30

    ) as client:

        send_json(client, request)

        with client.makefile("rb") as reader:

            reply = reader.readline()

            if not reply:

                raise ConnectionError("Index server closed without a reply.")

            response = json.loads(reply)

    if response["status"] != "ok":

        raise ValueError(

            response.get("message", "Registration failed.")

        )

def serve_file(conn, file_name, shared_dir):

    """Send a local file to the requester."""

    file_path = shared_dir / file_name

    if file_path.is_symlink() or not file_path.is_file():

        send_json(conn, {

            "status": "error",

            "message": "File not found",

        })

        return

    with file_path.open("rb") as source:

        file_size = os.fstat(source.fileno()).st_size

        send_json(conn, {

            "status": "ok",

            "file_name": file_name,

            "file_size": file_size,

        })

        remaining = file_size

        while remaining > 0:

            chunk = source.read(min(CHUNK_SIZE, remaining))

            if not chunk:

                raise OSError("File ended before the expected size.")

            conn.sendall(chunk)

            remaining -= len(chunk)

    print(f"Sent: {file_name} ({file_size} bytes)")

def replicate_file(conn, request, args, shared_dir):

    """Download a replica and register it after completion."""

    file_name = request["file_name"]

    source = request["source"]

    with replication_lock:

        success = download_file(

            file_name,

            source["host"],

            source["port"],

            shared_dir,

        )

        if not success:

            send_json(conn, {

                "status": "error",

                "message": "Replica download failed",

            })

            return

        try:

            register_file(file_name, args)

        except (OSError, ValueError, KeyError, TypeError) as error:

            send_json(conn, {

                "status": "error",

                "message": (

                    "Replica saved locally, but registration failed: "

                    f"{error}"

                ),

            })

            return

    print(f"Replica registered: {file_name}")

    send_json(conn, {

        "status": "ok",

        "message": "Replica downloaded and registered",

        "file_name": file_name,

        "peer_id": args.peer_id,

    })

def handle_client(conn, address, args, shared_dir):

    """Handle a download request or a replication request."""

    try:

        with conn:

            conn.settimeout(30)

            with conn.makefile("rb") as reader:

                message = reader.readline()

                if not message:

                    return

                request = json.loads(message)

                action = request["action"]

                if action not in ("obtain", "replicate"):

                    send_json(conn, {

                        "status": "error",

                        "message": "Unknown action",

                    })

                    return

                file_name = request["file_name"]

                # Accept only complete files with plain file names.

                if (

                    not isinstance(file_name, str)

                    or not file_name

                    or Path(file_name).name != file_name

                    or file_name in (".", "..")

                    or file_name.endswith(".part")

                ):

                    send_json(conn, {

                        "status": "error",

                        "message": "Invalid file name",

                    })

                    return

                if action == "obtain":

                    serve_file(conn, file_name, shared_dir)

                elif action == "replicate":

                    replicate_file(conn, request, args, shared_dir)

    except (OSError, ValueError, KeyError, TypeError) as error:

        print(f"Client error {address}: {error}")

def main():

    """Start a peer with download and replication services."""

    args = parse_config_args("peer")

    shared_dir = Path(args.shared_dir).resolve()

    shared_dir.mkdir(parents=True, exist_ok=True)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:

        server.setsockopt(

            socket.SOL_SOCKET, socket.SO_REUSEADDR, 1

        )

        server.bind((args.host, args.port))

        server.listen()

        print(f"Peer ID: {args.peer_id}")

        print(f"Listening on {args.host}:{args.port}")

        print(f"Shared directory: {shared_dir}")

        while True:

            conn, address = server.accept()

            worker = threading.Thread(

                target=handle_client,

                args=(conn, address, args, shared_dir),

                daemon=True,

            )

            worker.start()

if __name__ == "__main__":

    main()

