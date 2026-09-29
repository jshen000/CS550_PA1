"""Read all deployment addresses from a JSON configuration file."""
import argparse
import json
from pathlib import Path


def parse_config_args(role, configure=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    if configure:
        configure(parser)
    args = parser.parse_args()
    try:
        path = Path(args.config).expanduser().resolve()
        data = json.loads(path.read_text(encoding="utf-8"))

        def text(section, key):
            value = section[key]
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{key} must be a nonempty string")
            return value

        def port(section):
            value = section["port"]
            if type(value) is not int or not 1 <= value <= 65535:
                raise ValueError("port must be an integer from 1 to 65535")
            return value

        index = data["index"]
        args.index_host = text(index, "host")
        args.index_port = port(index)
        if role == "index":
            args.host = text(index, "listen_host")
            args.port = args.index_port
            args.replication_factor = index["replication_factor"]
            if type(args.replication_factor) is not int or args.replication_factor < 1:
                raise ValueError("replication_factor must be a positive integer")
        else:
            peer = data["peer"]
            args.peer_id = text(peer, "id")
            args.peer_host = text(peer, "host")
            args.host = text(peer, "listen_host")
            args.peer_port = args.port = port(peer)
            shared = Path(text(peer, "shared_dir")).expanduser()
            args.shared_dir = (path.parent / shared).resolve()
        if hasattr(args, "replication_timeout") and args.replication_timeout <= 0:
            raise ValueError("replication timeout must be positive")
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.error(f"Invalid configuration: {error}")
    return args

