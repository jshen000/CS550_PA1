"""Search the configured index for an exact file name."""
from config import parse_config_args
from register import index_request


def options(parser):
    parser.add_argument("--file")
    parser.add_argument("--all-owners", action="store_true",
                        help="Include this peer in search results.")


def main():
    args = parse_config_args("peer", options)
    file_name = args.file if args.file is not None else input("Enter file name: ").strip()
    request = {"action": "search", "file_name": file_name}
    if not args.all_owners:
        request["peer_id"] = args.peer_id
    peers = index_request(args, request)["peers"]
    if not peers:
        print(f"No matching peers found for '{file_name}'.")
        return
    print(f"Peers holding '{file_name}':")
    for peer in peers:
        print(f"  {peer['peer_id']} at {peer['host']}:{peer['port']}")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"Search failed: {error}")
        raise SystemExit(1)

