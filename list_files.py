from config import parse_config_args
from register import index_request


def main():
    """Display all file names registered with the indexing server."""
    args = parse_config_args("peer")

    response = index_request(args, {
        "action": "list_files",
    })

    files = sorted(
        response["files"],
        key=lambda item: item["file_name"],
    )

    print(f"Total registered file names: {len(files)}")

    if not files:
        print("No files are registered.")
        return

    for number, item in enumerate(files, start=1):
        print(f"\n{number}. {item['file_name']}")

        for peer in item["peers"]:
            print(
                f"   {peer['peer_id']} "
                f"at {peer['host']}:{peer['port']}"
            )


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"Cannot list registered files: {error}")
        raise SystemExit(1)
