"""Drop all test databases created by tracehouse-generate.

Discovers datasets from the same plugin registry used by the generator,
so new datasets are automatically included without any hardcoded lists.

Usage:
    tracehouse-drop [options]
"""

from __future__ import annotations

import argparse

from data_utils.capabilities import probe
from data_utils.env import (
    add_connection_args, make_client, pre_parse_env_file, print_connection,
    confirm_or_exit,
)
from data_utils.tables import build_all_datasets


def main() -> None:
    env_path = pre_parse_env_file()

    parser = argparse.ArgumentParser(
        description="Drop all test databases created by tracehouse-generate",
    )
    add_connection_args(parser)
    args = parser.parse_args()

    print_connection(args, env_path)
    print(f"\nConnecting to ClickHouse at {args.host}:{args.port}...")
    client = make_client(args)

    print("Probing server capabilities...")
    caps = probe(client)

    datasets = build_all_datasets(caps=caps)
    names = [ds.name for ds in datasets]

    # Check which databases actually exist
    existing = {
        row[0]
        for row in client.execute(
            "SELECT name FROM system.databases WHERE name IN %(names)s",
            {"names": names},
        )
    }

    if not existing:
        print("\nNo test databases found — nothing to drop.")
        return

    print("\nDatabases to drop:")
    for name in names:
        marker = "  (exists)" if name in existing else "  (not found)"
        print(f"  - {name}{marker}")

    confirm_or_exit(args)

    print()
    for ds in datasets:
        if ds.name in existing:
            # Kill active queries against this database so DROP doesn't hang
            client.execute(
                "KILL QUERY WHERE databases = %(dbs)s SYNC",
                {"dbs": [ds.name]},
            )
            ds.drop(client)
            print(f"  dropped {ds.name}")

    print("\nAll test databases dropped.")


if __name__ == "__main__":
    main()
