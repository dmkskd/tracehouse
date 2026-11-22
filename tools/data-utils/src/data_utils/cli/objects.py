"""Create, exercise, verify or remove the persistent object-browser demo."""
from __future__ import annotations

import argparse
import os

from data_utils.env import add_connection_args, confirm_or_exit, make_client, pre_parse_env_file, print_connection
from data_utils.object_demo import DATABASE, append_object_demo, drop_object_demo, setup_object_demo, verify_object_demo


def main() -> None:
    env_path = pre_parse_env_file()
    parser = argparse.ArgumentParser(description="Object catalog and cascading materialized-view demo")
    add_connection_args(parser)
    parser.add_argument("action", choices=["setup", "verify", "append", "drop"], nargs="?", default="setup")
    parser.add_argument("--cluster", default=os.environ.get("CH_CLUSTER", ""), help="Propagate DDL ON CLUSTER; data and verification run on the connected host")
    args = parser.parse_args()
    print_connection(args, env_path)
    if args.action != "verify":
        confirm_or_exit(args)
    client = make_client(args)
    try:
        if args.action == "setup":
            setup_object_demo(client, args.cluster)
        elif args.action == "drop":
            drop_object_demo(client, args.cluster)
        elif args.action == "append":
            append_object_demo(client)
        if args.action != "drop":
            print(f"{DATABASE}: cascade verified (tenant, amount, events): {verify_object_demo(client)}")
            print("Catalog users: th_ob_alice, th_ob_bob, th_ob_carol, th_ob_denied (HOST NONE)")
    finally:
        client.disconnect()


if __name__ == "__main__":
    main()
