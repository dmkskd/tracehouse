"""Create, exercise, verify or remove the persistent object-browser demo."""
from __future__ import annotations

import argparse
import os
import sys
import time

from data_utils.env import add_connection_args, confirm_or_exit, make_client, pre_parse_env_file, print_connection
from data_utils.object_demo import DATABASE, append_object_demo, drop_object_demo, run_object_demo_activity, setup_object_demo, verify_object_demo


def main() -> None:
    env_path = pre_parse_env_file()
    parser = argparse.ArgumentParser(description="Object catalog and cascading materialized-view demo")
    add_connection_args(parser)
    parser.add_argument("action", choices=["setup", "verify", "append", "activity", "drop"], nargs="?", default="setup",
                        help="activity: run queries as the demo users (allowed and denied); they can log in only during the run")
    parser.add_argument("--rounds", type=int, default=1, help="activity: times to run the script (default: 1)")
    parser.add_argument("--interval", type=float, default=5.0, help="activity: seconds between rounds (default: 5)")
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
        elif args.action == "activity":
            run_activity(client, args)
            return
        if args.action != "drop":
            print(f"{DATABASE}: cascade verified (tenant, amount, events): {verify_object_demo(client)}")
            print("Catalog users: th_ob_alice, th_ob_bob, th_ob_carol, th_ob_denied (HOST NONE)")
    finally:
        client.disconnect()


def run_activity(client, args) -> None:
    def connect(user: str, password: str):
        return make_client(argparse.Namespace(**{**vars(args), "user": user, "password": password}))

    mismatches = 0
    for round_number in range(1, args.rounds + 1):
        results = run_object_demo_activity(client, connect, args.cluster)
        print(f"Round {round_number}/{args.rounds}")
        for r in results:
            mark = "✓" if r.matched else "✗"
            outcome = r.actual if r.matched else f"{r.actual} (expected {r.expected})"
            print(f"  {mark} {r.user:<13} {str(outcome):<20} {r.why}")
        mismatches += sum(not r.matched for r in results)
        if round_number < args.rounds:
            time.sleep(args.interval)
    print("In query_log: log_comment = 'tracehouse:object-demo'. Demo users are locked again (HOST NONE).")
    if mismatches:
        sys.exit(f"{mismatches} step(s) did not match the expected outcome")


if __name__ == "__main__":
    main()
