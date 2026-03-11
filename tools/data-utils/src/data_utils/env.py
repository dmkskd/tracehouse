"""Shared environment and connection helpers for CLI entry points."""

from __future__ import annotations

import argparse
import os
import subprocess

from clickhouse_driver import Client


def env_int(key: str, default: str) -> int:
    """Read an int from env, allowing underscores (e.g. 100_000_000)."""
    return int(os.environ.get(key, default).replace("_", ""))


def load_env_file(env_file: str | None = None) -> str | None:
    """Load .env file into os.environ.

    When the file is explicitly requested (via --env-file or $CH_ENV_FILE),
    its values override any existing environment variables so the user's
    intent is always honoured.  When falling back to the default repo-root
    .env, existing env vars take precedence (override=False).

    Resolution order:
      1. Explicit --env-file path
      2. $CH_ENV_FILE environment variable
      3. .env in the repo root (found via git)
    """
    from dotenv import load_dotenv

    explicit = False
    if env_file:
        path = env_file
        explicit = True
    elif os.environ.get("CH_ENV_FILE"):
        path = os.environ["CH_ENV_FILE"]
        explicit = True
    else:
        path = os.path.join(_repo_root(), ".env")

    path = os.path.abspath(path)
    if os.path.isfile(path):
        load_dotenv(path, override=explicit)
        return path
    if explicit:
        print(f"\n  Env file not found: {path}")
        print(f"  Copy .env.example to get started: cp .env.example .env\n")
        raise SystemExit(1)
    return None


def obfuscate(password: str) -> str:
    """Return an obfuscated password for display."""
    if not password:
        return "(empty)"
    if len(password) <= 4:
        return "****"
    return password[:2] + "*" * (len(password) - 4) + password[-2:]


def print_connection(args: argparse.Namespace, env_path: str | None) -> None:
    """Print which .env was loaded and the final resolved connection variables."""
    print("─" * 50)
    if env_path:
        print(f"  Env file:  {env_path}")
    else:
        print("  Env file:  (none found)")
    print(f"  CH_HOST:     {args.host}")
    print(f"  CH_PORT:     {args.port}")
    print(f"  CH_USER:     {args.user}")
    print(f"  CH_PASSWORD: {obfuscate(args.password)}")
    print(f"  CH_SECURE:   {args.secure}")
    print("─" * 50)

    if not env_path:
        print()
        print("⚠  No .env file loaded. Set CH_ENV_FILE, pass --env-file, or create .env in the repo root (see .env.example)")
        print()


def confirm_or_exit(args: argparse.Namespace) -> None:
    """Prompt user to confirm before proceeding. Skipped with -y/--assume-yes or CH_ASSUME_YES=1."""
    if getattr(args, "assume_yes", False):
        return
    try:
        answer = input("Proceed? [y/N] ").strip().lower()
    except (KeyboardInterrupt, EOFError):
        print("\nAborted.")
        raise SystemExit(1)
    if answer != "y":
        print("Aborted.")
        raise SystemExit(1)


def add_connection_args(parser: argparse.ArgumentParser) -> None:
    """Add the standard --host/--port/--user/--password/--secure/--yes arguments."""
    parser.add_argument("--env-file", default=None, help="Path to .env file (default: $CH_ENV_FILE or <repo>/.env)")
    parser.add_argument("--host", default=os.environ.get("CH_HOST", "localhost"), help="ClickHouse host (default: $CH_HOST or localhost)")
    parser.add_argument("--port", type=int, default=env_int("CH_PORT", "9000"), help="ClickHouse native port (default: $CH_PORT or 9000)")
    parser.add_argument("--user", default=os.environ.get("CH_USER", "default"), help="ClickHouse user (default: $CH_USER or default)")
    parser.add_argument("--password", default=os.environ.get("CH_PASSWORD", ""), help="ClickHouse password (default: $CH_PASSWORD or empty)")
    parser.add_argument("--secure", action="store_true", default=os.environ.get("CH_SECURE", "").lower() in ("1", "true", "yes"), help="Use TLS (default: $CH_SECURE or false)")
    parser.add_argument("-y", "--assume-yes", action="store_true", default=os.environ.get("CH_ASSUME_YES", "").lower() in ("1", "true", "yes"), help="Skip confirmation prompt (default: $CH_ASSUME_YES or false)")
    parser.add_argument("--users", type=int, default=env_int("CH_USERS", "0"), help="Number of test users (1-10) to distribute work across. 0 = use default user only (default: $CH_USERS or 0)")
    parser.add_argument("--user-skew", type=float, default=float(os.environ.get("CH_USER_SKEW", "0")), help="Skew user distribution: 0 = equal, 1 = Zipf, 2+ = very skewed (default: $CH_USER_SKEW or 0)")


def make_client(args: argparse.Namespace, **kwargs) -> Client:
    """Create a clickhouse-driver Client from parsed args."""
    return Client(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        secure=args.secure,
        compression=kwargs.pop("compression", False),
        **kwargs,
    )


def pre_parse_env_file() -> str | None:
    """Pre-parse --env-file from sys.argv and load it before argparse defaults are read."""
    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--env-file", default=None)
    pre_args, _ = pre.parse_known_args()
    return load_env_file(pre_args.env_file)


def _repo_root() -> str:
    """Find the repository root by asking git."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True, text=True, check=True,
        )
        return result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        # Fallback: walk up from this file
        d = os.path.dirname(__file__)
        for _ in range(10):
            if os.path.isfile(os.path.join(d, ".env")):
                return d
            d = os.path.dirname(d)
        return os.getcwd()
