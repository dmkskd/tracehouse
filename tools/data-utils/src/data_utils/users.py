"""Test user management for multi-user workloads.

Creates temporary ClickHouse users with random passwords so that queries,
inserts, mutations, etc. show up under different usernames in system tables
(query_log, processes, …).

Security model:
  - Passwords are random 32-byte hex tokens per script run.
  - Users are created with ``IDENTIFIED BY …`` (plaintext auth).
  - The password appears in query_log, but is a random token with no
    reuse value.

When running multiple tools in parallel, use ``tracehouse-data-tools-tui`` which
creates users once and shares credentials via the ``TRACEHOUSE_TEST_USERS``
environment variable.
"""

from __future__ import annotations

import json
import os
import random
import secrets
import sys
from dataclasses import dataclass

from clickhouse_driver import Client

# Static list of friendly user names — max 10.
# Prefixed with th_ (tracehouse) to avoid collisions with real users.
TEST_USER_NAMES = [
    "th_alice", "th_bob", "th_charlie", "th_diana", "th_eve",
    "th_frank", "th_grace", "th_hank", "th_ivy", "th_jack",
]

MAX_USERS = len(TEST_USER_NAMES)


@dataclass
class TestUser:
    name: str
    password: str


def create_test_users(admin_client: Client, n: int) -> list[TestUser]:
    """Create (or reset) *n* test users with random passwords.

    Returns a list of ``TestUser`` with the plaintext passwords needed
    to connect.  The admin client must have ``ACCESS MANAGEMENT`` privileges.
    """
    if n < 1 or n > MAX_USERS:
        print(f"Error: --users must be between 1 and {MAX_USERS}, got {n}")
        sys.exit(1)

    users: list[TestUser] = []
    names = TEST_USER_NAMES[:n]

    for name in names:
        # Use token_hex to avoid any special chars that could break SQL
        password = secrets.token_hex(16)

        # Drop and recreate to ensure clean state (avoids stale auth from previous runs)
        admin_client.execute(f"DROP USER IF EXISTS {name}")
        admin_client.execute(
            f"CREATE USER {name} "
            f"IDENTIFIED BY '{password}'"
        )
        # Grant the privileges needed by all tools:
        # SELECT, INSERT, ALTER, CREATE, DROP, OPTIMIZE, KILL QUERY, SYSTEM
        admin_client.execute(
            f"GRANT SELECT, INSERT, ALTER, CREATE DATABASE, CREATE TABLE, "
            f"DROP DATABASE, DROP TABLE, OPTIMIZE, KILL QUERY, SYSTEM, "
            f"S3 ON *.* TO {name}"
        )
        users.append(TestUser(name=name, password=password))

    return users


ENV_VAR = "TRACEHOUSE_TEST_USERS"


def serialize_test_users(users: list[TestUser]) -> str:
    """Serialize test users to JSON for the TRACEHOUSE_TEST_USERS env var."""
    return json.dumps({u.name: u.password for u in users})


def load_test_users_from_env() -> list[TestUser] | None:
    """Load test users from TRACEHOUSE_TEST_USERS env var, if set.

    Returns None if the env var is not set.
    """
    raw = os.environ.get(ENV_VAR)
    if not raw:
        return None
    data = json.loads(raw)
    return [TestUser(name=name, password=pw) for name, pw in data.items()]


def verify_test_user(args, user: TestUser) -> None:
    """Verify a test user can connect and run a basic query."""
    test_client = make_user_client(args, user)
    try:
        result = test_client.execute("SELECT currentUser()")
        actual_user = result[0][0]
        if actual_user != user.name:
            raise RuntimeError(f"Connected as '{actual_user}' instead of '{user.name}'")
        print(f"  ✓ Verified {user.name} can connect")
    except Exception as e:
        raise RuntimeError(
            f"Test user '{user.name}' cannot connect: {e}\n"
            f"Check that the admin user has ACCESS MANAGEMENT privileges."
        ) from e
    finally:
        test_client.disconnect()


def lock_test_users(admin_client: Client, users: list[TestUser] | None = None) -> None:
    """Lock test users so no one can connect as them.

    If *users* is None, locks all names in ``TEST_USER_NAMES`` (safe to call
    even if some don't exist).
    """
    names = [u.name for u in users] if users else TEST_USER_NAMES
    for name in names:
        try:
            admin_client.execute(f"ALTER USER {name} HOST NONE")
        except Exception:
            pass  # user may not exist


def make_user_client(args, user: TestUser) -> Client:
    """Create a clickhouse-driver Client connected as a test user."""
    return Client(
        host=args.host,
        port=args.port,
        user=user.name,
        password=user.password,
        secure=args.secure,
    )


def get_user_for_index(users: list[TestUser], i: int) -> TestUser:
    """Round-robin user assignment (deterministic, for pre-assigning workers)."""
    return users[i % len(users)]


def pick_random_user(users: list[TestUser], skew: float = 0) -> TestUser:
    """Weighted random pick with configurable skew.

    *skew* controls how uneven the distribution is:
      0 = equal (flat), 1 = Zipf (~55/27/18%), 2+ = very skewed.
    Weight formula: 1 / rank^skew.
    """
    if skew == 0:
        return random.choice(users)
    weights = [1.0 / (i + 1) ** skew for i in range(len(users))]
    return random.choices(users, weights=weights, k=1)[0]


def print_test_users(users: list[TestUser], skew: float = 0) -> None:
    """Print the list of active test users with expected traffic distribution."""
    n = len(users)
    if skew == 0:
        pcts = [100.0 / n] * n
        label = "equal"
    else:
        weights = [1.0 / (i + 1) ** skew for i in range(n)]
        total = sum(weights)
        pcts = [w / total * 100 for w in weights]
        label = f"skew={skew}"

    print(f"\n  Test users ({n}, {label}):")
    for user, pct in zip(users, pcts):
        bar = "#" * max(1, round(pct / 2))
        print(f"    {user.name:>10}  {pct:5.1f}%  {bar}")
