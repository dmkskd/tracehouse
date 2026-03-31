"""Tests for TTL helpers: parse_ttl, ttl_clause, ttl_settings, partition_clause."""

from __future__ import annotations

import pytest

from data_utils.tables.helpers import parse_ttl, ttl_clause, ttl_settings, partition_clause


# ── parse_ttl ─────────────────────────────────────────────────────


class TestParseTtl:
    """parse_ttl converts human-friendly TTL strings to CH interval expressions."""

    @pytest.mark.parametrize("input_val,expected", [
        ("12h", "12 HOUR"),
        ("1hr", "1 HOUR"),
        ("24hour", "24 HOUR"),
        ("3hours", "3 HOUR"),
        ("2d", "2 DAY"),
        ("7day", "7 DAY"),
        ("14days", "14 DAY"),
        ("30m", "30 MINUTE"),
        ("5min", "5 MINUTE"),
        ("90minute", "90 MINUTE"),
        ("120minutes", "120 MINUTE"),
    ])
    def test_suffixed_formats(self, input_val: str, expected: str) -> None:
        assert parse_ttl(input_val) == expected

    @pytest.mark.parametrize("input_val,expected", [
        ("12", "12 HOUR"),
        ("1", "1 HOUR"),
        ("24", "24 HOUR"),
    ])
    def test_bare_integer_backward_compat(self, input_val: str, expected: str) -> None:
        """Bare integers are treated as hours for backward compat with CH_GEN_TTL_HOURS."""
        assert parse_ttl(input_val) == expected

    @pytest.mark.parametrize("input_val", ["0", "", "  0  ", "  "])
    def test_disabled(self, input_val: str) -> None:
        assert parse_ttl(input_val) == ""

    @pytest.mark.parametrize("input_val", ["abc", "12x", "h12", "-5h"])
    def test_invalid_raises(self, input_val: str) -> None:
        with pytest.raises(ValueError, match="Invalid TTL format"):
            parse_ttl(input_val)

    def test_whitespace_stripped(self) -> None:
        assert parse_ttl("  12h  ") == "12 HOUR"


# ── ttl_clause ────────────────────────────────────────────────────


class TestTtlClause:
    def test_active(self) -> None:
        assert ttl_clause("12 HOUR") == "TTL _inserted_at + INTERVAL 12 HOUR"

    def test_minutes(self) -> None:
        assert ttl_clause("30 MINUTE") == "TTL _inserted_at + INTERVAL 30 MINUTE"

    def test_days(self) -> None:
        assert ttl_clause("2 DAY") == "TTL _inserted_at + INTERVAL 2 DAY"

    def test_disabled(self) -> None:
        assert ttl_clause("") == ""


# ── ttl_settings ──────────────────────────────────────────────────


class TestTtlSettings:
    def test_active_includes_drop_parts(self) -> None:
        result = ttl_settings("12 HOUR")
        assert "merge_with_ttl_timeout" in result
        assert "ttl_only_drop_parts = 1" in result

    def test_disabled(self) -> None:
        assert ttl_settings("") == ""


# ── partition_clause ──────────────────────────────────────────────


class TestPartitionClause:
    def test_ttl_active_uses_hourly(self) -> None:
        result = partition_clause("12 HOUR", "toYYYYMM(event_date)")
        assert "toStartOfHour(_inserted_at)" in result
        assert "event_date" not in result

    def test_ttl_disabled_uses_default(self) -> None:
        result = partition_clause("", "toYYYYMM(event_date)")
        assert "toYYYYMM(event_date)" in result
        assert "_inserted_at" not in result

    def test_different_defaults(self) -> None:
        assert "toYear(date)" in partition_clause("", "toYear(date)")
        assert "toYYYYMM(pickup_date)" in partition_clause("", "toYYYYMM(pickup_date)")

    def test_days_trigger_daily(self) -> None:
        result = partition_clause("2 DAY", "toYYYYMM(x)")
        assert "toStartOfDay" in result
        assert "toStartOfHour" not in result

    def test_minutes_trigger_minutely(self) -> None:
        """Minute TTLs use minute-level partitions so drops happen promptly."""
        result = partition_clause("5 MINUTE", "toYYYYMM(x)")
        assert "toStartOfMinute" in result
        assert "toStartOfHour" not in result

    def test_hours_trigger_hourly(self) -> None:
        result = partition_clause("12 HOUR", "toYYYYMM(x)")
        assert "toStartOfHour" in result
        assert "toStartOfMinute" not in result


# ── Integration: TTL-enabled dataset creates correct DDL ─────────


class TestTtlCreateTable:
    """Verify that TTL-enabled datasets produce the right PARTITION BY and SETTINGS."""

    @pytest.fixture()
    def client(self, client: pytest.fixture) -> pytest.fixture:
        return client

    def test_synthetic_data_ttl_partitioning(self, client) -> None:
        from data_utils.tables import SyntheticData

        ds = SyntheticData(ttl_interval="12 HOUR")
        ds.create(client)
        try:
            result = client.execute(
                "SELECT partition_key, sorting_key "
                "FROM system.tables "
                "WHERE database = 'synthetic_data' AND name = 'events'"
            )
            assert len(result) == 1
            partition_key, sorting_key = result[0]
            # Partition should use hourly _inserted_at, not business date
            assert "toStartOfHour(_inserted_at)" in partition_key
            assert "event_date" not in partition_key
            # ORDER BY should be unchanged
            assert "event_date" in sorting_key
            assert "user_id" in sorting_key
        finally:
            client.execute("DROP TABLE IF EXISTS synthetic_data.user_tiers SYNC")
            client.execute("DROP TABLE IF EXISTS synthetic_data.events SYNC")
            client.execute("DROP DATABASE IF EXISTS synthetic_data SYNC")

    def test_synthetic_data_no_ttl_uses_default(self, client) -> None:
        from data_utils.tables import SyntheticData

        ds = SyntheticData(ttl_interval="")
        ds.create(client)
        try:
            result = client.execute(
                "SELECT partition_key "
                "FROM system.tables "
                "WHERE database = 'synthetic_data' AND name = 'events'"
            )
            partition_key = result[0][0]
            assert "toYYYYMM(event_date)" in partition_key
            assert "_inserted_at" not in partition_key
        finally:
            client.execute("DROP TABLE IF EXISTS synthetic_data.user_tiers SYNC")
            client.execute("DROP TABLE IF EXISTS synthetic_data.events SYNC")
            client.execute("DROP DATABASE IF EXISTS synthetic_data SYNC")
