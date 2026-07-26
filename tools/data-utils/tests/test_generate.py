"""Tests for data-generation orchestration."""

from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest

from data_utils.cli import generate
from data_utils.tables import InsertConfig, InsertMode


class FakeDataset:
    def __init__(self, name: str) -> None:
        self.name = name
        self.created = 0
        self.dropped = 0

    def create(self, client: object) -> None:
        del client
        self.created += 1

    def drop(self, client: object) -> None:
        del client
        self.dropped += 1


def test_skip_create_cannot_be_combined_with_drop(monkeypatch) -> None:
    monkeypatch.setattr(generate, "pre_parse_env_file", lambda: None)
    monkeypatch.setattr(
        sys,
        "argv",
        ["tracehouse-generate", "--mode", "drop", "--skip-create"],
    )

    with pytest.raises(SystemExit) as error:
        generate._parse_args()

    assert error.value.code == 2


def test_prepare_datasets_can_skip_schema_creation(monkeypatch) -> None:
    client = object()
    monkeypatch.setattr(generate, "make_client", lambda args: client)
    datasets = [FakeDataset("one"), FakeDataset("two")]
    config = InsertConfig(
        rows=10,
        partitions=1,
        batch_size=10,
        mode=InsertMode.APPEND,
    )

    clients = generate._prepare_datasets(
        datasets,
        config,
        SimpleNamespace(),
        skip_create=True,
    )

    assert clients == {"one": client, "two": client}
    assert all(dataset.created == 0 for dataset in datasets)
    assert all(dataset.dropped == 0 for dataset in datasets)


def test_prepare_datasets_still_recreates_in_drop_mode(monkeypatch) -> None:
    client = object()
    monkeypatch.setattr(generate, "make_client", lambda args: client)
    dataset = FakeDataset("one")
    config = InsertConfig(
        rows=10,
        partitions=1,
        batch_size=10,
        mode=InsertMode.DROP,
    )

    generate._prepare_datasets(
        [dataset],
        config,
        SimpleNamespace(),
    )

    assert dataset.dropped == 1
    assert dataset.created == 1
