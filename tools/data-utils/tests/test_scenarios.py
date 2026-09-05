"""Tests for the fault-injection scenarios used to evaluate LLM investigations.

Only the failures that are silent and expensive. A leaked cause or a mismatched
window does not crash anything: it produces eval runs that look fine and mean
nothing, and the damage is only visible much later, if ever.

Everything else here (dataclass shape, registry contents) fails loudly the
first time the CLI runs, so it is not worth a test.

Injection needs a live ClickHouse and is not covered.
"""

from __future__ import annotations

import argparse
import json

import pytest

from data_utils.cli.scenarios import SCENARIOS, write_answer, write_question

STARTED = "2026-01-01T00:00:00+00:00"
ENDED = "2026-01-01T00:10:00+00:00"


def make_args(**overrides) -> argparse.Namespace:
    defaults = dict(
        host="localhost", port=9000, user="default", password="",
        database="synthetic_data", table="events",
        warmup=120.0, settle=180.0, intensity=10,
        no_workload=False, keep=False, out="scenario-runs", dry_run=False,
    )
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


class FakeContext:
    """Stands in for the live Context, which owns a ClickHouse connection."""

    def __init__(self) -> None:
        self.timeline: list[dict] = []
        self.client = None


@pytest.mark.parametrize("key", sorted(SCENARIOS))
def test_question_does_not_name_the_cause(tmp_path, key: str):
    """The question is the prompt. Naming the mechanism, or the system table
    that reveals it, hands over most of the task and quietly inflates every
    score from that run on."""
    scenario = SCENARIOS[key]
    path = tmp_path / "run.question.md"
    write_question(str(path), scenario, make_args(), STARTED, ENDED)
    question = path.read_text()

    assert scenario.mechanism not in question
    for table in scenario.expected_system_tables:
        assert table not in question, f"{key} leaks {table}"


def test_control_answer_states_that_nothing_was_injected(tmp_path):
    """control is the false-positive check: a model that confidently names a
    cause has failed. That only grades correctly if the key says so."""
    path = tmp_path / "run.answer.json"
    write_answer(str(path), SCENARIOS["control"], make_args(), FakeContext(),
                 STARTED, ENDED, {})

    assert "no fault" in json.dumps(json.loads(path.read_text())).lower()


def test_question_and_answer_agree_on_scope(tmp_path):
    """Grading compares the model's findings against the answer key. If the
    window or target drifts between the two files, the run evaluates a
    different slice of history than the model was shown."""
    args = make_args(host="ch-02", database="prod")
    question_path = tmp_path / "run.question.md"
    answer_path = tmp_path / "run.answer.json"

    write_question(str(question_path), SCENARIOS["pk-scan"], args, STARTED, ENDED)
    write_answer(str(answer_path), SCENARIOS["pk-scan"], args, FakeContext(),
                 STARTED, ENDED, {})

    question = question_path.read_text()
    answer = json.loads(answer_path.read_text())

    assert answer["started_at"] == STARTED and STARTED in question
    assert answer["ended_at"] == ENDED and ENDED in question
    assert answer["connection"]["host"] == args.host and args.host in question
    assert answer["targets"]["database"] == args.database and args.database in question
