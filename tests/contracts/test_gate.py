from __future__ import annotations

import pickle

import pytest

from great_expectations.checkpoint import Checkpoint
from great_expectations.checkpoint.checkpoint import CheckpointResult
from great_expectations.contracts import DataContractValidationError, enforce_contract_gate
from great_expectations.core import ExpectationSuiteValidationResult, RunIdentifier
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)

pytestmark = pytest.mark.unit


def _validation_result(*, success: bool, batch_id: str | None) -> ExpectationSuiteValidationResult:
    return ExpectationSuiteValidationResult(
        success=success,
        statistics={
            "evaluated_expectations": 1,
            "successful_expectations": 1 if success else 0,
            "unsuccessful_expectations": 0 if success else 1,
            "success_percent": 100.0 if success else 0.0,
        },
        suite_name="contract_suite",
        results=[],
        batch_id=batch_id,
    )


def _checkpoint_result(runs: list[tuple[str, bool, str | None]]) -> CheckpointResult:
    run_results = {}
    for identifier_batch, success, result_batch in runs:
        identifier = ValidationResultIdentifier(
            expectation_suite_identifier=ExpectationSuiteIdentifier(name="contract_suite"),
            run_id=RunIdentifier(run_name="contract_run"),
            batch_identifier=identifier_batch,
        )
        run_results[identifier] = _validation_result(
            success=success,
            batch_id=result_batch,
        )
    return CheckpointResult(
        run_id=RunIdentifier(run_name="contract_run"),
        run_results=run_results,
        checkpoint_config=Checkpoint(name="contract_gate", validation_definitions=[]),
    )


def test_gate_returns_none_for_success() -> None:
    result = _checkpoint_result([("batch-a", True, "batch-a")])
    assert enforce_contract_gate(result) is None


def test_gate_raises_for_failure_with_real_batch_id() -> None:
    result = _checkpoint_result([("identifier-batch", False, "result-batch")])

    with pytest.raises(DataContractValidationError) as exc_info:
        enforce_contract_gate(result)

    assert exc_info.value.failed_batch_ids == ["result-batch"]
    assert exc_info.value.result is result
    assert "result-batch" in str(exc_info.value)


def test_gate_falls_back_to_result_identifier_batch_id() -> None:
    result = _checkpoint_result([("identifier-batch", False, None)])

    with pytest.raises(DataContractValidationError) as exc_info:
        enforce_contract_gate(result)

    assert exc_info.value.failed_batch_ids == ["identifier-batch"]


def test_gate_deduplicates_and_sorts_failed_batch_ids() -> None:
    result = _checkpoint_result(
        [
            ("batch-b", False, "batch-b"),
            ("batch-a", False, "batch-a"),
            ("batch-a-duplicate", False, "batch-a"),
            ("batch-c", True, "batch-c"),
        ]
    )

    with pytest.raises(DataContractValidationError) as exc_info:
        enforce_contract_gate(result)

    assert exc_info.value.failed_batch_ids == ["batch-a", "batch-b"]


def test_validation_error_is_picklable() -> None:
    result = _checkpoint_result([("batch-a", False, "batch-a")])
    with pytest.raises(DataContractValidationError) as exc_info:
        enforce_contract_gate(result)

    restored = pickle.loads(pickle.dumps(exc_info.value))

    assert restored.failed_batch_ids == ["batch-a"]
    assert restored.result.success is False
    assert "batch-a" in str(restored)


def test_gate_calls_failure_callback_before_raising() -> None:
    result = _checkpoint_result([("batch-a", False, "batch-a")])
    received: list[list[str]] = []

    def on_failure(batch_ids) -> None:
        received.append(list(batch_ids))

    with pytest.raises(DataContractValidationError):
        enforce_contract_gate(result, on_failure=on_failure)

    assert received == [["batch-a"]]
