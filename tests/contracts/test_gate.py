from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

import great_expectations as gx
from great_expectations.contracts.gate import (
    DataContractValidationError,
    enforce_contract_gate,
    get_failed_batch_ids,
)

if TYPE_CHECKING:
    from pytest_mock import MockerFixture


@pytest.mark.unit
def test_get_failed_batch_ids_success(mocker: MockerFixture) -> None:
    mock_result = mocker.MagicMock()
    mock_result.success = True
    mock_result.run_results = {}

    batch_ids = get_failed_batch_ids(mock_result)
    assert batch_ids == []


@pytest.mark.unit
def test_get_failed_batch_ids_with_failures(mocker: MockerFixture) -> None:
    mock_result = mocker.MagicMock()
    mock_result.success = False

    val_res_1 = mocker.MagicMock()
    val_res_1.success = False
    val_res_1.meta = {"batch_id": "batch_abc"}

    val_res_2 = mocker.MagicMock()
    val_res_2.success = True
    val_res_2.meta = {"batch_id": "batch_xyz"}

    val_res_3 = mocker.MagicMock()
    val_res_3.success = False
    val_res_3.meta = {"batch_id": "batch_def"}

    mock_run_1 = mocker.MagicMock(validation_result=val_res_1)
    mock_run_2 = mocker.MagicMock(validation_result=val_res_2)
    mock_run_3 = mocker.MagicMock(validation_result=val_res_3)

    mock_result.run_results = {
        "res_1": mock_run_1,
        "res_2": mock_run_2,
        "res_3": mock_run_3,
    }

    batch_ids = get_failed_batch_ids(mock_result)
    assert batch_ids == ["batch_abc", "batch_def"]


@pytest.mark.unit
def test_get_failed_batch_ids_fallback_to_run_id(mocker: MockerFixture) -> None:
    mock_result = mocker.MagicMock()
    mock_result.success = False
    mock_result.run_id = "run_20261017"

    val_res = mocker.MagicMock()
    val_res.success = False
    val_res.meta = {}

    mock_run = mocker.MagicMock(validation_result=val_res)
    mock_result.run_results = {"res_1": mock_run}

    batch_ids = get_failed_batch_ids(mock_result)
    assert batch_ids == ["run_20261017"]


@pytest.mark.unit
def test_enforce_contract_gate_success(mocker: MockerFixture) -> None:
    mock_result = mocker.MagicMock()
    mock_result.success = True
    mock_result.run_results = {}

    quarantine_called = False

    def quarantine_handler(batch_ids: list[str]) -> None:
        nonlocal quarantine_called
        quarantine_called = True

    failed_ids = enforce_contract_gate(
        mock_result,
        raise_error=True,
        quarantine_callback=quarantine_handler,
    )
    assert failed_ids == []
    assert not quarantine_called


@pytest.mark.unit
def test_enforce_contract_gate_failure_raises_error(mocker: MockerFixture) -> None:
    mock_result = mocker.MagicMock()
    mock_result.success = False

    val_res = mocker.MagicMock()
    val_res.success = False
    val_res.meta = {"batch_id": "batch_bad_1"}

    mock_run = mocker.MagicMock(validation_result=val_res)
    mock_result.run_results = {"res": mock_run}

    quarantined: list[str] = []

    def quarantine_handler(batch_ids: list[str]) -> None:
        quarantined.extend(batch_ids)

    with pytest.raises(DataContractValidationError) as exc_info:
        enforce_contract_gate(
            mock_result,
            raise_error=True,
            quarantine_callback=quarantine_handler,
            error_message="Custom contract failure message",
        )

    assert "Custom contract failure message" in str(exc_info.value)
    assert exc_info.value.failed_batch_ids == ["batch_bad_1"]
    assert exc_info.value.result is mock_result
    assert quarantined == ["batch_bad_1"]


@pytest.mark.unit
def test_enforce_contract_gate_failure_no_raise(mocker: MockerFixture) -> None:
    mock_result = mocker.MagicMock()
    mock_result.success = False

    val_res = mocker.MagicMock()
    val_res.success = False
    val_res.meta = {"batch_id": "batch_bad_2"}

    mock_run = mocker.MagicMock(validation_result=val_res)
    mock_result.run_results = {"res": mock_run}

    failed_ids = enforce_contract_gate(
        mock_result,
        raise_error=False,
    )
    assert failed_ids == ["batch_bad_2"]


@pytest.mark.unit
def test_gate_module_accessible_via_gx_namespace() -> None:
    assert gx.contracts.enforce_contract_gate is enforce_contract_gate
    assert gx.contracts.DataContractValidationError is DataContractValidationError
    assert gx.contracts.get_failed_batch_ids is get_failed_batch_ids
