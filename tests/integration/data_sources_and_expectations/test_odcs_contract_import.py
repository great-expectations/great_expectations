from __future__ import annotations

import pandas as pd
import pytest

import great_expectations as gx
from great_expectations.checkpoint.checkpoint import Checkpoint
from great_expectations.contracts import (
    DataContractValidationError,
    enforce_contract_gate,
    from_odcs,
)
from great_expectations.core.validation_definition import ValidationDefinition

pytestmark = pytest.mark.integration


def _contract(*, require_email: bool = True) -> dict:
    return {
        "version": "1.0.0",
        "apiVersion": "v3.1.0",
        "kind": "DataContract",
        "id": "11111111-1111-1111-1111-111111111111",
        "status": "active",
        "name": "orders_contract",
        "schema": [
            {
                "name": "orders",
                "logicalType": "object",
                "properties": [
                    {
                        "name": "id",
                        "logicalType": "integer",
                        "primaryKey": True,
                        "physicalType": "INTEGER",
                    },
                    {
                        "name": "email",
                        "logicalType": "string",
                        "required": require_email,
                        "logicalTypeOptions": {"format": "email"},
                    },
                    {
                        "name": "amount",
                        "logicalType": "number",
                        "logicalTypeOptions": {"minimum": 0},
                    },
                ],
                "quality": [{"metric": "rowCount", "mustBeBetween": [1, 10]}],
            }
        ],
    }


def _run_checkpoint(
    context,
    dataframe: pd.DataFrame,
    *,
    type_map: dict[str, str] | None = None,
    require_email: bool = True,
):
    datasource = context.data_sources.add_pandas("contract_pandas")
    asset = datasource.add_dataframe_asset("orders")
    batch_definition = asset.add_batch_definition_whole_dataframe("whole_orders")
    suite = context.suites.add(from_odcs(_contract(require_email=require_email), type_map=type_map))
    validation_definition = context.validation_definitions.add(
        ValidationDefinition(name="orders_contract", data=batch_definition, suite=suite)
    )
    checkpoint = context.checkpoints.add(
        Checkpoint(
            name="orders_contract_checkpoint",
            validation_definitions=[validation_definition],
        )
    )
    return checkpoint.run(batch_parameters={"dataframe": dataframe}), suite


def test_odcs_contract_passes_and_gate_allows_success() -> None:
    context = gx.get_context(mode="ephemeral")
    dataframe = pd.DataFrame(
        {
            "id": [1, 2],
            "email": ["first@example.com", "second@example.com"],
            "amount": [1.5, 2.5],
        }
    )

    result, suite = _run_checkpoint(context, dataframe)

    assert result.success
    assert enforce_contract_gate(result) is None
    assert context.suites.get(suite.name).name == suite.name


def test_odcs_contract_fails_and_gate_reports_the_real_batch_id() -> None:
    context = gx.get_context(mode="ephemeral")
    dataframe = pd.DataFrame(
        {
            "id": [1, 2],
            "email": ["first@example.com", "not-an-email"],
            "amount": [1.5, 2.5],
        }
    )

    result, _ = _run_checkpoint(context, dataframe)

    assert not result.success
    run_result = next(iter(result.run_results.values()))
    with pytest.raises(DataContractValidationError) as exc_info:
        enforce_contract_gate(result)

    assert run_result.batch_id is not None
    assert exc_info.value.failed_batch_ids == [run_result.batch_id]


def test_odcs_type_map_adds_a_backend_type_check() -> None:
    context = gx.get_context(mode="ephemeral")
    dataframe = pd.DataFrame(
        {
            "id": [1, 2],
            "email": ["first@example.com", "second@example.com"],
            "amount": [1.5, 2.5],
        }
    )

    result, _ = _run_checkpoint(context, dataframe, type_map={"integer": "int64"})

    assert result.success


def test_odcs_type_map_fails_when_values_do_not_match() -> None:
    context = gx.get_context(mode="ephemeral")
    dataframe = pd.DataFrame(
        {
            "id": ["1", "2"],
            "email": ["first@example.com", "second@example.com"],
            "amount": [1.5, 2.5],
        }
    )

    result, _ = _run_checkpoint(context, dataframe, type_map={"integer": "int64"})

    assert not result.success
    run_result = next(iter(result.run_results.values()))
    with pytest.raises(DataContractValidationError) as exc_info:
        enforce_contract_gate(result)

    assert run_result.batch_id is not None
    assert exc_info.value.failed_batch_ids == [run_result.batch_id]


def test_optional_email_with_null_passes_the_email_rule() -> None:
    context = gx.get_context(mode="ephemeral")
    dataframe = pd.DataFrame(
        {
            "id": [1, 2],
            "email": ["first@example.com", None],
            "amount": [1.5, 2.5],
        }
    )

    result, _ = _run_checkpoint(context, dataframe, require_email=False)

    assert result.success
