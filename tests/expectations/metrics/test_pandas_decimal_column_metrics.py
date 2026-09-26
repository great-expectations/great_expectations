from __future__ import annotations

from decimal import Decimal

import pandas as pd
import pytest

import great_expectations as gx
import great_expectations.expectations as gxe


def _validate(dataframe: pd.DataFrame, expectation):
    context = gx.get_context(mode="ephemeral")
    batch_definition = (
        context.data_sources.add_pandas("pandas")
        .add_dataframe_asset("df")
        .add_batch_definition_whole_dataframe("all")
    )
    batch = batch_definition.get_batch(batch_parameters={"dataframe": dataframe})
    return batch.validate(expectation)


@pytest.mark.unit
@pytest.mark.parametrize(
    "expectation",
    [
        gxe.ExpectColumnMeanToBeBetween(column="amount", min_value=9, max_value=11),
        gxe.ExpectColumnSumToBeBetween(column="amount", min_value=29, max_value=31),
        gxe.ExpectColumnStdevToBeBetween(column="amount", min_value=4.9, max_value=5.1),
    ],
    ids=["mean", "sum", "stdev"],
)
def test_decimal_column_with_a_custom_index(expectation):
    dataframe = pd.DataFrame(
        {"amount": [Decimal("5"), Decimal("10"), Decimal("15")]}, index=[10, 11, 12]
    )

    result = _validate(dataframe, expectation)

    assert result.success, result.exception_info


@pytest.mark.unit
def test_decimal_column_filtered_by_a_row_condition():
    dataframe = pd.DataFrame(
        {
            "region": ["eu", "us", "eu", "eu"],
            "amount": [Decimal("5"), Decimal("7"), Decimal("1"), Decimal("2")],
        }
    )

    result = _validate(
        dataframe,
        gxe.ExpectColumnSumToBeBetween(
            column="amount",
            min_value=8,
            max_value=8,
            row_condition='region == "eu"',
            condition_parser="pandas",
        ),
    )

    assert result.success, result.exception_info
    assert result.result["observed_value"] == 8


@pytest.mark.unit
@pytest.mark.parametrize(
    "expectation",
    [
        gxe.ExpectColumnMeanToBeBetween(column="amount", min_value=9, max_value=11),
        gxe.ExpectColumnSumToBeBetween(column="amount", min_value=29, max_value=31),
        gxe.ExpectColumnStdevToBeBetween(column="amount", min_value=4.9, max_value=5.1),
    ],
    ids=["mean", "sum", "stdev"],
)
def test_validating_a_decimal_column_does_not_change_the_dataframe(expectation):
    dataframe = pd.DataFrame({"amount": [Decimal("5"), Decimal("10"), Decimal("15")]})

    result = _validate(dataframe, expectation)

    assert result.success, result.exception_info
    assert all(isinstance(value, Decimal) for value in dataframe["amount"])
