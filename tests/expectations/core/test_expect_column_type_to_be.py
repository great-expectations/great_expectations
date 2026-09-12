import numpy as np
import pandas as pd
import pytest

import great_expectations.expectations as gxe
from great_expectations.expectations.core.expect_column_type_to_be import (
    ExpectColumnTypeToBe,
)
from great_expectations.self_check.util import build_sa_validator_with_data


@pytest.mark.unit
def test_registered_and_uses_table_column_types_metric():
    assert gxe.ExpectColumnTypeToBe is ExpectColumnTypeToBe
    assert ExpectColumnTypeToBe.metric_dependencies == ("table.column_types",)
    assert set(ExpectColumnTypeToBe.success_keys) == {"column", "type_"}
    assert ExpectColumnTypeToBe.args_keys == ("column", "type_")


@pytest.mark.unit
def test_validate_pandas_success_and_observed_value():
    expectation = ExpectColumnTypeToBe(column="a", type_="int64")
    result = expectation._validate_pandas(
        actual_column_type=np.dtype("int64"), expected_type="int64"
    )
    assert result["success"] is True
    assert result["result"] == {"observed_value": "int64"}


@pytest.mark.unit
def test_validate_pandas_failure_reports_observed_value():
    expectation = ExpectColumnTypeToBe(column="a", type_="int64")
    result = expectation._validate_pandas(
        actual_column_type=np.dtype("float64"), expected_type="int64"
    )
    assert result["success"] is False
    assert result["result"] == {"observed_value": "float64"}


@pytest.mark.unit
def test_validate_missing_column_fails_with_null_observed_value():
    expectation = ExpectColumnTypeToBe(column="missing", type_="INTEGER")
    result = expectation._validate(metrics={"table.column_types": []})
    assert result == {"success": False, "result": {"observed_value": None}}


@pytest.mark.unit
def test_result_has_no_row_level_fields():
    expectation = ExpectColumnTypeToBe(column="a", type_="int64")
    result = expectation._validate_pandas(
        actual_column_type=np.dtype("int64"), expected_type="int64"
    )
    assert set(result["result"]) == {"observed_value"}
    assert "mostly" not in expectation.success_keys


@pytest.mark.sqlite
def test_delegates_to_compare_column_type(sa, mocker):
    df = pd.DataFrame({"str_col": ["a", "b", "c"]})
    validator = build_sa_validator_with_data(
        df=df, sa_engine_name="sqlite", table_name="column_type_to_be_wiring"
    )

    mock_compare = mocker.patch(
        "great_expectations.expectations.core.expect_column_type_to_be.compare_column_type",
        return_value=(True, "SENTINEL_TYPE"),
    )

    result = validator.expect_column_type_to_be("str_col", type_="TEXT")

    mock_compare.assert_called_once_with(validator.execution_engine, mocker.ANY, "TEXT")
    assert result.success is True
    assert result.result["observed_value"] == "SENTINEL_TYPE"


@pytest.mark.sqlite
def test_sqlite_end_to_end_success_and_failure(sa):
    df = pd.DataFrame({"col": ["test_val1", "test_val2"]})
    validator = build_sa_validator_with_data(
        df=df,
        sa_engine_name="sqlite",
        table_name="expect_column_type_to_be_sqlite_e2e",
    )

    success_result = validator.expect_column_type_to_be("col", type_="TEXT")
    assert success_result.success is True
    assert success_result.result["observed_value"] == "TEXT"

    failure_result = validator.expect_column_type_to_be("col", type_="INTEGER")
    assert failure_result.success is False
