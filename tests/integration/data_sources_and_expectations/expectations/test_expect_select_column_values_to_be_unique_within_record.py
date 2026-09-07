import pandas as pd
import pytest

import great_expectations.expectations as gxe
from great_expectations.core.result_format import ResultFormat
from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.data_sources_and_expectations.data_source_lists import (
    JUST_PANDAS_DATA_SOURCES,
)
from tests.integration.test_utils.data_source_config import (
    ALL_DATA_SOURCES,
    PostgreSQLDatasourceTestConfig,
)

INT_COL_A = "INT_COL_A"
INT_COL_B = "INT_COL_B"
INT_COL_C = "INT_COL_C"
STRING_COL_A = "STRING_COL_A"
STRING_COL_B = "STRING_COL_B"


DATA = pd.DataFrame(
    {
        INT_COL_A: [1, 1, 2, 3],
        INT_COL_B: [2, 2, 3, 4],
        INT_COL_C: [3, 3, 4, 4],
        STRING_COL_A: ["a", "b", "c", "d"],
        STRING_COL_B: ["x", "y", "z", "a"],
    }
)


@parameterize_batch_for_data_sources(data_source_configs=ALL_DATA_SOURCES, data=DATA)
def test_golden_path(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectSelectColumnValuesToBeUniqueWithinRecord(
        column_list=[INT_COL_A, INT_COL_B]
    )
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectSelectColumnValuesToBeUniqueWithinRecord(
                column_list=[INT_COL_A, INT_COL_B, INT_COL_C], mostly=0.75
            ),
            id="mostly",
        ),
        pytest.param(
            gxe.ExpectSelectColumnValuesToBeUniqueWithinRecord(
                column_list=[STRING_COL_A, STRING_COL_B]
            ),
            id="strings_dont_error",
        ),
        pytest.param(
            gxe.ExpectSelectColumnValuesToBeUniqueWithinRecord(
                column_list=[STRING_COL_A, INT_COL_A]
            ),
            id="strings_and_ints",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success(
    batch_for_datasource: Batch, expectation: gxe.ExpectSelectColumnValuesToBeUniqueWithinRecord
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectSelectColumnValuesToBeUniqueWithinRecord(column_list=[INT_COL_B, INT_COL_C]),
            id="one_non_unique",
        ),
        pytest.param(
            gxe.ExpectSelectColumnValuesToBeUniqueWithinRecord(
                column_list=[INT_COL_A, INT_COL_B, INT_COL_C], mostly=0.8
            ),
            id="mostly_threshold_not_met",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_failure(
    batch_for_datasource: Batch, expectation: gxe.ExpectSelectColumnValuesToBeUniqueWithinRecord
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert not result.success


@pytest.mark.parametrize(
    "suite_param_value,expected_result",
    [
        pytest.param("all_values_are_missing", True, id="success"),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success_with_suite_param_ignore_row_if_(
    batch_for_datasource: Batch, suite_param_value: str, expected_result: bool
) -> None:
    suite_param_key = "test_expect_select_column_values_to_be_unique_within_record"
    expectation = gxe.ExpectSelectColumnValuesToBeUniqueWithinRecord(
        column_list=[INT_COL_A, INT_COL_B, INT_COL_C],
        mostly=0.75,
        ignore_row_if={"$PARAMETER": suite_param_key},
        result_format=ResultFormat.SUMMARY,
    )
    result = batch_for_datasource.validate(
        expectation, expectation_parameters={suite_param_key: suite_param_value}
    )
    assert result.success == expected_result


@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_include_unexpected_rows(batch_for_datasource: Batch) -> None:
    """Test include_unexpected_rows for ExpectSelectColumnValuesToBeUniqueWithinRecord."""
    expectation = gxe.ExpectSelectColumnValuesToBeUniqueWithinRecord(
        column_list=[INT_COL_A, INT_COL_B, INT_COL_C]
    )
    result = batch_for_datasource.validate(
        expectation, result_format={"result_format": "BASIC", "include_unexpected_rows": True}
    )

    assert not result.success
    result_dict = result["result"]

    # Verify that unexpected_rows is present and contains the expected data
    assert "unexpected_rows" in result_dict
    assert result_dict["unexpected_rows"] is not None

    # Convert to DataFrame for easier comparison
    unexpected_rows_data = result_dict["unexpected_rows"]
    assert isinstance(unexpected_rows_data, pd.DataFrame)
    unexpected_rows_df = unexpected_rows_data

    # Should contain rows that have non-unique values in the selected columns
    assert len(unexpected_rows_df) > 0

    # Check that the rows contain the expected columns
    assert INT_COL_A in unexpected_rows_df.columns
    assert INT_COL_B in unexpected_rows_df.columns
    assert INT_COL_C in unexpected_rows_df.columns


@parameterize_batch_for_data_sources(
    data_source_configs=[PostgreSQLDatasourceTestConfig()], data=DATA
)
def test_include_unexpected_rows_sql(batch_for_datasource: Batch) -> None:
    """Test include_unexpected_rows for ExpectSelectColumnValuesToBeUniqueWithinRecord with SQL."""
    expectation = gxe.ExpectSelectColumnValuesToBeUniqueWithinRecord(
        column_list=[INT_COL_A, INT_COL_B, INT_COL_C]
    )
    result = batch_for_datasource.validate(
        expectation, result_format={"result_format": "BASIC", "include_unexpected_rows": True}
    )

    assert not result.success
    result_dict = result["result"]

    # Verify that unexpected_rows is present and contains the expected data
    assert "unexpected_rows" in result_dict
    assert result_dict["unexpected_rows"] is not None

    # For SQL data sources, unexpected_rows should be a list
    unexpected_rows_data = result_dict["unexpected_rows"]
    assert isinstance(unexpected_rows_data, list)

    assert len(unexpected_rows_data) == 1

    unexpected_row = unexpected_rows_data[0]

    assert isinstance(unexpected_row, dict)

    unexpected_row_normalized = {str(k).upper(): v for k, v in unexpected_row.items()}

    expected = {
        "INT_COL_A": 3,
        "INT_COL_B": 4,
        "INT_COL_C": 4,
        "STRING_COL_A": "d",
        "STRING_COL_B": "a",
    }

    assert unexpected_row_normalized == expected
