from typing import Sequence, cast
from unittest.mock import ANY

import pandas as pd
import pytest

import great_expectations.expectations as gxe
from great_expectations.compatibility import pydantic
from great_expectations.core.result_format import ResultFormat
from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.data_sources_and_expectations.data_source_lists import (
    JUST_PANDAS_DATA_SOURCES,
)
from tests.integration.test_utils.data_source_config import (
    BigQueryDatasourceTestConfig,
    GenericSQLDatasourceTestConfig,
    MySQLDatasourceTestConfig,
    PostgreSQLDatasourceTestConfig,
    RedshiftDatasourceTestConfig,
    SparkFilesystemCsvDatasourceTestConfig,
)
from tests.integration.test_utils.data_source_config.base import DataSourceTestConfig
from tests.integration.test_utils.data_source_config.sqlite import SqliteDatasourceTestConfig

SUPPORTED_SQL_DATA_SOURCES: Sequence[DataSourceTestConfig] = [
    BigQueryDatasourceTestConfig(),
    MySQLDatasourceTestConfig(),
    PostgreSQLDatasourceTestConfig(),
    RedshiftDatasourceTestConfig(),
    GenericSQLDatasourceTestConfig(),
    SqliteDatasourceTestConfig(),
]
SUPPORTED_NON_SQL_DATA_SOURCES: Sequence[DataSourceTestConfig] = [
    SparkFilesystemCsvDatasourceTestConfig()
]
ALL_SUPPORTED_DATA_SOURCES: Sequence[DataSourceTestConfig] = [
    *SUPPORTED_SQL_DATA_SOURCES,
    *SUPPORTED_NON_SQL_DATA_SOURCES,
]

BASIC_STRINGS = "basic_strings"
COMPLEX_STRINGS = "complex_strings"
WITH_NULL = "with_null"

DATA = pd.DataFrame(
    {
        BASIC_STRINGS: ["abc", "def", "ghi"],
        COMPLEX_STRINGS: ["a1b2", "cccc", "123"],
        WITH_NULL: ["abc", None, "ghi"],
    }
)


@parameterize_batch_for_data_sources(data_source_configs=SUPPORTED_SQL_DATA_SOURCES, data=DATA)
def test_basic_success(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValuesToMatchRegexList(
        column=BASIC_STRINGS,
        regex_list=["^[a-z]{3}$"],
    )
    result = batch_for_datasource.validate(expectation)
    assert result.success


@parameterize_batch_for_data_sources(data_source_configs=SUPPORTED_SQL_DATA_SOURCES, data=DATA)
def test_basic_failure(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValuesToMatchRegexList(
        column=BASIC_STRINGS,
        regex_list=["^xyz.*"],
    )
    result = batch_for_datasource.validate(expectation)
    assert not result.success


@parameterize_batch_for_data_sources(
    data_source_configs=[PostgreSQLDatasourceTestConfig(), RedshiftDatasourceTestConfig()],
    data=DATA,
)
def test_postgresql_complete_results_failure(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValuesToMatchRegexList(
        column=BASIC_STRINGS,
        regex_list=["^xyz.*"],
    )
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    json_dict = result.to_json_dict()
    result_dict = json_dict.get("result")

    assert isinstance(result_dict, dict)
    assert not result.success
    assert "WHERE basic_strings IS NOT NULL AND NOT (basic_strings ~ '^xyz.*')" in cast(
        "str", result_dict.get("unexpected_index_query")
    )
    assert result_dict == {
        "element_count": 3,
        "unexpected_count": 3,
        "unexpected_percent": 100.0,
        "partial_unexpected_list": ["abc", "def", "ghi"],
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_percent_total": 100.0,
        "unexpected_percent_nonmissing": 100.0,
        "partial_unexpected_counts": [
            {"value": "abc", "count": 1},
            {"value": "def", "count": 1},
            {"value": "ghi", "count": 1},
        ],
        "unexpected_list": ["abc", "def", "ghi"],
        "unexpected_index_query": ANY,
    }


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnValuesToMatchRegexList(
                column=BASIC_STRINGS,
                regex_list=["[a-z]*"],
            ),
            id="match_all",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToMatchRegexList(
                column=BASIC_STRINGS,
                regex_list=["a.+", "d.+", "g.+"],
                match_on="any",
            ),
            id="match_any_patterns",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToMatchRegexList(
                column=BASIC_STRINGS,
                regex_list=["^[a-z]{3}$"],
                match_on="all",
            ),
            id="match_on_all",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToMatchRegexList(
                column=COMPLEX_STRINGS,
                regex_list=["^[a-z0-9]+$"],
            ),
            id="alphanumeric_regex",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToMatchRegexList(
                column=WITH_NULL,
                regex_list=["^abc$"],
                mostly=0.3,
            ),
            id="mostly_with_null",
        ),
    ],
)
@parameterize_batch_for_data_sources(
    data_source_configs=[PostgreSQLDatasourceTestConfig(), RedshiftDatasourceTestConfig()],
    data=DATA,
)
def test_success(
    batch_for_datasource: Batch,
    expectation: gxe.ExpectColumnValuesToMatchRegexList,
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnValuesToMatchRegexList(
                column=BASIC_STRINGS,
                regex_list=["^xyz.*"],
            ),
            id="no_matches",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToMatchRegexList(
                column=BASIC_STRINGS,
                regex_list=["a.+", "d.+", "g.+"],
                match_on="all",
            ),
            id="match_all_patterns",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToMatchRegexList(
                column=WITH_NULL,
                regex_list=["^abc$"],
                mostly=0.9,
            ),
            id="mostly_threshold_not_met",
        ),
    ],
)
@parameterize_batch_for_data_sources(
    data_source_configs=[PostgreSQLDatasourceTestConfig(), RedshiftDatasourceTestConfig()],
    data=DATA,
)
def test_failure(
    batch_for_datasource: Batch,
    expectation: gxe.ExpectColumnValuesToMatchRegexList,
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert not result.success


@pytest.mark.parametrize(
    "suite_param_value,expected_result",
    [
        pytest.param("any", True, id="success"),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success_with_suite_param_match_on_(
    batch_for_datasource: Batch, suite_param_value: str, expected_result: bool
) -> None:
    suite_param_key = "test_expect_column_values_to_match_regex_list"

    expectation = gxe.ExpectColumnValuesToMatchRegexList(
        column=BASIC_STRINGS,
        regex_list=["a.+", "d.+", "g.+"],
        match_on={"$PARAMETER": suite_param_key},
        result_format=ResultFormat.SUMMARY,
    )
    result = batch_for_datasource.validate(
        expectation, expectation_parameters={suite_param_key: suite_param_value}
    )
    assert result.success == expected_result


@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_include_unexpected_rows_pandas(batch_for_datasource: Batch) -> None:
    """Test that include_unexpected_rows works correctly for ExpectColumnValuesToMatchRegexList."""
    expectation = gxe.ExpectColumnValuesToMatchRegexList(
        column=BASIC_STRINGS, regex_list=["^xyz.*"]
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

    # Should contain 3 rows where BASIC_STRINGS doesn't match regex_list ["^xyz.*"] (none match)
    assert len(unexpected_rows_df) == 3

    # The unexpected rows should contain all the non-matching values
    unexpected_values = sorted(unexpected_rows_df[BASIC_STRINGS].tolist())
    assert unexpected_values == ["abc", "def", "ghi"]


@parameterize_batch_for_data_sources(
    data_source_configs=[PostgreSQLDatasourceTestConfig()], data=DATA
)
def test_include_unexpected_rows_sql(batch_for_datasource: Batch) -> None:
    """Test include_unexpected_rows for ExpectColumnValuesToMatchRegexList with SQL data sources."""
    expectation = gxe.ExpectColumnValuesToMatchRegexList(
        column=BASIC_STRINGS, regex_list=["^xyz.*"]
    )
    result = batch_for_datasource.validate(
        expectation, result_format={"result_format": "BASIC", "include_unexpected_rows": True}
    )

    assert not result.success
    result_dict = result["result"]

    # Verify that unexpected_rows is present and contains the expected data
    assert "unexpected_rows" in result_dict
    assert result_dict["unexpected_rows"] is not None

    unexpected_rows_data = result_dict["unexpected_rows"]
    assert isinstance(unexpected_rows_data, list)

    # Should contain 3 rows where BASIC_STRINGS doesn't match regex_list ["^xyz.*"] (none match)
    assert len(unexpected_rows_data) == 3

    # Check that all non-matching values appear in the unexpected rows data
    unexpected_rows_str = str(unexpected_rows_data)
    for value in ["abc", "def", "ghi"]:
        assert value in unexpected_rows_str


@pytest.mark.unit
def test_invalid_config() -> None:
    with pytest.raises(pydantic.ValidationError):
        gxe.ExpectColumnValuesToMatchRegexList(column=BASIC_STRINGS, regex_list=[])
