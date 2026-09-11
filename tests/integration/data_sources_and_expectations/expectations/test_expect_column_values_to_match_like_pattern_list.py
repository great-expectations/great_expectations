from typing import Sequence, cast
from unittest.mock import ANY

import pandas as pd
import pytest

import great_expectations.expectations as gxe
from great_expectations.core.result_format import ResultFormat
from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.test_utils.data_source_config import (
    BigQueryDatasourceTestConfig,
    DataSourceTestConfig,
    GenericSQLDatasourceTestConfig,
    MySQLDatasourceTestConfig,
    PostgreSQLDatasourceTestConfig,
    RedshiftDatasourceTestConfig,
    SnowflakeDatasourceTestConfig,
    SqliteDatasourceTestConfig,
    SQLServerDatasourceTestConfig,
)

BASIC_PATTERNS = "basic_patterns"
PREFIXED_PATTERNS = "prefixed_patterns"
SUFFIXED_PATTERNS = "suffixed_patterns"
COMMON_PATTERN = "suffixed_patterns"
WITH_NULL = "with_null"
WILDCARD_LITERALS = "wildcard_literals"

DATA = pd.DataFrame(
    {
        BASIC_PATTERNS: ["abc", "def", "ghi"],
        PREFIXED_PATTERNS: ["foo_abc", "foo_def", "foo_ghi"],
        SUFFIXED_PATTERNS: ["abc_foo", "def_foo", "ghi_foo"],
        COMMON_PATTERN: ["abc_foo", "def_foo", "ghi_foo"],
        WITH_NULL: ["ba", None, "ab"],
        # One row holds a literal underscore, one a literal percent, one neither.
        WILDCARD_LITERALS: ["a_b", "axb", "a%b"],
    }
)

SUPPORTED_DATA_SOURCES: Sequence[DataSourceTestConfig] = [
    BigQueryDatasourceTestConfig(),
    SQLServerDatasourceTestConfig(),
    MySQLDatasourceTestConfig(),
    PostgreSQLDatasourceTestConfig(),
    RedshiftDatasourceTestConfig(),
    GenericSQLDatasourceTestConfig(),
    SnowflakeDatasourceTestConfig(),
    SqliteDatasourceTestConfig(),
]


@parameterize_batch_for_data_sources(data_source_configs=SUPPORTED_DATA_SOURCES, data=DATA)
def test_basic_success(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValuesToMatchLikePatternList(
        column=PREFIXED_PATTERNS,
        like_pattern_list=["foo%"],
    )
    result = batch_for_datasource.validate(expectation)
    assert result.success


@parameterize_batch_for_data_sources(data_source_configs=SUPPORTED_DATA_SOURCES, data=DATA)
def test_basic_failure(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValuesToMatchLikePatternList(
        column=BASIC_PATTERNS,
        like_pattern_list=["xyz%"],
    )
    result = batch_for_datasource.validate(expectation)
    assert not result.success


@parameterize_batch_for_data_sources(
    data_source_configs=[PostgreSQLDatasourceTestConfig(), RedshiftDatasourceTestConfig()],
    data=DATA,
)
def test_complete_results_failure(batch_for_datasource: Batch) -> None:
    ABOUT_TWO_THIRDS = pytest.approx(2 / 3 * 100)
    expectation = gxe.ExpectColumnValuesToMatchLikePatternList(
        column=BASIC_PATTERNS,
        like_pattern_list=["%b%"],
    )
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    json_dict = result.to_json_dict()
    result_dict = json_dict.get("result")

    assert isinstance(result_dict, dict)
    assert not result.success
    assert "IS NOT NULL AND basic_patterns NOT LIKE '%b%'" in cast(
        "str", result_dict.get("unexpected_index_query")
    )
    assert result.to_json_dict().get("result") == {
        "element_count": 3,
        "unexpected_count": 2,
        "unexpected_percent": ABOUT_TWO_THIRDS,
        "partial_unexpected_list": ["def", "ghi"],
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_percent_total": ABOUT_TWO_THIRDS,
        "unexpected_percent_nonmissing": ABOUT_TWO_THIRDS,
        "partial_unexpected_counts": [
            {"value": "def", "count": 1},
            {"value": "ghi", "count": 1},
        ],
        "unexpected_list": ["def", "ghi"],
        "unexpected_index_query": ANY,
    }


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnValuesToMatchLikePatternList(
                column=COMMON_PATTERN, like_pattern_list=["%foo"], match_on="any"
            ),
            id="match_all",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToMatchLikePatternList(
                column=BASIC_PATTERNS, like_pattern_list=["abc", "def", "ghi"], match_on="any"
            ),
            id="multiple_patterns",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToMatchLikePatternList(
                column=BASIC_PATTERNS, like_pattern_list=["a%", "d%", "g%"], match_on="any"
            ),
            id="multiple_patterns_with_prefix",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToMatchLikePatternList(
                column=COMMON_PATTERN, like_pattern_list=["%foo"], match_on="any"
            ),
            id="match_all",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToMatchLikePatternList(
                column=PREFIXED_PATTERNS, like_pattern_list=["foo%"], match_on="any"
            ),
            id="prefixed_pattern",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToMatchLikePatternList(
                column=SUFFIXED_PATTERNS, like_pattern_list=["%foo"], match_on="any"
            ),
            id="suffixed_pattern",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToMatchLikePatternList(
                column=PREFIXED_PATTERNS,
                like_pattern_list=["foo%", "bar%"],
                match_on="any",
            ),
            id="matches_one_of",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToMatchLikePatternList(
                column=BASIC_PATTERNS, like_pattern_list=["%b%"], match_on="any", mostly=0.3
            ),
            id="mostly",
        ),
    ],
)
@parameterize_batch_for_data_sources(
    data_source_configs=[PostgreSQLDatasourceTestConfig(), RedshiftDatasourceTestConfig()],
    data=DATA,
)
def test_success(
    batch_for_datasource: Batch,
    expectation: gxe.ExpectColumnValuesToMatchLikePatternList,
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnValuesToMatchLikePatternList(
                column=BASIC_PATTERNS,
                like_pattern_list=["%xyz%"],
                match_on="any",
            ),
            id="no_matches",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToMatchLikePatternList(
                column=BASIC_PATTERNS,
                like_pattern_list=["%b%"],
                match_on="any",
                mostly=0.4,
            ),
            id="mostly_threshold_not_met",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToMatchLikePatternList(
                column=PREFIXED_PATTERNS,
                like_pattern_list=["foo%", "bar%"],
                match_on="all",
            ),
            id="does_not_match_all",
        ),
    ],
)
@parameterize_batch_for_data_sources(
    data_source_configs=[PostgreSQLDatasourceTestConfig(), RedshiftDatasourceTestConfig()],
    data=DATA,
)
def test_failure(
    batch_for_datasource: Batch,
    expectation: gxe.ExpectColumnValuesToMatchLikePatternList,
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert not result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnValuesToMatchLikePatternList(
                column=BASIC_PATTERNS,
                like_pattern_list=["[adg]%"],
                match_on="any",
            ),
        ),
        pytest.param(
            gxe.ExpectColumnValuesToMatchLikePatternList(
                column=BASIC_PATTERNS,
                like_pattern_list=["[a]%", "[d]%", "[g]%"],
                match_on="any",
            ),
        ),
    ],
)
@parameterize_batch_for_data_sources(
    data_source_configs=[SQLServerDatasourceTestConfig()], data=DATA
)
def test_msql_fancy_syntax(
    batch_for_datasource: Batch,
    expectation: gxe.ExpectColumnValuesToMatchLikePatternList,
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "suite_param_value,expected_result",
    [
        pytest.param("any", True, id="success"),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=SUPPORTED_DATA_SOURCES, data=DATA)
def test_success_with_suite_param_match_on_(
    batch_for_datasource: Batch, suite_param_value: str, expected_result: bool
) -> None:
    suite_param_key = "test_expect_column_values_to_match_like_pattern_list"

    expectation = gxe.ExpectColumnValuesToMatchLikePatternList(
        column=BASIC_PATTERNS,
        like_pattern_list=["abc", "def", "ghi"],
        match_on={"$PARAMETER": suite_param_key},
        result_format=ResultFormat.SUMMARY,
    )
    result = batch_for_datasource.validate(
        expectation, expectation_parameters={suite_param_key: suite_param_value}
    )
    assert result.success == expected_result


@parameterize_batch_for_data_sources(
    data_source_configs=[PostgreSQLDatasourceTestConfig()], data=DATA
)
def test_include_unexpected_rows_postgres(batch_for_datasource: Batch) -> None:
    """Test include_unexpected_rows for ExpectColumnValuesToMatchLikePatternList."""
    expectation = gxe.ExpectColumnValuesToMatchLikePatternList(
        column=BASIC_PATTERNS, like_pattern_list=["%xyz%"]
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

    # Should contain 3 rows where BASIC_PATTERNS doesn't match like_pattern_list ["%xyz%"]
    # (none match)
    assert len(unexpected_rows_data) == 3

    # Check that "abc", "def", and "ghi" appear in the unexpected rows data
    unexpected_rows_str = str(unexpected_rows_data)
    assert "abc" in unexpected_rows_str
    assert "def" in unexpected_rows_str
    assert "ghi" in unexpected_rows_str


# BigQuery is excluded: GoogleSQL has no ESCAPE clause, asserted separately as a unit test.
ESCAPE_DATA_SOURCES: Sequence[DataSourceTestConfig] = [
    config
    for config in SUPPORTED_DATA_SOURCES
    if not isinstance(config, BigQueryDatasourceTestConfig)
]


@parameterize_batch_for_data_sources(data_source_configs=ESCAPE_DATA_SOURCES, data=DATA)
def test_escape_applies_to_every_pattern_in_the_list(batch_for_datasource: Batch) -> None:
    """One escape character governs the whole list, not just the first pattern.

    Both patterns name a literal wildcard character, so together they match the two rows
    that contain one, leaving only the row with neither as unexpected. Were the escape
    dropped for the trailing patterns, 'axb' would match as well and nothing would be
    reported.
    """
    expectation = gxe.ExpectColumnValuesToMatchLikePatternList(
        column=WILDCARD_LITERALS,
        like_pattern_list=["a!_b", "a!%b"],
        match_on="any",
        escape="!",
    )
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)

    assert not result.success
    assert result.result["unexpected_list"] == ["axb"]
