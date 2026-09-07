from enum import Enum
from typing import Dict, List, Optional, Sequence

import pandas as pd
import pytest

import great_expectations.expectations as gxe
from great_expectations.core.expectation_validation_result import ExpectationValidationResult
from great_expectations.core.result_format import ResultFormat
from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.data_sources_and_expectations.data_source_lists import (
    JUST_PANDAS_DATA_SOURCES,
)
from tests.integration.test_utils.data_source_config import (
    ALL_DATA_SOURCES,
    SQL_DATA_SOURCES,
    BigQueryDatasourceTestConfig,
    DatabricksDatasourceTestConfig,
    DataSourceTestConfig,
    GenericSQLDatasourceTestConfig,
    MySQLDatasourceTestConfig,
    PostgreSQLDatasourceTestConfig,
    RedshiftDatasourceTestConfig,
    SnowflakeDatasourceTestConfig,
    SqliteDatasourceTestConfig,
    SQLServerDatasourceTestConfig,
)

COL_A = "col_a"
COL_B = "col_b"
COL_C = "col_c"


DATA = pd.DataFrame(
    {
        COL_A: [1],
        COL_B: [2],
        COL_C: [3],
    }
)


@parameterize_batch_for_data_sources(data_source_configs=ALL_DATA_SOURCES, data=DATA)
def test_golden_path(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectTableColumnsToMatchSet(column_set=[COL_A, COL_B, COL_C])
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectTableColumnsToMatchSet(column_set=[COL_C, COL_A, COL_B]),
            id="order_doesnt_matter",
        ),
        pytest.param(
            gxe.ExpectTableColumnsToMatchSet(column_set=[COL_A, COL_B], exact_match=False),
            id="allows_subset_for_non_exact_match",
        ),
        pytest.param(
            gxe.ExpectTableColumnsToMatchSet(column_set=[COL_A, COL_B, COL_C], exact_match=False),
            id="allows_all_for_non_exact_match",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success(
    batch_for_datasource: Batch, expectation: gxe.ExpectTableColumnsToMatchSet
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectTableColumnsToMatchSet(column_set=[COL_A, COL_B], exact_match=True),
            id="requires_all_columns_for_exact_match",
        ),
        pytest.param(
            gxe.ExpectTableColumnsToMatchSet(column_set=[COL_A]),
            id="defaults_to_exact_match",
        ),
        pytest.param(
            gxe.ExpectTableColumnsToMatchSet(
                column_set=[COL_A, COL_B, COL_C, "col_d"], exact_match=True
            ),
            id="does_not_allow_extras",
        ),
        pytest.param(
            gxe.ExpectTableColumnsToMatchSet(
                column_set=[COL_A, COL_B, COL_C, "col_d"], exact_match=False
            ),
            id="does_not_allow_extrasfor_non_exact_match",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_failure(
    batch_for_datasource: Batch, expectation: gxe.ExpectTableColumnsToMatchSet
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert not result.success


@pytest.mark.parametrize(
    "suite_param_value,expected_result",
    [
        pytest.param(False, True, id="success"),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success_with_suite_param_exact_match_(
    batch_for_datasource: Batch, suite_param_value: str, expected_result: bool
) -> None:
    suite_param_key = "test_expect_table_columns_to_match_set"
    expectation = gxe.ExpectTableColumnsToMatchSet(
        column_set=[COL_A, COL_B],
        exact_match={"$PARAMETER": suite_param_key},
        result_format=ResultFormat.SUMMARY,
    )
    result = batch_for_datasource.validate(
        expectation, expectation_parameters={suite_param_key: suite_param_value}
    )
    assert result.success == expected_result


# Case insenstivity tests
# For some of our case insensitive tests we break out Snowflake and Redshift into separate
# tests for the following reasons:
##### Snowflake #####
# In test setup, sqlalchemy's CREATE TABLE will not quote lowercase column names
# but will quote uppercase. Since snowflake stores case insensitive strings
# as uppercase, while all the other databases we are testing stores them as lowercase
# this creates different case insensitive columns. We break out the snowflake tests
# into their own tests.
#
#### Redshift ####
# Redshift by default is case insensitive so trying to match case via quoted identifiers
# fails. There is a global setting you can set on a redshift cluster,
# enable_case_sensitive_identifier, one can apply to make it case sensitive. Our code
# looks like it would handle this successfully but I haven't verified since I need to
# change our redshift CI cluster. There is tracked in GX-1197.

CASE_INSENSITIVE_DATA = pd.DataFrame(
    {
        "column_a": [1],
        "COLUMN_B": [2],
        "CoLuMn_C": [3],
    }
)


def observed_column_names(datasource_type: str) -> List[str]:
    """Returns observed column name base on type in A, B, C order."""
    if datasource_type == "snowflake":
        # Since SQLAlchemy doesn't quote lowercase names in CREATE TABLE and since uppercase is
        # case insenstive in Snowflake, both column_a and column_b will be case insensitive.
        # SQLAlchemy will return case insensitive names in lowercase when inspecting the table.
        return [
            "column_a",
            "column_b",
            "CoLuMn_C",
        ]
    elif datasource_type == "redshift":
        # Redshift is case insensitive by default
        # SQLAlchemy will return case insensitive names in lowercase when inspecting the table.
        return [
            "column_a",
            "column_b",
            "column_c",
        ]
    else:
        # For most datasources we expected the observed column names to match CASE_INSENSITIVE_DATA.
        return [
            "column_a",
            "COLUMN_B",
            "CoLuMn_C",
        ]


SQL_DATA_SOURCES_WITHOUT_SNOWFLAKE_REDSHIFT: Sequence[DataSourceTestConfig] = [
    BigQueryDatasourceTestConfig(),
    DatabricksDatasourceTestConfig(),
    SQLServerDatasourceTestConfig(),
    MySQLDatasourceTestConfig(),
    PostgreSQLDatasourceTestConfig(),
    GenericSQLDatasourceTestConfig(),
    SqliteDatasourceTestConfig(),
]


@pytest.mark.unit
def test_sql_data_sources_without_snowflake_redshift() -> None:
    """`SQL_DATA_SOURCES_WITHOUT_SNOWFLAKE_REDSHIFT` is what its name says it is.

    Stated as a set difference in both directions rather than as `len(...) + 2 == len(...)`, which
    is what this assertion used to be. The count held only while `SQL_DATA_SOURCES` was the
    hand-written nine that included the generic SQL escape hatch; that name now resolves to the
    derived eight, which does not, and a count cannot express "two fewer, plus the escape hatch
    this module keeps on purpose". A count also could not have told a swapped backend from an
    unchanged list — it passes on any two entries leaving and two arriving.

    This list keeps `GenericSQLDatasourceTestConfig`. It is module-private, parameterizes only this
    module's tests, and is not one of the derived lists that gate CI, so the reason the escape hatch
    left those does not reach it.
    """
    local = set(SQL_DATA_SOURCES_WITHOUT_SNOWFLAKE_REDSHIFT)
    shared = set(SQL_DATA_SOURCES)

    assert shared - local == {SnowflakeDatasourceTestConfig(), RedshiftDatasourceTestConfig()}
    assert local - shared == {GenericSQLDatasourceTestConfig()}


@parameterize_batch_for_data_sources(
    data_source_configs=SQL_DATA_SOURCES,
    data=CASE_INSENSITIVE_DATA,
)
def test_case_insensitive_success(batch_for_datasource: Batch) -> None:
    # Arrange
    expectation = gxe.ExpectTableColumnsToMatchSet(column_set=["COLUMN_A", "column_b", "COLumN_c"])

    # Act
    result = batch_for_datasource.validate(expectation)
    result.render()  # creates rendered content

    # Assert
    assert result.success

    # Assert rendered content is as expected
    observed_values = _extract_observed_state(result)
    assert observed_values == dict(
        zip(
            observed_column_names(batch_for_datasource.datasource.type),
            ["expected", "expected", "expected"],
            strict=False,
        )
    )
    expected_values = _extract_expected_state(result)
    assert expected_values == {"COLUMN_A": None, "column_b": None, "COLumN_c": None}


@parameterize_batch_for_data_sources(
    data_source_configs=SQL_DATA_SOURCES,
    data=CASE_INSENSITIVE_DATA,
)
def test_case_insensitive_failure(batch_for_datasource: Batch) -> None:
    # Arrange
    expectation = gxe.ExpectTableColumnsToMatchSet(column_set=["COLUMN_Az", "column_b", "COLumN_c"])

    # Act
    result = batch_for_datasource.validate(expectation)
    result.render()  # creates rendered content

    # Assert
    assert not result.success

    # Assert rendered content is as expected
    observed_values = _extract_observed_state(result)
    assert observed_values == dict(
        zip(
            observed_column_names(batch_for_datasource.datasource.type),
            ["unexpected", "expected", "expected"],
            strict=False,
        )
    )
    expected_values = _extract_expected_state(result)
    assert expected_values == {"COLUMN_Az": "missing", "column_b": None, "COLumN_c": None}


def _extract_observed_state(result: ExpectationValidationResult) -> Dict[str, str]:
    """Extracts observed column name to rendered state from validation result"""
    # ExpectationValidationResult is not narrowly typed so we need to a lot of
    # isinstance assertions for our expectation.
    assert isinstance(result.rendered_content, list)
    assert len(result.rendered_content) == 1
    value = result.rendered_content[0].to_json_dict()["value"]
    assert isinstance(value, dict)
    rendered_params = value["params"]
    assert isinstance(rendered_params, dict)
    # We don't use a comprehension because we need to do type assertions with
    # isinstance calls.
    observed_state: Dict[str, str] = {}
    for k, v in rendered_params.items():
        assert isinstance(v, dict)
        if k.startswith("ov__"):
            assert isinstance(v["value"], str)
            assert isinstance(v["render_state"], Enum)
            observed_state[v["value"]] = v["render_state"].value
    return observed_state


def _extract_expected_state(result: ExpectationValidationResult) -> Dict[str, Optional[str]]:
    """Extracts expected column name to rendered state from validation result"""
    # ExpectationValidationResult is not narrowly typed so we need to a lot of
    # isinstance assertions for our expectation.
    assert isinstance(result.rendered_content, list)
    assert len(result.rendered_content) == 1
    value = result.rendered_content[0].to_json_dict()["value"]
    assert isinstance(value, dict)
    rendered_params = value["params"]
    assert isinstance(rendered_params, dict)
    # We don't use a comprehension because we need to do type assertions with
    # isinstance calls.
    expected_state: Dict[str, Optional[str]] = {}
    for k, v in rendered_params.items():
        assert isinstance(v, dict)
        if k.startswith("exp__"):
            assert isinstance(v["value"], str)
            if "render_state" in v and isinstance(v["render_state"], Enum):
                expected_state[v["value"]] = v["render_state"].value
            else:
                expected_state[v["value"]] = None
    return expected_state


@parameterize_batch_for_data_sources(
    data_source_configs=SQL_DATA_SOURCES_WITHOUT_SNOWFLAKE_REDSHIFT,
    data=CASE_INSENSITIVE_DATA,
)
def test_quoted_success(batch_for_datasource: Batch) -> None:
    # When we create the table in the testing fr
    expectation = gxe.ExpectTableColumnsToMatchSet(
        column_set=['"column_a"', '"COLUMN_B"', '"CoLuMn_C"']
    )
    result = batch_for_datasource.validate(expectation)
    assert result.success


@parameterize_batch_for_data_sources(
    data_source_configs=[SnowflakeDatasourceTestConfig()],
    data=CASE_INSENSITIVE_DATA,
)
def test_quoted_success_snowflake(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectTableColumnsToMatchSet(
        column_set=['"column_a"', '"CoLuMn_C"'],
        exact_match=False,
    )
    result = batch_for_datasource.validate(expectation)
    assert result.success


@parameterize_batch_for_data_sources(
    data_source_configs=SQL_DATA_SOURCES,
    data=CASE_INSENSITIVE_DATA,
)
def test_quoted_failure(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectTableColumnsToMatchSet(
        column_set=['"Column_a"', '"COLUMN_B"', '"CoLuMn_C"']
    )
    result = batch_for_datasource.validate(expectation)
    assert not result.success


@parameterize_batch_for_data_sources(
    data_source_configs=SQL_DATA_SOURCES_WITHOUT_SNOWFLAKE_REDSHIFT,
    data=CASE_INSENSITIVE_DATA,
)
def test_unquoted_and_quoted_success(batch_for_datasource: Batch) -> None:
    # Column A and B are case insensitve
    expectation = gxe.ExpectTableColumnsToMatchSet(
        column_set=["Column_a", "COLUMN_B", '"CoLuMn_C"']
    )
    result = batch_for_datasource.validate(expectation)
    assert result.success


@parameterize_batch_for_data_sources(
    data_source_configs=SQL_DATA_SOURCES,
    data=CASE_INSENSITIVE_DATA,
)
def test_unquoted_and_quoted_with_unquoted_failure(batch_for_datasource: Batch) -> None:
    # Column A and B are case insensitve
    expectation = gxe.ExpectTableColumnsToMatchSet(
        column_set=["Column_az", "COLUMN_B", '"CoLuMn_C"']
    )
    result = batch_for_datasource.validate(expectation)
    assert not result.success


@parameterize_batch_for_data_sources(
    data_source_configs=SQL_DATA_SOURCES,
    data=CASE_INSENSITIVE_DATA,
)
def test_unquoted_and_quoted_with_quoted_failure(batch_for_datasource: Batch) -> None:
    # Column C is case sensitve
    expectation = gxe.ExpectTableColumnsToMatchSet(
        column_set=["column_a", "COLUMN_B", '"Column_c"']
    )
    result = batch_for_datasource.validate(expectation)
    assert not result.success
