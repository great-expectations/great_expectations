from __future__ import annotations

import contextlib
import random
from types import ModuleType, SimpleNamespace
from typing import TYPE_CHECKING, Callable, Final, List, Union
from unittest.mock import create_autospec, patch

import pytest
import sqlalchemy.dialects.mysql
import sqlalchemy.dialects.oracle
from _pytest import monkeypatch

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility import sqlalchemy
from great_expectations.compatibility.sqlalchemy import (
    Dialect,
    Engine,
)
from great_expectations.compatibility.sqlalchemy import (
    sqlalchemy as sa,
)
from great_expectations.data_context.util import file_relative_path
from great_expectations.exceptions import MetricResolutionError
from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.expectations.metrics import util as metrics_util
from great_expectations.expectations.metrics.util import (
    CaseInsensitiveString,
    _build_column_metadata_result,
    column_reflection_fallback,
    get_dbms_compatible_metric_domain_kwargs,
    get_dialect_like_pattern_expression,
    get_dialect_regex_expression,
    get_unexpected_indices_for_multiple_pandas_named_indices,
    get_unexpected_indices_for_single_pandas_named_index,
    sqlalchemy_select_to_sql_string,
)
from tests.test_utils import (
    get_awsathena_connection_url,
    get_bigquery_connection_url,
    get_default_mysql_url,
    get_default_postgres_url,
    get_default_sql_server_url,
    get_default_trino_url,
    get_redshift_connection_url,
    get_snowflake_connection_url,
)

if TYPE_CHECKING:
    import pandas as pd

# The following class allows for declarative instantiation of base class for SqlAlchemy. Adopted from  # noqa: E501 # FIXME CoP
# https://docs.sqlalchemy.org/en/14/faq/sqlexpressions.html#rendering-postcompile-parameters-as-bound-parameters

Base = sqlalchemy.declarative_base()


class A(Base):
    __tablename__ = "a"
    id = sa.Column(sa.Integer, primary_key=True)
    data = sa.Column(sa.String)


def select_with_post_compile_statements() -> sqlalchemy.Select:
    test_id: str = "00000000"
    return sa.select(A).where(A.data == test_id)


def _compare_select_statement_with_converted_string(engine) -> None:
    """
    Helper method used to do the call to sqlalchemy_select_to_sql_string()
    and compare with expected value.
    Args:
        engine (ExecutionEngine): SqlAlchemyExecutionEngine with connection to backend under test
    """
    select_statement: sqlalchemy.Select = select_with_post_compile_statements()
    returned_string = sqlalchemy_select_to_sql_string(
        engine=engine, select_statement=select_statement
    )
    assert returned_string == ("SELECT a.id, a.data \nFROM a \nWHERE a.data = '00000000';")


@pytest.fixture
def unexpected_index_list_one_index_column():
    return [
        {"animals": "cat", "pk_1": 0},
        {"animals": "fish", "pk_1": 1},
        {"animals": "dog", "pk_1": 2},
        {"animals": "giraffe", "pk_1": 3},
        {"animals": "lion", "pk_1": 4},
        {"animals": "zebra", "pk_1": 5},
    ]


@pytest.fixture
def unexpected_index_list_one_index_column_without_column_values():
    return [
        {"pk_1": [0, 1, 2, 3, 4, 5]},
    ]


@pytest.fixture
def unexpected_index_list_two_index_columns():
    return [
        {"animals": "cat", "pk_1": 0, "pk_2": "zero"},
        {"animals": "fish", "pk_1": 1, "pk_2": "one"},
        {"animals": "dog", "pk_1": 2, "pk_2": "two"},
        {"animals": "giraffe", "pk_1": 3, "pk_2": "three"},
        {"animals": "lion", "pk_1": 4, "pk_2": "four"},
        {"animals": "zebra", "pk_1": 5, "pk_2": "five"},
    ]


@pytest.fixture
def unexpected_index_list_two_index_columns_without_column_values():
    return [
        {
            "pk_1": [0, 1, 2, 3, 4, 5],
            "pk_2": ["zero", "one", "two", "three", "four", "five"],
        },
    ]


@pytest.mark.unit
@pytest.mark.parametrize(
    "backend_name,connection_string",
    [
        (
            "sqlite",
            f"sqlite:///{file_relative_path(__file__, '../../test_sets/metrics_test.db')}",
        ),
        ("postgresql", get_default_postgres_url()),
        ("mysql", get_default_mysql_url()),
        ("mssql", get_default_sql_server_url()),
        ("trino", get_default_trino_url()),
        ("redshift", get_redshift_connection_url()),
        ("snowflake", get_snowflake_connection_url()),
    ],
)
def test_sql_statement_conversion_to_string_for_backends(
    backend_name: str, connection_string: str, test_backends: List[str]
):
    if backend_name in test_backends:
        engine = SqlAlchemyExecutionEngine(connection_string=connection_string)
        _compare_select_statement_with_converted_string(engine=engine)
    else:
        pytest.skip(f"skipping sql statement conversion test for : {backend_name}")


@pytest.mark.unit
def test_sql_statement_conversion_to_string_awsathena(test_backends):
    if "awsathena" in test_backends:
        monkeypatch.setenv("ATHENA_STAGING_S3", "s3://test-staging/")
        monkeypatch.setenv("ATHENA_DB_NAME", "test_db_name")
        monkeypatch.setenv("ATHENA_TEN_TRIPS_DB_NAME", "test_ten_trips_db_name")
        connection_string = get_awsathena_connection_url()
        engine = SqlAlchemyExecutionEngine(connection_string=connection_string)
        _compare_select_statement_with_converted_string(engine=engine)
    else:
        pytest.skip("skipping sql statement conversion test for : awsathena")


@pytest.mark.unit
def test_sql_statement_conversion_to_string_bigquery(test_backends):
    """
    Bigquery backend returns a slightly different query
    """
    if "bigquery" in test_backends:
        monkeypatch.setenv("GE_TEST_GCP_PROJECT", "ge-oss")
        connection_string = get_bigquery_connection_url()
        engine = SqlAlchemyExecutionEngine(connection_string=connection_string)
        select_statement: sqlalchemy.Select = select_with_post_compile_statements()
        returned_string = sqlalchemy_select_to_sql_string(
            engine=engine, select_statement=select_statement
        )
        assert returned_string == (
            "SELECT `a`.`id`, `a`.`data` \nFROM `a` \nWHERE `a`.`data` = '00000000';"
        )
    else:
        pytest.skip("skipping sql statement conversion test for : bigquery")


@pytest.mark.unit
def test_get_unexpected_indices_for_single_pandas_named_index_named_unexpected_index_columns(
    pandas_animals_dataframe_for_unexpected_rows_and_index,
    unexpected_index_list_one_index_column,
):
    dataframe: pd.DataFrame = pandas_animals_dataframe_for_unexpected_rows_and_index
    updated_dataframe: pd.DataFrame = dataframe.set_index(["pk_1"])
    expectation_domain_column_list: List[str] = ["animals"]
    unexpected_index_column_names: List[str] = ["pk_1"]

    unexpected_index_list = get_unexpected_indices_for_single_pandas_named_index(
        domain_records_df=updated_dataframe,
        unexpected_index_column_names=unexpected_index_column_names,
        expectation_domain_column_list=expectation_domain_column_list,
    )
    assert unexpected_index_list == unexpected_index_list_one_index_column


@pytest.mark.unit
def test_get_unexpected_indices_for_single_pandas_named_index_named_unexpected_index_columns_without_column_values(  # noqa: E501 # FIXME CoP
    pandas_animals_dataframe_for_unexpected_rows_and_index,
    unexpected_index_list_one_index_column_without_column_values,
):
    dataframe: pd.DataFrame = pandas_animals_dataframe_for_unexpected_rows_and_index
    updated_dataframe: pd.DataFrame = dataframe.set_index(["pk_1"])
    expectation_domain_column_list: List[str] = ["animals"]
    unexpected_index_column_names: List[str] = ["pk_1"]

    unexpected_index_list = get_unexpected_indices_for_single_pandas_named_index(
        domain_records_df=updated_dataframe,
        unexpected_index_column_names=unexpected_index_column_names,
        expectation_domain_column_list=expectation_domain_column_list,
        exclude_unexpected_values=True,  # the new argument
    )
    assert unexpected_index_list == unexpected_index_list_one_index_column_without_column_values


@pytest.mark.unit
def test_get_unexpected_indices_for_single_pandas_named_index(
    pandas_animals_dataframe_for_unexpected_rows_and_index,
    unexpected_index_list_one_index_column,
):
    dataframe: pd.DataFrame = pandas_animals_dataframe_for_unexpected_rows_and_index
    updated_dataframe: pd.DataFrame = dataframe.set_index(["pk_1"])
    expectation_domain_column_list: List[str] = ["animals"]
    unexpected_index_column_names: List[str] = [updated_dataframe.index.name]

    unexpected_index_list = get_unexpected_indices_for_single_pandas_named_index(
        domain_records_df=updated_dataframe,
        unexpected_index_column_names=unexpected_index_column_names,
        expectation_domain_column_list=expectation_domain_column_list,
    )
    assert unexpected_index_list == unexpected_index_list_one_index_column


@pytest.mark.unit
def test_get_unexpected_indices_for_single_pandas_named_index_without_column_values(
    pandas_animals_dataframe_for_unexpected_rows_and_index,
    unexpected_index_list_one_index_column_without_column_values,
):
    dataframe: pd.DataFrame = pandas_animals_dataframe_for_unexpected_rows_and_index
    updated_dataframe: pd.DataFrame = dataframe.set_index(["pk_1"])
    expectation_domain_column_list: List[str] = ["animals"]
    unexpected_index_column_names: List[str] = [updated_dataframe.index.name]

    unexpected_index_list = get_unexpected_indices_for_single_pandas_named_index(
        domain_records_df=updated_dataframe,
        unexpected_index_column_names=unexpected_index_column_names,
        expectation_domain_column_list=expectation_domain_column_list,
        exclude_unexpected_values=True,  # the new argument
    )
    assert unexpected_index_list == unexpected_index_list_one_index_column_without_column_values


@pytest.mark.unit
def test_get_unexpected_indices_for_multiple_pandas_named_indices(
    pandas_animals_dataframe_for_unexpected_rows_and_index,
    unexpected_index_list_two_index_columns,
):
    dataframe: pd.DataFrame = pandas_animals_dataframe_for_unexpected_rows_and_index
    updated_dataframe: pd.DataFrame = dataframe.set_index(["pk_1", "pk_2"])
    expectation_domain_column_list: List[str] = ["animals"]
    unexpected_index_column_names: List[str] = list(updated_dataframe.index.names)

    unexpected_index_list = get_unexpected_indices_for_multiple_pandas_named_indices(
        domain_records_df=updated_dataframe,
        unexpected_index_column_names=unexpected_index_column_names,
        expectation_domain_column_list=expectation_domain_column_list,
    )
    assert unexpected_index_list == unexpected_index_list_two_index_columns


@pytest.mark.unit
def test_get_unexpected_indices_for_multiple_pandas_named_indices_without_column_values(
    pandas_animals_dataframe_for_unexpected_rows_and_index,
    unexpected_index_list_two_index_columns_without_column_values,
):
    dataframe: pd.DataFrame = pandas_animals_dataframe_for_unexpected_rows_and_index
    updated_dataframe: pd.DataFrame = dataframe.set_index(["pk_1", "pk_2"])
    expectation_domain_column_list: List[str] = ["animals"]
    unexpected_index_column_names: List[str] = list(updated_dataframe.index.names)

    unexpected_index_list = get_unexpected_indices_for_multiple_pandas_named_indices(
        domain_records_df=updated_dataframe,
        unexpected_index_column_names=unexpected_index_column_names,
        expectation_domain_column_list=expectation_domain_column_list,
        exclude_unexpected_values=True,  # the new argument
    )
    assert unexpected_index_list == unexpected_index_list_two_index_columns_without_column_values


@pytest.mark.unit
def test_get_unexpected_indices_for_multiple_pandas_named_indices_named_unexpected_index_columns(
    pandas_animals_dataframe_for_unexpected_rows_and_index,
    unexpected_index_list_two_index_columns,
):
    dataframe: pd.DataFrame = pandas_animals_dataframe_for_unexpected_rows_and_index
    updated_dataframe: pd.DataFrame = dataframe.set_index(["pk_1", "pk_2"])
    expectation_domain_column_list: List[str] = ["animals"]
    unexpected_index_column_names: List[str] = ["pk_1", "pk_2"]

    unexpected_index_list = get_unexpected_indices_for_multiple_pandas_named_indices(
        domain_records_df=updated_dataframe,
        unexpected_index_column_names=unexpected_index_column_names,
        expectation_domain_column_list=expectation_domain_column_list,
    )
    assert unexpected_index_list == unexpected_index_list_two_index_columns


@pytest.mark.unit
def test_get_unexpected_indices_for_multiple_pandas_named_indices_named_unexpected_index_columns_without_column_values(  # noqa: E501 # FIXME CoP
    pandas_animals_dataframe_for_unexpected_rows_and_index,
    unexpected_index_list_two_index_columns_without_column_values,
):
    dataframe: pd.DataFrame = pandas_animals_dataframe_for_unexpected_rows_and_index
    updated_dataframe: pd.DataFrame = dataframe.set_index(["pk_1", "pk_2"])
    expectation_domain_column_list: List[str] = ["animals"]
    unexpected_index_column_names: List[str] = ["pk_1", "pk_2"]

    unexpected_index_list = get_unexpected_indices_for_multiple_pandas_named_indices(
        domain_records_df=updated_dataframe,
        unexpected_index_column_names=unexpected_index_column_names,
        expectation_domain_column_list=expectation_domain_column_list,
        exclude_unexpected_values=True,  # the new argument
    )
    assert unexpected_index_list == unexpected_index_list_two_index_columns_without_column_values


@pytest.mark.unit
def test_get_unexpected_indices_for_multiple_pandas_named_indices_named_unexpected_index_columns_one_column(  # noqa: E501 # FIXME CoP
    pandas_animals_dataframe_for_unexpected_rows_and_index,
    unexpected_index_list_one_index_column,
):
    dataframe: pd.DataFrame = pandas_animals_dataframe_for_unexpected_rows_and_index
    updated_dataframe: pd.DataFrame = dataframe.set_index(["pk_1", "pk_2"])
    expectation_domain_column_list: List[str] = ["animals"]
    unexpected_index_column_names: List[str] = ["pk_1"]

    unexpected_index_list = get_unexpected_indices_for_multiple_pandas_named_indices(
        domain_records_df=updated_dataframe,
        unexpected_index_column_names=unexpected_index_column_names,
        expectation_domain_column_list=expectation_domain_column_list,
    )
    assert unexpected_index_list == unexpected_index_list_one_index_column


@pytest.mark.unit
def test_get_unexpected_indices_for_multiple_pandas_named_indices_named_unexpected_index_columns_one_column_without_column_values(  # noqa: E501 # FIXME CoP
    pandas_animals_dataframe_for_unexpected_rows_and_index,
    unexpected_index_list_one_index_column_without_column_values,
):
    dataframe: pd.DataFrame = pandas_animals_dataframe_for_unexpected_rows_and_index
    updated_dataframe: pd.DataFrame = dataframe.set_index(["pk_1", "pk_2"])
    expectation_domain_column_list: List[str] = ["animals"]
    unexpected_index_column_names: List[str] = ["pk_1"]

    unexpected_index_list = get_unexpected_indices_for_multiple_pandas_named_indices(
        domain_records_df=updated_dataframe,
        unexpected_index_column_names=unexpected_index_column_names,
        expectation_domain_column_list=expectation_domain_column_list,
        exclude_unexpected_values=True,  # the new argument
    )
    assert unexpected_index_list == unexpected_index_list_one_index_column_without_column_values


@pytest.mark.unit
def test_get_unexpected_indices_for_multiple_pandas_named_indices_named_unexpected_index_columns_wrong_column(  # noqa: E501 # FIXME CoP
    pandas_animals_dataframe_for_unexpected_rows_and_index,
):
    dataframe: pd.DataFrame = pandas_animals_dataframe_for_unexpected_rows_and_index
    updated_dataframe: pd.DataFrame = dataframe.set_index(["pk_1", "pk_2"])
    expectation_domain_column_list: List[str] = ["animals"]
    unexpected_index_column_names: List[str] = ["i_dont_exist"]
    with pytest.raises(MetricResolutionError) as e:
        get_unexpected_indices_for_multiple_pandas_named_indices(
            domain_records_df=updated_dataframe,
            unexpected_index_column_names=unexpected_index_column_names,
            expectation_domain_column_list=expectation_domain_column_list,
        )
    assert e.value.message == (
        "Error: The column i_dont_exist does not exist in the named indices. Please "
        "check your configuration."
    )


@pytest.mark.unit
def test_get_unexpected_indices_for_multiple_pandas_named_indices_named_unexpected_index_wrong_domain(  # noqa: E501 # FIXME CoP
    pandas_animals_dataframe_for_unexpected_rows_and_index,
):
    dataframe: pd.DataFrame = pandas_animals_dataframe_for_unexpected_rows_and_index
    updated_dataframe: pd.DataFrame = dataframe.set_index(["pk_1", "pk_2"])
    expectation_domain_column_list = []
    unexpected_index_column_names = ["pk_1"]
    with pytest.raises(MetricResolutionError) as e:
        get_unexpected_indices_for_multiple_pandas_named_indices(
            domain_records_df=updated_dataframe,
            unexpected_index_column_names=unexpected_index_column_names,
            expectation_domain_column_list=expectation_domain_column_list,
        )
    assert e.value.message == (
        "Error: The list of domain columns is currently empty. Please check your configuration."
    )


@pytest.fixture
def column_names_all_lowercase() -> list[str]:
    return [
        "artists",
        "healthcare_workers",
        "engineers",
        "lawyers",
        "scientists",
    ]


@pytest.fixture
def column_names_all_uppercase(column_names_all_lowercase: list[str]) -> list[str]:
    name: str
    return [name.upper() for name in column_names_all_lowercase]


@pytest.mark.unit
def test_get_dbms_compatible_metric_domain_column_kwargs_column_not_found(
    sa, column_names_all_lowercase: list[str]
):
    test_column_names: list[str] = column_names_all_lowercase
    with pytest.raises(gx_exceptions.InvalidMetricAccessorDomainKwargsKeyError) as eee:
        _ = get_dbms_compatible_metric_domain_kwargs(
            metric_domain_kwargs={"column": "non_existent_column"},
            batch_columns_list=test_column_names,
        )
    assert str(eee.value) == 'Error: The column "non_existent_column" in BatchData does not exist.'


@pytest.mark.unit
@pytest.mark.parametrize(
    [
        "input_column_name",
        "output_column_name",
        "confirm_not_equal_column_name",
    ],
    [
        pytest.param(
            "SHOULD_NOT_BE_QUOTED",
            "SHOULD_NOT_BE_QUOTED",
            None,
            id="column_does_not_need_to_be_quoted",
        ),
        pytest.param(
            "should_be_quoted",
            sqlalchemy.quoted_name(value="should_be_quoted", quote=True),
            "SHOULD_NOT_BE_QUOTED",
            id="column_must_be_quoted",
        ),
    ],
)
def test_get_dbms_compatible_metric_domain_column_kwargs(
    sa,
    column_names_all_uppercase: list[str],
    input_column_name: str,
    output_column_name: Union[str, sqlalchemy.quoted_name],
    confirm_not_equal_column_name: Union[str, sqlalchemy.quoted_name],
):
    not_quoted_column_name = "SHOULD_NOT_BE_QUOTED"
    quoted_column_name: sqlalchemy.quoted_name = sqlalchemy.quoted_name(
        value="should_be_quoted", quote=True
    )
    test_column_names: list[str] = column_names_all_uppercase + [
        not_quoted_column_name,
        quoted_column_name,
    ]

    metric_domain_kwargs: dict

    metric_domain_kwargs = get_dbms_compatible_metric_domain_kwargs(
        metric_domain_kwargs={"column": input_column_name},
        batch_columns_list=test_column_names,
    )
    assert metric_domain_kwargs["column"] == output_column_name
    if confirm_not_equal_column_name:
        assert metric_domain_kwargs["column"] != confirm_not_equal_column_name


@pytest.mark.unit
@pytest.mark.parametrize(
    [
        "input_column_name_a",
        "input_column_name_b",
        "output_column_name_a",
        "output_column_name_b",
    ],
    [
        pytest.param(
            "SHOULD_NOT_BE_QUOTED",
            sqlalchemy.quoted_name(value="should_be_quoted", quote=True),
            "SHOULD_NOT_BE_QUOTED",
            sqlalchemy.quoted_name(value="should_be_quoted", quote=True),
            id="column_a_does_not_need_to_be_quoted_column_b_must_remain_as_quoted",
        ),
        pytest.param(
            "SHOULD_NOT_BE_QUOTED",
            "should_be_quoted",
            "SHOULD_NOT_BE_QUOTED",
            sqlalchemy.quoted_name(value="should_be_quoted", quote=True),
            id="column_a_does_not_need_to_be_quoted_column_b_needs_to_be_quoted",
        ),
    ],
)
def test_get_dbms_compatible_metric_domain_column_pair_kwargs(
    sa,
    column_names_all_uppercase: list[str],
    input_column_name_a: str,
    input_column_name_b: str,
    output_column_name_a: Union[str, sqlalchemy.quoted_name],
    output_column_name_b: Union[str, sqlalchemy.quoted_name],
):
    not_quoted_column_name = "SHOULD_NOT_BE_QUOTED"
    quoted_column_name: sqlalchemy.quoted_name = sqlalchemy.quoted_name(
        value="should_be_quoted", quote=True
    )
    test_column_names: list[str] = column_names_all_uppercase + [
        not_quoted_column_name,
        quoted_column_name,
    ]

    metric_domain_kwargs: dict

    metric_domain_kwargs = get_dbms_compatible_metric_domain_kwargs(
        metric_domain_kwargs={
            "column_A": input_column_name_a,
            "column_B": input_column_name_b,
        },
        batch_columns_list=test_column_names,
    )
    assert metric_domain_kwargs["column_A"] == output_column_name_a
    assert metric_domain_kwargs["column_B"] == output_column_name_b


@pytest.mark.unit
@pytest.mark.unit
@pytest.mark.parametrize(
    [
        "input_column_list",
        "output_column_list",
    ],
    [
        pytest.param(
            [
                "SHOULD_NOT_BE_QUOTED",
                "should_be_quoted_0",
                "should_be_quoted_1",
                "should_be_quoted_2",
            ],
            [
                "SHOULD_NOT_BE_QUOTED",
                sqlalchemy.quoted_name(value="should_be_quoted_0", quote=True),
                sqlalchemy.quoted_name(value="should_be_quoted_1", quote=True),
                sqlalchemy.quoted_name(value="should_be_quoted_2", quote=True),
            ],
            id="column_list_has_three_columns_that_must_be_quoted",
        ),
    ],
)
def test_get_dbms_compatible_metric_domain_column_list_kwargs(
    sa,
    column_names_all_uppercase: list[str],
    input_column_list: list[str],
    output_column_list: list[Union[str, sqlalchemy.quoted_name]],
):
    not_quoted_column_name = "SHOULD_NOT_BE_QUOTED"
    quoted_column_name_0: sqlalchemy.quoted_name = sqlalchemy.quoted_name(
        value="should_be_quoted_0", quote=True
    )
    quoted_column_name_1: sqlalchemy.quoted_name = sqlalchemy.quoted_name(
        value="should_be_quoted_1", quote=True
    )
    quoted_column_name_2: sqlalchemy.quoted_name = sqlalchemy.quoted_name(
        value="should_be_quoted_2", quote=True
    )
    test_column_names: list[str] = column_names_all_uppercase + [
        not_quoted_column_name,
        quoted_column_name_0,
        quoted_column_name_1,
        quoted_column_name_2,
    ]
    """
    This shuffle intersperses input "column_list" so to ensure that there is no dependency on position of column names
    that must be quoted.  Sorting in assertion below ensures that types are correct, regardless of column order.
    """  # noqa: E501 # FIXME CoP
    random.shuffle(test_column_names)

    metric_domain_kwargs: dict

    metric_domain_kwargs = get_dbms_compatible_metric_domain_kwargs(
        metric_domain_kwargs={"column_list": input_column_list},
        batch_columns_list=test_column_names,
    )
    assert sorted(metric_domain_kwargs["column_list"]) == sorted(output_column_list)


_CASE_PARAMS: Final[list[str]] = [
    "mixedCase",
    "UPPERCASE",
    "lowercase",
    '"quotedMixedCase"',
    '"QUOTED_UPPERCASE"',
    '"quoted_lowercase"',
]


@pytest.mark.unit
@pytest.mark.parametrize("input_str", _CASE_PARAMS)
class TestCaseInsensitiveString:
    @pytest.mark.parametrize("other", _CASE_PARAMS)
    def test__eq__(
        self,
        input_str: str,
        other: str,
    ):
        other_case_insensitive = CaseInsensitiveString(other)
        input_case_insensitive = CaseInsensitiveString(input_str)

        # if either string is quoted, they must be exact match
        if input_case_insensitive.is_quoted() or other_case_insensitive.is_quoted():
            if input == other:
                assert input_case_insensitive == other
                assert input_case_insensitive == other_case_insensitive
            assert input_case_insensitive != CaseInsensitiveString(other.swapcase())
        elif input_str.lower() == other.lower():
            assert input_case_insensitive == other.swapcase()
            assert input_case_insensitive == CaseInsensitiveString(other.swapcase())
        else:
            assert input_case_insensitive != other_case_insensitive
            assert input_case_insensitive != other


@pytest.mark.unit
def test_get_sqlalchemy_column_metadata_includes_primary_key_field(
    sql_data_connector_test_db_execution_engine,
):
    """Test that get_sqlalchemy_column_metadata includes primary_key field for all columns."""
    from great_expectations.execution_engine.sqlalchemy_batch_data import SqlAlchemyBatchData
    from great_expectations.expectations.metrics.util import get_sqlalchemy_column_metadata

    engine = sql_data_connector_test_db_execution_engine

    # Test table with single primary key
    batch_data = SqlAlchemyBatchData(execution_engine=engine, table_name="table_with_single_pk")
    engine.load_batch_data("__test_single_pk", batch_data)

    columns = get_sqlalchemy_column_metadata(
        execution_engine=engine,
        table_selectable=sqlalchemy.quoted_name("table_with_single_pk", quote=False),
        schema_name=None,
    )

    assert columns is not None
    assert len(columns) == 3  # id, name, value

    # All columns should have primary_key field
    assert all("primary_key" in col for col in columns)

    # Only 'id' should be marked as primary key
    pk_columns = [col["name"] for col in columns if col["primary_key"]]
    assert pk_columns == ["id"]

    # Other columns should not be primary keys
    non_pk_columns = [col["name"] for col in columns if not col["primary_key"]]
    assert set(non_pk_columns) == {"name", "value"}


@pytest.mark.unit
def test_get_sqlalchemy_column_metadata_composite_primary_key(
    sql_data_connector_test_db_execution_engine,
):
    """Test that composite primary keys are correctly identified."""
    from great_expectations.execution_engine.sqlalchemy_batch_data import SqlAlchemyBatchData
    from great_expectations.expectations.metrics.util import get_sqlalchemy_column_metadata

    engine = sql_data_connector_test_db_execution_engine

    batch_data = SqlAlchemyBatchData(execution_engine=engine, table_name="table_with_composite_pk")
    engine.load_batch_data("__test_composite_pk", batch_data)

    columns = get_sqlalchemy_column_metadata(
        execution_engine=engine,
        table_selectable=sqlalchemy.quoted_name("table_with_composite_pk", quote=False),
        schema_name=None,
    )

    assert columns is not None
    assert len(columns) == 4  # user_id, order_id, product, quantity

    # All columns should have primary_key field
    assert all("primary_key" in col for col in columns)

    # Both user_id and order_id should be marked as primary keys
    pk_columns = sorted([col["name"] for col in columns if col["primary_key"]])
    assert pk_columns == ["order_id", "user_id"]

    # Other columns should not be primary keys
    non_pk_columns = sorted([col["name"] for col in columns if not col["primary_key"]])
    assert non_pk_columns == ["product", "quantity"]


@pytest.mark.unit
def test_get_sqlalchemy_column_metadata_no_primary_key(
    sql_data_connector_test_db_execution_engine,
):
    """Test that tables without primary keys don't break."""
    from great_expectations.execution_engine.sqlalchemy_batch_data import SqlAlchemyBatchData
    from great_expectations.expectations.metrics.util import get_sqlalchemy_column_metadata

    engine = sql_data_connector_test_db_execution_engine

    batch_data = SqlAlchemyBatchData(execution_engine=engine, table_name="table_without_pk")
    engine.load_batch_data("__test_no_pk", batch_data)

    columns = get_sqlalchemy_column_metadata(
        execution_engine=engine,
        table_selectable=sqlalchemy.quoted_name("table_without_pk", quote=False),
        schema_name=None,
    )

    assert columns is not None
    assert len(columns) == 2  # description, amount

    # All columns should have primary_key field
    assert all("primary_key" in col for col in columns)

    # No columns should be marked as primary keys
    pk_columns = [col["name"] for col in columns if col["primary_key"]]
    assert pk_columns == []

    # All columns should have primary_key=False
    assert all(not col["primary_key"] for col in columns)


@pytest.mark.unit
def test_get_sqlalchemy_column_metadata_quoted_pk_column(
    sql_data_connector_test_db_execution_engine,
):
    """Test that quoted column names as primary keys work correctly."""
    from great_expectations.execution_engine.sqlalchemy_batch_data import SqlAlchemyBatchData
    from great_expectations.expectations.metrics.util import get_sqlalchemy_column_metadata

    engine = sql_data_connector_test_db_execution_engine

    batch_data = SqlAlchemyBatchData(execution_engine=engine, table_name="table_with_quoted_pk")
    engine.load_batch_data("__test_quoted_pk", batch_data)

    columns = get_sqlalchemy_column_metadata(
        execution_engine=engine,
        table_selectable=sqlalchemy.quoted_name("table_with_quoted_pk", quote=False),
        schema_name=None,
    )

    assert columns is not None
    assert len(columns) == 2  # UserId, UserName

    # All columns should have primary_key field
    assert all("primary_key" in col for col in columns)

    # UserId should be marked as primary key
    pk_columns = [col["name"] for col in columns if col["primary_key"]]
    assert len(pk_columns) == 1
    # Case-insensitive check
    assert pk_columns[0].lower() == "userid"


@pytest.mark.unit
@patch("great_expectations.expectations.metrics.util.sa")
def test_get_dialect_like_pattern_expression_is_resilient_to_missing_dialects(mock_sqlalchemy):
    # arrange
    # force the test to not depend on _anything_ in sqlalchemy.dialects
    mock_sqlalchemy.dialects = None
    column = create_autospec(sa.Column)

    class SomeSpecificDialect: ...

    class MockDialect(ModuleType):
        dialect = SomeSpecificDialect

    like_pattern = "foo"

    # act
    # expect this test to not raise an AttributeError
    expression = get_dialect_like_pattern_expression(
        column=column, dialect=MockDialect(name="mock dialect"), like_pattern=like_pattern
    )

    # assert
    assert expression is None


def _compiled_like_expression(**kwargs) -> str:
    """Render a LIKE expression against a real dialect so the SQL text can be asserted on."""
    expression = get_dialect_like_pattern_expression(
        column=sa.column("col"), dialect=sqlalchemy.dialects.postgresql, **kwargs
    )
    assert expression is not None
    return str(
        expression.compile(
            dialect=sqlalchemy.dialects.postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    "positive,expected",
    [
        pytest.param(True, "col LIKE 'a!_b' ESCAPE '!'", id="positive"),
        pytest.param(False, "col NOT LIKE 'a!_b' ESCAPE '!'", id="negative"),
    ],
)
def test_get_dialect_like_pattern_expression_emits_escape_clause(positive: bool, expected: str):
    """An escape character must reach the SQL as an ESCAPE clause.

    Without it there is no way to match a literal '_' or '%', because both are wildcards.
    """
    assert _compiled_like_expression(like_pattern="a!_b", positive=positive, escape="!") == expected


@pytest.mark.unit
@pytest.mark.parametrize("positive", [True, False])
def test_get_dialect_like_pattern_expression_omits_escape_clause_by_default(positive: bool):
    """Callers that do not ask for an escape must get exactly the SQL they got before."""
    assert "ESCAPE" not in _compiled_like_expression(like_pattern="a_b", positive=positive)


@pytest.mark.unit
def test_get_dialect_like_pattern_expression_rejects_escape_on_bigquery():
    """GoogleSQL has no ESCAPE clause, so asking for one must fail with a usable message.

    Emitting the clause anyway would hand the user a database syntax error about SQL they
    never wrote.
    """
    bigquery_dialect = SimpleNamespace(BigQueryDialect=object)

    with pytest.raises(ValueError, match="BigQuery does not support an ESCAPE clause"):
        get_dialect_like_pattern_expression(
            column=sa.column("col"),
            dialect=bigquery_dialect,
            like_pattern="a!_b",
            escape="!",
        )


@pytest.mark.unit
def test_get_dialect_like_pattern_expression_allows_bigquery_without_escape():
    """The BigQuery guard must only fire when an escape was actually requested."""
    expression = get_dialect_like_pattern_expression(
        column=sa.column("col"),
        dialect=SimpleNamespace(BigQueryDialect=object),
        like_pattern="a_b",
    )

    assert expression is not None


@pytest.mark.unit
@pytest.mark.parametrize(
    "dialect_name,select_statement_factory,expected_sql,mock_params,should_fail_substitution",
    [
        pytest.param(
            "sqlite",
            lambda: sa.select(A.id, A.data).where(A.data == "test_value"),
            "SELECT a.id, a.data FROM a WHERE a.data = 'test_value'",
            {"data_1": "test_value"},
            False,
            id="string_param-sqlite",
        ),
        pytest.param(
            "postgresql",
            lambda: sa.select(A.id, A.data).where(A.data == "test_value"),
            "SELECT a.id, a.data FROM a WHERE a.data = 'test_value'",
            {"data_1": "test_value"},
            False,
            id="string_param-postgresql",
        ),
        pytest.param(
            "databricks",
            lambda: sa.select(A.id, A.data).where(A.data == "test_value"),
            "SELECT a.id, a.data FROM a WHERE a.data = 'test_value'",
            {"data_1": "test_value"},
            False,
            id="string_param-databricks",
        ),
        pytest.param(
            "sqlite",
            lambda: sa.select(A.id, A.data).where(A.id == 42),
            "SELECT a.id, a.data FROM a WHERE a.id = 42",
            {"id_1": 42},
            False,
            id="int_param-sqlite",
        ),
        pytest.param(
            "postgresql",
            lambda: sa.select(A.id, A.data).where(A.id == 42),
            "SELECT a.id, a.data FROM a WHERE a.id = 42",
            {"id_1": 42},
            False,
            id="int_param-postgresql",
        ),
        pytest.param(
            "sqlite",
            lambda: sa.select(A.id, A.data).where(A.id == 3.14),
            "SELECT a.id, a.data FROM a WHERE a.id = 3.14",
            {"id_1": 3.14},
            False,
            id="float_param-sqlite",
        ),
        pytest.param(
            "postgresql",
            lambda: sa.select(A.id, A.data).where(A.id == 3.14),
            "SELECT a.id, a.data FROM a WHERE a.id = 3.14",
            {"id_1": 3.14},
            False,
            id="float_param-postgresql",
        ),
        pytest.param(
            "sqlite",
            lambda: sa.select(A.id, A.data).where(A.id.is_(True)),
            "SELECT a.id, a.data FROM a WHERE a.id = True",
            {"id_1": True},
            False,
            id="bool_param-sqlite",
        ),
        pytest.param(
            "postgresql",
            lambda: sa.select(A.id, A.data).where(A.id.is_(True)),
            "SELECT a.id, a.data FROM a WHERE a.id = True",
            {"id_1": True},
            False,
            id="bool_param-postgresql",
        ),
        pytest.param(
            "sqlite",
            lambda: sa.select(A.id, A.data).where(A.data.is_(None)),
            "SELECT a.id, a.data FROM a WHERE a.data = None",
            {"data_1": None},
            False,
            id="none_param-sqlite",
        ),
        pytest.param(
            "postgresql",
            lambda: sa.select(A.id, A.data).where(A.data.is_(None)),
            "SELECT a.id, a.data FROM a WHERE a.data = None",
            {"data_1": None},
            False,
            id="none_param-postgresql",
        ),
        pytest.param(
            "databricks",
            lambda: sa.select(A.id, A.data).where(
                sa.or_(A.data == "value1", sa.and_(A.id > 10, A.data.like("%end%")))
            ),
            "SELECT a.id, a.data FROM a WHERE a.data = 'value1' "
            "OR (a.id > 10 AND a.data LIKE '%end%')",
            {"data_1": "value1", "id_1": 10, "data_2": "%end%"},
            False,
            id="multiple_params-databricks",
        ),
        pytest.param(
            "postgresql",
            lambda: sa.select(A.id, A.data).where(
                sa.or_(A.data == "value1", sa.and_(A.id > 10, A.data.like("%end%")))
            ),
            "SELECT a.id, a.data FROM a WHERE a.data = 'value1' "
            "OR (a.id > 10 AND a.data LIKE '%end%')",
            {"data_1": "value1", "id_1": 10, "data_2": "%end%"},
            False,
            id="multiple_params-postgresql",
        ),
        pytest.param(
            "postgresql",
            lambda: sa.select(A.id, A.data).where(A.data.like("%test%")),
            "SELECT a.id, a.data FROM a WHERE a.data LIKE '%test%'",
            {"data_1": "%test%"},
            True,
            id="like_pattern-postgresql-fallback",
        ),
        pytest.param(
            "mysql",
            lambda: sa.select(A.id, A.data).where(A.data.like("%test%")),
            "SELECT a.id, a.data FROM a WHERE a.data LIKE '%test%'",
            {"data_1": "%test%"},
            True,
            id="like_pattern-mysql-fallback",
        ),
        pytest.param(
            "redshift",
            lambda: sa.select(A.id, A.data).where(A.data.like("%test%")),
            "SELECT a.id, a.data FROM a WHERE a.data LIKE '%test%'",
            {"data_1": "%test%"},
            True,
            id="like_pattern-redshift-fallback",
        ),
        pytest.param(
            "snowflake",
            lambda: sa.select(A.id, A.data).where(A.data.like("%test%")),
            "SELECT a.id, a.data FROM a WHERE a.data LIKE '%test%'",
            {"data_1": "%test%"},
            False,
            id="like_pattern-snowflake",
        ),
        pytest.param(
            "sqlite",
            lambda: sa.select(A.id, A.data),
            "SELECT a.id, a.data FROM a",
            {},
            False,
            id="no_params-sqlite",
        ),
        pytest.param(
            "postgresql",
            lambda: sa.select(A.id, A.data),
            "SELECT a.id, a.data FROM a",
            {},
            False,
            id="no_params-postgresql",
        ),
    ],
)
def test_sqlalchemy_select_to_sql_string_parameter_styles(
    dialect_name: str,
    select_statement_factory: Callable[[], sa.Select],
    expected_sql: str,
    mock_params: dict,
    should_fail_substitution: bool,
    mocker: MockerFixture,
) -> None:
    """
    Test sqlalchemy_select_to_sql_string with to verify
    different parameter styles work correctly.

    Args:
        should_fail_substitution: If True, the render_postcompile path will return
            unsubstituted placeholders, forcing fallback to literal_binds. This tests
            the dialect_name usage for %% unescaping.
    """
    # Arrange
    select_statement = select_statement_factory()

    # Track which compile call we're on
    compile_call_count = [0]

    def mock_compile(engine, compile_kwargs=None):
        """Mock compile that returns different results based on compile_kwargs."""
        compile_call_count[0] += 1

        mock_compiled = mocker.MagicMock()
        mock_compiled.params = mock_params

        if compile_kwargs and compile_kwargs.get("render_postcompile"):
            # First call with render_postcompile=True
            if should_fail_substitution and mock_params:
                # Return query with placeholder to force fallback
                placeholder_query = expected_sql
                for param_name, param_value in mock_params.items():
                    # Replace first param value with placeholder to trigger fallback
                    param_value_repr = repr(param_value)
                    if param_value_repr in placeholder_query:
                        placeholder_query = placeholder_query.replace(
                            param_value_repr, f":{param_name}", 1
                        )
                        break
                mock_compiled.__str__ = lambda self: placeholder_query
            else:
                # Successful render_postcompile - return fully substituted SQL
                mock_compiled.__str__ = lambda self: expected_sql
        # Second call with literal_binds=True (only happens on fallback)
        # For dialects that escape %, return with %% to test unescaping
        elif dialect_name in ("postgresql", "mysql", "redshift") and "%" in expected_sql:
            escaped_sql = expected_sql.replace("%", "%%")
            mock_compiled.__str__ = lambda self: escaped_sql
        else:
            mock_compiled.__str__ = lambda self: expected_sql

        return mock_compiled

    # Patch select_statement.compile
    with patch.object(select_statement, "compile", side_effect=mock_compile):
        # Create a mock engine with the specified dialect
        mock_engine = create_autospec(SqlAlchemyExecutionEngine)
        mock_engine.dialect_name = dialect_name

        # Create a mock dialect and engine
        mock_dialect = create_autospec(Dialect)
        mock_dialect.name = dialect_name
        mock_engine.dialect = mock_dialect

        mock_sqlalchemy_engine = create_autospec(Engine)
        mock_sqlalchemy_engine.dialect = mock_dialect
        mock_engine.engine = mock_sqlalchemy_engine

        # Act
        result = sqlalchemy_select_to_sql_string(mock_engine, select_statement)

        # Assert
        assert result == expected_sql + ";"

        # Verify compile call count based on whether fallback was expected
        if should_fail_substitution:
            assert compile_call_count[0] == 2, (
                f"Expected 2 compile calls (render_postcompile + literal_binds fallback) "
                f"but got {compile_call_count[0]}"
            )
        else:
            assert compile_call_count[0] == 1, (
                f"Expected 1 compile call (successful render_postcompile) "
                f"but got {compile_call_count[0]}"
            )


@pytest.mark.unit
@pytest.mark.parametrize(
    "schema_name,expected_table_ref",
    [
        pytest.param(
            "my_schema",
            "my_schema.my_table",
            id="with_schema_name",
        ),
        pytest.param(
            None,
            "my_table",
            id="without_schema_name",
        ),
    ],
)
def test_column_reflection_fallback_redshift_schema_qualified(
    schema_name: str | None,
    expected_table_ref: str,
    mocker: MockerFixture,
) -> None:
    """Test that column_reflection_fallback uses schema-qualified table names for Redshift.

    This tests the fix for the bug where fallback column detection would fail with
    'relation "my_table" does not exist' when tables are in a non-default schema.
    The fix ensures that when schema_name is provided, the fallback query uses
    schema-qualified table names (e.g., 'my_schema.my_table' instead of just 'my_table').
    """
    # Create a mock dialect that reports as Redshift
    mock_dialect = mocker.MagicMock()
    mock_dialect.name = "redshift"

    # Create mock connection and result
    mock_result = mocker.MagicMock()
    mock_result.keys.return_value = ["id", "name", "value"]
    mock_result.fetchone.return_value = (1, "test", 100)

    mock_connection = mocker.MagicMock()
    mock_connection.execute.return_value = mock_result
    mock_connection.__enter__ = mocker.MagicMock(return_value=mock_connection)
    mock_connection.__exit__ = mocker.MagicMock(return_value=False)

    mock_engine = mocker.MagicMock()
    mock_engine.engine = mocker.MagicMock()
    mock_engine.engine.connect.return_value = mock_connection

    # Track what gets passed to sa.text()
    text_calls = []
    original_text = sa.text

    def track_text(arg):
        text_calls.append(arg)
        return original_text(arg)

    mocker.patch.object(sa, "text", side_effect=track_text)

    # Call the function
    result = column_reflection_fallback(
        selectable="my_table",  # type: ignore[arg-type]
        dialect=mock_dialect,
        sqlalchemy_engine=mock_engine,
        schema_name=schema_name,
    )

    # Verify the correct table reference was used
    assert expected_table_ref in text_calls, (
        f"Expected '{expected_table_ref}' in sa.text() calls, but got: {text_calls}"
    )
    assert isinstance(result, list)


class TestBuildColumnMetadataResultSQLServer:
    @staticmethod
    def _make_mock_engine(mocker: MockerFixture):
        mock_engine = mocker.MagicMock(spec=SqlAlchemyExecutionEngine)
        mock_dialect = mocker.MagicMock(spec=Dialect)
        mock_dialect.name = "mssql"
        mock_engine.dialect = mock_dialect
        return mock_engine

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "col_type,expected_str",
        [
            pytest.param("VARCHAR", "VARCHAR", id="string_from_fallback"),
            pytest.param("NVARCHAR", "NVARCHAR", id="string_nvarchar_from_fallback"),
            pytest.param("INTEGER", "INTEGER", id="string_integer_from_fallback"),
            pytest.param(sa.types.VARCHAR(), "VARCHAR", id="type_engine_varchar"),
            pytest.param(
                sa.types.VARCHAR(collation="SQL_Latin1_General_CP1_CI_AS"),
                "VARCHAR",
                id="type_engine_varchar_with_collation",
            ),
            pytest.param(sa.types.NVARCHAR(), "NVARCHAR", id="type_engine_nvarchar"),
            pytest.param(sa.types.INTEGER(), "INTEGER", id="type_engine_integer"),
        ],
    )
    def test_type_normalized_to_case_insensitive_string(
        self, mocker: MockerFixture, col_type, expected_str: str
    ):
        engine = self._make_mock_engine(mocker)
        result = _build_column_metadata_result([{"name": "col1", "type": col_type}], set(), engine)

        assert isinstance(result[0]["type"], CaseInsensitiveString)
        assert str(result[0]["type"]) == expected_str
        assert result[0]["type"] == expected_str.lower()


class _DialectDetectionStub:
    """A minimal stand-in for the ``dialect`` argument of
    ``get_dialect_regex_expression``, exposing only the attributes explicitly
    passed in.

    Unlike ``unittest.mock.Mock()``, which auto-creates every attribute on
    access, this object raises ``AttributeError`` for anything not set here.
    That distinction matters for this chain: several branches are detected
    with ``hasattr(dialect, "<Name>Dialect")``, so a ``Mock()`` would satisfy
    the *first* such branch (redshift) for every dialect under test, making
    every later ``hasattr`` branch permanently unreachable while the test
    still passed.
    """

    def __init__(self, **attrs: object) -> None:
        for name, value in attrs.items():
            setattr(self, name, value)


class _FakeSnowflakeDialect:
    """Stand-in for ``snowflake.sqlalchemy.snowdialect.SnowflakeDialect``.

    The real class lives in an optional vendor package. A fake is used here,
    together with patching the module-level ``snowflake`` name the helper
    reads, so the snowflake branch is exercised without that package
    installed.
    """


class _FakeTeradataDialect:
    """Stand-in for ``teradatasqlalchemy.dialect.TeradataDialect``, for the
    same reason as ``_FakeSnowflakeDialect`` above.
    """


class _FakeDatabricksDialect:
    """Stand-in for the databricks vendor package's ``DatabricksDialect``.

    The helper's databricks detection (``_is_databricks_dialect``)
    short-circuits to ``False`` whenever the module-level ``sqla_databricks``
    name is falsy, so that name must be patched to something truthy even
    though the branch itself is reached through ``hasattr``, not
    ``issubclass``.
    """


class _OracleSub(sa.dialects.oracle.dialect):
    """A concrete subclass of SQLAlchemy's bundled Oracle dialect.

    The chain detects Oracle with ``issubclass`` against the bundled
    dialect, the same idiom postgres, mysql, and sqlite use, because Oracle
    ships with SQLAlchemy. A subclass is used here for the same reason the
    postgres, mysql, and sqlite stubs are: it exercises the ``issubclass``
    relation rather than object identity, and so is strictly more permissive
    than what the execution engine passes, which is the bundled module.
    """


class _PGSub(sa.dialects.postgresql.dialect):
    pass


class _MySQLSub(sqlalchemy.dialects.mysql.base.MySQLDialect):
    pass


class _SQLiteSub(sa.dialects.sqlite.dialect):
    pass


class _SnowflakeSub(_FakeSnowflakeDialect):
    pass


class _TeradataSub(_FakeTeradataDialect):
    pass


# Attribute names the chain probes with `hasattr` directly on the top-level
# dialect object (as opposed to on `.dialect`). Used below to prove that no
# stub for one branch accidentally also satisfies another branch's
# detection.
_HASATTR_PROBED_DIALECT_NAMES: Final = (
    "DatabricksDialect",
    "RedshiftDialect",
    "BigQueryDialect",
    "TrinoDialect",
    "ClickHouseDialect",
    "DremioDialect",
)

# One entry per existing branch in `get_dialect_regex_expression`, in chain
# order: (case_id, dialect stub, module-level patches needed to reach it
# without installing an optional vendor package).
_REGEX_DIALECT_CASES: Final = {
    "postgresql": (
        _DialectDetectionStub(dialect=_PGSub),
        {},
    ),
    "databricks": (
        _DialectDetectionStub(DatabricksDialect=object()),
        {"sqla_databricks": SimpleNamespace(DatabricksDialect=_FakeDatabricksDialect)},
    ),
    "redshift": (
        _DialectDetectionStub(RedshiftDialect=object()),
        {},
    ),
    "mysql": (
        _DialectDetectionStub(dialect=_MySQLSub),
        {},
    ),
    "snowflake": (
        _DialectDetectionStub(dialect=_SnowflakeSub),
        {
            "snowflake": SimpleNamespace(
                sqlalchemy=SimpleNamespace(
                    snowdialect=SimpleNamespace(SnowflakeDialect=_FakeSnowflakeDialect)
                )
            )
        },
    ),
    "bigquery": (
        _DialectDetectionStub(BigQueryDialect=object()),
        {},
    ),
    "trino": (
        _DialectDetectionStub(TrinoDialect=object()),
        {},
    ),
    "clickhouse": (
        _DialectDetectionStub(ClickHouseDialect=object()),
        {},
    ),
    "dremio": (
        _DialectDetectionStub(DremioDialect=object()),
        {},
    ),
    "teradata": (
        _DialectDetectionStub(dialect=_TeradataSub),
        {
            "teradatasqlalchemy": SimpleNamespace(
                dialect=SimpleNamespace(TeradataDialect=_FakeTeradataDialect)
            )
        },
    ),
    "sqlite": (
        _DialectDetectionStub(dialect=_SQLiteSub),
        {},
    ),
}


def _render_regex_expression(case_id: str, positive: bool) -> str | None:
    stub, patches = _REGEX_DIALECT_CASES[case_id]
    column = sa.column("a")
    with contextlib.ExitStack() as stack:
        for name, value in patches.items():
            stack.enter_context(patch.object(metrics_util, name, value))
        expr = get_dialect_regex_expression(
            column=column, regex="test", dialect=stub, positive=positive
        )
    if expr is None:
        return None
    return str(expr.compile(compile_kwargs={"literal_binds": True}))


@pytest.mark.unit
def test_get_dialect_regex_expression_stubs_are_mutually_exclusive() -> None:
    """Prove the eleven stubs above are pairwise non-overlapping, so a green
    result below reflects eleven distinct branches firing rather than one
    branch (e.g. the first `hasattr` branch, redshift) matching every case.

    Every stub is built from `_DialectDetectionStub`, which -- unlike
    `unittest.mock.Mock()` -- exposes exactly the attributes passed to it.
    This test checks that construction invariant mechanically: a `.dialect`
    stub exposes none of the names any `hasattr`-idiom branch probes, and a
    `hasattr`-idiom stub exposes exactly one such name (its own) and no
    `.dialect` attribute at all.
    """
    for case_id, (stub, _patches) in _REGEX_DIALECT_CASES.items():
        exposed_hasattr_names = [
            name for name in _HASATTR_PROBED_DIALECT_NAMES if hasattr(stub, name)
        ]
        if hasattr(stub, "dialect"):
            assert exposed_hasattr_names == [], (
                f"{case_id} stub uses the issubclass idiom but also exposes "
                f"hasattr-probed name(s) {exposed_hasattr_names}, which would let it "
                f"satisfy another branch's detection too"
            )
        else:
            assert len(exposed_hasattr_names) == 1, (
                f"{case_id} stub does not expose exactly one hasattr-probed name: "
                f"{exposed_hasattr_names}"
            )


@pytest.mark.unit
@pytest.mark.parametrize(
    "case_id,positive,expected_sql",
    [
        pytest.param("postgresql", True, "a ~ 'test'", id="postgresql-positive"),
        pytest.param("postgresql", False, "a !~ 'test'", id="postgresql-negative"),
        pytest.param("databricks", True, "regexp_like(a, 'test')", id="databricks-positive"),
        pytest.param("databricks", False, "NOT regexp_like(a, 'test')", id="databricks-negative"),
        pytest.param("redshift", True, "a ~ 'test'", id="redshift-positive"),
        pytest.param("redshift", False, "a !~ 'test'", id="redshift-negative"),
        pytest.param("mysql", True, "a REGEXP 'test'", id="mysql-positive"),
        pytest.param("mysql", False, "a NOT REGEXP 'test'", id="mysql-negative"),
        pytest.param("snowflake", True, "a REGEXP 'test'", id="snowflake-positive"),
        pytest.param("snowflake", False, "a NOT REGEXP 'test'", id="snowflake-negative"),
        pytest.param("bigquery", True, "REGEXP_CONTAINS(a, 'test')", id="bigquery-positive"),
        pytest.param("bigquery", False, "NOT REGEXP_CONTAINS(a, 'test')", id="bigquery-negative"),
        pytest.param("trino", True, "regexp_like(a, 'test')", id="trino-positive"),
        pytest.param("trino", False, "NOT regexp_like(a, 'test')", id="trino-negative"),
        pytest.param("clickhouse", True, "regexp_like(a, 'test')", id="clickhouse-positive"),
        pytest.param("clickhouse", False, "NOT regexp_like(a, 'test')", id="clickhouse-negative"),
        pytest.param("dremio", True, "REGEXP_MATCHES(a, 'test')", id="dremio-positive"),
        pytest.param("dremio", False, "NOT REGEXP_MATCHES(a, 'test')", id="dremio-negative"),
        pytest.param(
            "teradata",
            True,
            "REGEXP_SIMILAR(a, 'test', 'i') = 1",
            id="teradata-positive",
        ),
        pytest.param(
            "teradata",
            False,
            "REGEXP_SIMILAR(a, 'test', 'i') = 0",
            id="teradata-negative",
        ),
        pytest.param("sqlite", True, "a <regexp> 'test'", id="sqlite-positive"),
        pytest.param("sqlite", False, "a <not regexp> 'test'", id="sqlite-negative"),
    ],
)
def test_get_dialect_regex_expression_pins_every_existing_branch(
    case_id: str, positive: bool, expected_sql: str
) -> None:
    """Pin the exact SQL `get_dialect_regex_expression` renders today for
    every one of the eleven existing dialect branches, in both the positive
    and negated form. This is a compile-only, database-free characterization
    test: no engine, connection, or optional vendor package is required.
    """
    rendered = _render_regex_expression(case_id, positive)

    assert rendered is not None, (
        f"{case_id} ({'positive' if positive else 'negative'}) unexpectedly returned None"
    )
    assert rendered == expected_sql


@pytest.mark.unit
@pytest.mark.parametrize(
    "positive,expected_sql",
    [
        pytest.param(True, "regexp_like(a, 'test')", id="positive"),
        # The negated form is `sa.not_()` applied to the positive one, so this
        # also pins how that wrapping renders -- which is what one aggregate
        # caller relies on, since it requests the positive form and negates in
        # Python rather than asking for the negated form.
        pytest.param(False, "NOT regexp_like(a, 'test')", id="negative"),
    ],
)
def test_get_dialect_regex_expression_renders_oracle_native_predicate(
    positive: bool, expected_sql: str
) -> None:
    """`get_dialect_regex_expression`'s branch chain now has an Oracle entry,
    detected the same `issubclass` way postgres, mysql, and sqlite already
    are, since Oracle ships with SQLAlchemy. It renders Oracle's native
    `REGEXP_LIKE` condition, in the same `sa.func.regexp_like(...)` /
    `sa.not_(...)` shape Trino and ClickHouse already use -- so the return
    value survives both direct use as a predicate and the one caller that
    wraps it in `sa.not_()` itself.
    """
    stub = _DialectDetectionStub(dialect=_OracleSub)
    column = sa.column("a")

    result = get_dialect_regex_expression(
        column=column, regex="test", dialect=stub, positive=positive
    )

    assert result is not None
    rendered = str(result.compile(compile_kwargs={"literal_binds": True}))
    assert rendered == expected_sql


# `get_dialect_regex_expression` has 6 calling modules / 9 call sites, which reduce to four
# distinct *families* by call shape: the row-level condition family (a single call, one per
# polarity), the aggregate-value family (a single call plus a Python-side `sa.not_()` for the
# negative form), and the two list-form families (a call per regex in the list, combined with
# `sa.or_`/`sa.and_`). Of these, the curated suite exercises only `column_values.match_regex`,
# the positive half of the row-level family; the negative half is pinned above by
# `test_get_dialect_regex_expression_renders_oracle_native_predicate[negative]`.
# The three tests below reproduce the exact resolution shape of the remaining three families --
# they are not re-tests of the helper in isolation, they replicate what each family's own
# `_sqlalchemy` method does with the helper's return value -- and name the module each covers.


@pytest.mark.unit
def test_get_dialect_regex_expression_resolves_oracle_aggregate_family() -> None:
    """Covers the aggregate family:
    `great_expectations/expectations/metrics/column_aggregate_metrics/column_values_match_regex_values.py`
    and
    `great_expectations/expectations/metrics/column_aggregate_metrics/column_values_not_match_regex_values.py`.

    Both call `get_dialect_regex_expression(column, regex, _dialect)` once and use the result
    directly in a `sa.select(column).where(...)` query; the "not match" module additionally
    wraps the result in `sa.not_()` before using it as the `where()` predicate. This reproduces
    that exact resolution shape against an Oracle dialect and pins the resulting SQL, proving
    the branch is reached (a `None` return would raise `NotImplementedError` in production
    before a query is ever built).
    """
    stub = _DialectDetectionStub(dialect=_OracleSub)
    column = sa.column("a")

    regex_expression = get_dialect_regex_expression(column=column, regex="test", dialect=stub)
    assert regex_expression is not None, (
        "aggregate family: get_dialect_regex_expression returned None for Oracle -- "
        "column_values_match_regex_values.py and column_values_not_match_regex_values.py "
        "would raise NotImplementedError before building a query"
    )

    match_query = sa.select(column).where(regex_expression)
    assert str(match_query.compile(compile_kwargs={"literal_binds": True})) == (
        "SELECT a \nWHERE regexp_like(a, 'test')"
    )

    not_match_query = sa.select(column).where(sa.not_(regex_expression))
    assert str(not_match_query.compile(compile_kwargs={"literal_binds": True})) == (
        "SELECT a \nWHERE NOT regexp_like(a, 'test')"
    )


@pytest.mark.unit
def test_get_dialect_regex_expression_resolves_oracle_regex_list_match_family() -> None:
    """Covers the list-form match family:
    `great_expectations/expectations/metrics/column_map_metrics/column_values_match_regex_list.py`.

    That module calls `get_dialect_regex_expression` once per entry in `regex_list` and combines
    the results with `sa.or_()` (`match_on="any"`) or `sa.and_()` (`match_on="all"`). This
    reproduces that exact resolution shape against an Oracle dialect and pins both combined
    forms, proving the branch is reached for every call in the list -- not just the first.
    """
    stub = _DialectDetectionStub(dialect=_OracleSub)
    column = sa.column("a")
    regex_list = ["foo", "bar"]

    conditions = [
        get_dialect_regex_expression(column=column, regex=regex, dialect=stub)
        for regex in regex_list
    ]
    assert all(condition is not None for condition in conditions), (
        "regex_list match family: get_dialect_regex_expression returned None for Oracle on at "
        "least one entry -- column_values_match_regex_list.py would raise NotImplementedError "
        "before combining conditions"
    )

    any_condition = sa.or_(*conditions)
    assert str(any_condition.compile(compile_kwargs={"literal_binds": True})) == (
        "regexp_like(a, 'foo') OR regexp_like(a, 'bar')"
    )

    all_condition = sa.and_(*conditions)
    assert str(all_condition.compile(compile_kwargs={"literal_binds": True})) == (
        "regexp_like(a, 'foo') AND regexp_like(a, 'bar')"
    )


@pytest.mark.unit
def test_get_dialect_regex_expression_resolves_oracle_regex_list_not_match_family() -> None:
    """Covers the list-form not-match family:
    `great_expectations/expectations/metrics/column_map_metrics/column_values_not_match_regex_list.py`.

    That module calls `get_dialect_regex_expression(..., positive=False)` once per entry in
    `regex_list` and combines the results with `sa.and_()`. This reproduces that exact
    resolution shape against an Oracle dialect and pins the combined SQL, proving the negative
    branch is reached for every call in the list.
    """
    stub = _DialectDetectionStub(dialect=_OracleSub)
    column = sa.column("a")
    regex_list = ["foo", "bar"]

    conditions = [
        get_dialect_regex_expression(column=column, regex=regex, dialect=stub, positive=False)
        for regex in regex_list
    ]
    assert all(condition is not None for condition in conditions), (
        "regex_list not-match family: get_dialect_regex_expression returned None for Oracle on "
        "at least one entry -- column_values_not_match_regex_list.py would raise "
        "NotImplementedError before combining conditions"
    )

    compound_condition = sa.and_(*conditions)
    assert str(compound_condition.compile(compile_kwargs={"literal_binds": True})) == (
        "NOT regexp_like(a, 'foo') AND NOT regexp_like(a, 'bar')"
    )
