from typing import Any, Final, List

import pandas as pd
import pytest
from contrib.experimental.great_expectations_experimental.expectations.expect_column_values_to_be_present_in_other_table import (
    ExpectColumnValuesToBePresentInOtherTable,  # needed for expectation registration
)

import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility.sqlalchemy_compatibility_wrappers import (
    add_dataframe_to_db,
)
from great_expectations.core import ExpectationConfiguration
from great_expectations.data_context.util import file_relative_path
from great_expectations.datasource.fluent import SqliteDatasource

DB_PATH: Final[str] = file_relative_path(
    __file__, "../../test_sets/referential_integrity_dataset.db"
)


@pytest.fixture
def referential_integrity_db(sa):
    """Create a sqlite database with 3 tables: order_table_1, order_table_2, and customer_table. We only run this once to create the database."""
    sqlite_engine = sa.create_engine(f"sqlite:///{DB_PATH}")
    order_table_1 = pd.DataFrame(
        {
            "ORDER_ID": ["aaa", "bbb", "ccc"],
            "CUSTOMER_ID": [1, 1, 3],
        }
    )
    order_table_2 = pd.DataFrame(
        {
            "ORDER_ID": ["aaa", "bbb", "ccc"],
            "CUSTOMER_ID": [1, 5, 6],
        }
    )
    customer_table = pd.DataFrame(
        {
            "CUSTOMER_ID": [1, 2, 3],
        }
    )

    add_dataframe_to_db(
        df=order_table_1,
        name="order_table_1",
        con=sqlite_engine,
        index=False,
        if_exists="replace",
    )
    add_dataframe_to_db(
        df=order_table_2,
        name="order_table_2",
        con=sqlite_engine,
        index=False,
        if_exists="replace",
    )
    add_dataframe_to_db(
        df=customer_table,
        name="customer_table",
        con=sqlite_engine,
        index=False,
        if_exists="replace",
    )
    return sqlite_engine


@pytest.fixture()
def sqlite_context(in_memory_runtime_context) -> SqliteDatasource:
    context = in_memory_runtime_context
    datasource_name = "my_snowflake_datasource"
    context.sources.add_sqlite(
        datasource_name, connection_string=f"sqlite:///{DB_PATH}"
    )
    return context


@pytest.mark.sqlite
def test_successful_expectation_run(sqlite_context):
    context = sqlite_context
    asset_name = "order_table_1"
    datasource = context.get_datasource("my_snowflake_datasource")
    asset = datasource.add_table_asset(name=asset_name, table_name="order_table_1")
    context.add_or_update_expectation_suite("my_suite")
    batch_request = asset.build_batch_request()
    validator = context.get_validator(
        batch_request=batch_request, expectation_suite_name="my_suite"
    )
    res = validator.expect_column_values_to_be_present_in_other_table(
        foreign_key_column="CUSTOMER_ID",
        foreign_table="CUSTOMER_TABLE",
        foreign_table_key_column="CUSTOMER_ID",
    )
    assert res.success is True


@pytest.mark.sqlite
def test_failed_expectation_run(sqlite_context):
    context = sqlite_context
    asset_name = "order_table_2"
    datasource = context.get_datasource("my_snowflake_datasource")
    asset = datasource.add_table_asset(name=asset_name, table_name="order_table_2")
    context.add_or_update_expectation_suite("my_suite")
    batch_request = asset.build_batch_request()
    validator = context.get_validator(
        batch_request=batch_request, expectation_suite_name="my_suite"
    )
    res = validator.expect_column_values_to_be_present_in_other_table(
        foreign_key_column="CUSTOMER_ID",
        foreign_table="CUSTOMER_TABLE",
        foreign_table_key_column="CUSTOMER_ID",
    )

    assert res.success is False
    assert res["result"]["observed_value"] == "2 missing values."
    assert res["result"]["unexpected_index_list"] == [
        {"CUSTOMER_ID": 5},
        {"CUSTOMER_ID": 6},
    ]
    assert res["result"]["unexpected_index_column_names"] == ["CUSTOMER_ID"]
    assert res["result"]["unexpected_list"] == [5, 6]
    assert res["result"]["partial_unexpected_counts"] == [
        {"count": 1, "value": 5},
        {"count": 1, "value": 6},
    ]


@pytest.mark.sqlite
def test_configuration_invalid_column_name(sqlite_context):
    """What does this test do, and why?

    This is testing default behavior of `batch.validate()` which catches Exception information
    and places it in `exception_info`. Here we check that the exception message contains the text we expect
    """
    context = sqlite_context
    asset_name = "order_table_2"
    datasource = context.get_datasource("my_snowflake_datasource")
    asset = datasource.add_table_asset(name=asset_name, table_name="order_table_2")
    context.add_or_update_expectation_suite("my_suite")
    batch_request = asset.build_batch_request()
    validator = context.get_validator(
        batch_request=batch_request, expectation_suite_name="my_suite"
    )

    with pytest.raises(gx_exceptions.MetricResolutionError):
        res = validator.expect_column_values_to_be_present_in_other_table(
            foreign_key_column="I_DONT_EXIST",
            foreign_table="CUSTOMER_TABLE",
            foreign_table_key_column="CUSTOMER_ID",
        )

        assert res.success is False
        for k, v in res["exception_info"].items():
            assert v["raised_exception"] is True
            assert "no such column: a.I_DONT_EXIST" in v["exception_message"]


@pytest.mark.unit
def test_template_dict_creation():
    expectation_configuration: ExpectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_present_in_other_table",
        kwargs={
            "foreign_key_column": "CUSTOMER_ID",
            "foreign_table": "CUSTOMER_TABLE",
            "foreign_table_key_column": "CUSTOMER_ID",
        },
    )
    ExpectColumnValuesToBePresentInOtherTable(expectation_configuration)
    assert expectation_configuration["kwargs"]["template_dict"] == {
        "foreign_key_column": "CUSTOMER_ID",
        "foreign_table": "CUSTOMER_TABLE",
        "foreign_table_key_column": "CUSTOMER_ID",
    }


@pytest.mark.unit
@pytest.mark.parametrize(
    "unexpected_list,unexpected_counts,partial_unexpected_count",
    [
        pytest.param(
            ["4", "5"],
            [{"value": "4", "count": 1}, {"value": "5", "count": 1}],
            20,
            id="basic test with values appearing once.",
        ),
        pytest.param(
            ["1", "1"],
            [{"value": "1", "count": 2}],
            20,
            id="basic test with single value appearing twice.",
        ),
        pytest.param(
            ["4", "5", "5"],
            [{"value": "5", "count": 2}, {"value": "4", "count": 1}],
            20,
            id="test with default partial_unexpected_count of 20",
        ),
        pytest.param(
            ["4", "5", "5"],
            [{"value": "5", "count": 2}],
            1,
            id="test with partial_unexpected_count set to 1 and output only showing 1 value with full count.",
        ),
    ],
)
def test_generate_partial_unexpected_counts(
    unexpected_list: List[Any],
    unexpected_counts: List[dict],
    partial_unexpected_count: int,
):
    expectation_configuration: ExpectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_present_in_other_table",
        kwargs={
            "foreign_key_column": "CUSTOMER_ID",
            "foreign_table": "CUSTOMER_TABLE",
            "foreign_table_key_column": "CUSTOMER_ID",
        },
    )
    expectation = ExpectColumnValuesToBePresentInOtherTable(expectation_configuration)
    calculated_unexpected_counts = expectation._generate_partial_unexpected_counts(
        unexpected_list=unexpected_list,
        partial_unexpected_count=partial_unexpected_count,
    )
    assert unexpected_counts == calculated_unexpected_counts
