from typing import Final

import pandas as pd
import pytest
from contrib.experimental.great_expectations_experimental.expectations.expect_column_values_to_be_present_in_other_table import (  # noqa: E501
    ExpectColumnValuesToBePresentInOtherTable,  # needed for expectation registration
)

from great_expectations.compatibility.sqlalchemy_compatibility_wrappers import (
    add_dataframe_to_db,
)
from great_expectations.data_context.util import file_relative_path
from great_expectations.datasource.fluent import SqliteDatasource

DB_PATH: Final[str] = file_relative_path(
    __file__, "../../test_sets/referential_integrity_dataset.db"
)


@pytest.fixture
def referential_integrity_db(sa):
    """Create a sqlite database with 3 tables: order_table_1, order_table_2, and customer_table. We only run this once to create the database."""  # noqa: E501
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
def sqlite_datasource(in_memory_runtime_context) -> SqliteDatasource:
    context = in_memory_runtime_context
    datasource_name = "my_snowflake_datasource"
    return context.data_sources.add_sqlite(
        datasource_name, connection_string=f"sqlite:///{DB_PATH}"
    )


@pytest.mark.sqlite
def test_successful_expectation_run(sqlite_datasource):
    datasource = sqlite_datasource
    asset_name = "order_table_1"
    asset = datasource.add_table_asset(name=asset_name, table_name="order_table_1")
    batch = asset.get_batch(asset.build_batch_request())
    res = batch.validate(
        ExpectColumnValuesToBePresentInOtherTable(
            foreign_key_column="CUSTOMER_ID",
            foreign_table="customer_table",
            foreign_table_key_column="CUSTOMER_ID",
        )
    )
    assert res.success is True


@pytest.mark.sqlite
def test_failed_expectation_run(sqlite_datasource):
    datasource = sqlite_datasource
    asset_name = "order_table_2"
    asset = datasource.add_table_asset(name=asset_name, table_name="order_table_2")
    batch = asset.get_batch(asset.build_batch_request())
    res = batch.validate(
        ExpectColumnValuesToBePresentInOtherTable(
            foreign_key_column="CUSTOMER_ID",
            foreign_table="customer_table",
            foreign_table_key_column="CUSTOMER_ID",
        )
    )
    assert res.success is False
    assert res["result"]["observed_value"] == "2 missing values."
    assert res["result"]["unexpected_index_list"] == [
        {"CUSTOMER_ID": 5},
        {"CUSTOMER_ID": 6},
    ]


@pytest.mark.sqlite
def test_configuration_invalid_column_name(sqlite_datasource):
    """What does this test do, and why?

    This is testing default behavior of `batch.validate()` which catches Exception information
    and places it in `exception_info`. Here we check that the exception message contains the text we expect
    """  # noqa: E501
    datasource = sqlite_datasource
    asset_name = "order_table_2"
    asset = datasource.add_table_asset(name=asset_name, table_name="order_table_2")
    batch = asset.get_batch(asset.build_batch_request())
    res = batch.validate(
        ExpectColumnValuesToBePresentInOtherTable(
            foreign_key_column="I_DONT_EXIST",
            foreign_table="customer_table",
            foreign_table_key_column="CUSTOMER_ID",
        ),
    )

    assert res.success is False
    for k, v in res["exception_info"].items():
        assert v["raised_exception"] is True
        assert "no such column: a.I_DONT_EXIST" in v["exception_message"]


@pytest.mark.unit
def test_template_dict_creation():
    expectation = ExpectColumnValuesToBePresentInOtherTable(
        foreign_key_column="CUSTOMER_ID",
        foreign_table="customer_table",
        foreign_table_key_column="CUSTOMER_ID",
    )
    assert expectation.template_dict == {
        "foreign_key_column": "CUSTOMER_ID",
        "foreign_table": "customer_table",
        "foreign_table_key_column": "CUSTOMER_ID",
    }
