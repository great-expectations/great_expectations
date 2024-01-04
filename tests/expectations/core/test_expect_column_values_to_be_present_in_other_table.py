import pandas as pd
import pytest
from contrib.experimental.great_expectations_experimental.expectations.expect_column_values_to_be_present_in_other_table import (
    ExpectColumnValuesToBePresentInAnotherTable,  # noqa: F401 # needed for expectation registration
)

import great_expectations as gx
import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility.sqlalchemy_compatibility_wrappers import (
    add_dataframe_to_db,
)


@pytest.fixture
def referential_integrity_db(sa):
    sqlite_engine = sa.create_engine(
        "sqlite:///../../test_sets/referential_integrity_dataset.db"
    )
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


def test_successful_expectation_run(referential_integrity_db):
    context = gx.get_context(cloud_mode=False)
    datasource_name = "my_snowflake_datasource"
    datasource = context.sources.add_sqlite(
        datasource_name,
        connection_string="sqlite:///../../test_sets/referential_integrity_dataset.db",
    )
    asset_name = "order_table_1"
    asset = datasource.add_table_asset(name=asset_name, table_name="order_table_1")
    batch_request = asset.build_batch_request()
    expectation_suite_name = "my_suite"
    context.add_or_update_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=expectation_suite_name,
    )
    res = validator.expect_column_values_to_be_present_in_another_table(
        foreign_key_column="CUSTOMER_ID",
        foreign_table="customer_table",
        foreign_table_key_column="CUSTOMER_ID",
    )
    assert res.success is True


def test_failed_expectation_run(referential_integrity_db):
    context = gx.get_context(cloud_mode=False)
    datasource_name = "my_snowflake_datasource"
    datasource = context.sources.add_sqlite(
        datasource_name,
        connection_string="sqlite:///../../test_sets/referential_integrity_dataset.db",
    )
    asset_name = "order_table_2"
    asset = datasource.add_table_asset(name=asset_name, table_name="order_table_2")
    batch_request = asset.build_batch_request()
    expectation_suite_name = "my_suite"
    context.add_or_update_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=expectation_suite_name,
    )
    res = validator.expect_column_values_to_be_present_in_another_table(
        foreign_key_column="CUSTOMER_ID",
        foreign_table="customer_table",
        foreign_table_key_column="CUSTOMER_ID",
    )
    assert res.success is False
    assert res["result"]["observed_value"] == "2 missing values."
    assert res["result"]["unexpected_index_list"] == [
        {"CUSTOMER_ID": 5},
        {"CUSTOMER_ID": 6},
    ]


def test_invalid_configurations(referential_integrity_db):
    context = gx.get_context(cloud_mode=False)
    datasource_name = "my_snowflake_datasource"
    datasource = context.sources.add_sqlite(
        datasource_name,
        connection_string="sqlite:///../../test_sets/referential_integrity_dataset.db",
    )
    asset_name = "order_table_2"
    asset = datasource.add_table_asset(name=asset_name, table_name="order_table_2")
    batch_request = asset.build_batch_request()
    expectation_suite_name = "my_suite"
    context.add_or_update_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=expectation_suite_name,
    )
    with pytest.raises(KeyError):
        # missing keys
        validator.expect_column_values_to_be_present_in_another_table(
            foreign_table_key_column="CUSTOMER_ID",
        )

    with pytest.raises(gx_exceptions.MetricResolutionError):
        # missing column
        validator.expect_column_values_to_be_present_in_another_table(
            foreign_key_column="I_DONT_EXIST",
            foreign_table="customer_table",
            foreign_table_key_column="CUSTOMER_ID",
        )

    with pytest.raises(KeyError):
        # invalid parameter
        validator.expect_column_values_to_be_present_in_another_table(
            i_dont_exist="CUSTOMER_ID",
            foreign_table="customer_table",
            foreign_table_key_column="CUSTOMER_ID",
        )
