import glob

import pytest

import great_expectations as gx
from great_expectations.data_context.util import file_relative_path
from tests.datasource.new_fixtures import test_dir_oscar
from tests.test_utils import _get_batch_request_from_validator


def test_ZEP_scenario_1(test_dir_oscar):
    context = gx.get_context(lite=True)

    # Use the built-in datasource to get a validator, runtime-style
    my_validator_1 = context.sources.runtime_pandas.read_csv(
        test_dir_oscar + "/A/data-202201.csv"
    )
    my_validator_1.head()
    my_validator_1.expect_column_values_to_be_between("x", min_value=1, max_value=2)

    # Add a configured asset and use it to fetch a Validator
    context.sources.configured_pandas.add_asset(
        name="oscar_A",
        base_directory=test_dir_oscar + "/A",
    )
    my_batch_request_2 = (
        context.sources.configured_pandas.assets.oscar_A.get_batch_request(
            filename="data-202202.csv"
        )
    )
    my_validator_2 = context.sources.configured_pandas.assets.oscar_A.get_validator(
        filename="data-202202.csv"
    )
    my_validator_2.head()
    my_validator_2.expect_column_values_to_be_between("x", min_value=1, max_value=2)

    my_new_asset = context.sources.configured_pandas.add_asset("my_new_asset")
    df = my_new_asset.get_batch(test_dir_oscar + "/A/data-202112.csv")

    my_new_asset.update_configuration(
        base_directory=test_dir_oscar + "/A/",
    )
    df = my_new_asset.get_batch(
        "data-202112.csv",
    )
    df.expect_column_values_to_be_between("x", min_value=1, max_value=2)

    df = my_new_asset.update_configuration(
        name="oscar_A_2",
        base_directory=test_dir_oscar + "/A/",
        method="read_csv",
    ).get_batch(
        filename="data-202112.csv",
    )

    df = my_new_asset.update_configuration(
        base_directory=test_dir_oscar + "/A/",
        method="read_csv",
    ).get_batch(
        filename="data-202112.csv",
    )

    df = my_new_asset.update_configuration(
        base_directory=test_dir_oscar + "/A/",
        method="read_csv",
        regex="(.*)\\.csv",
    ).get_batch(
        filename="data-202112",
    )

    df = my_new_asset.update_configuration(
        base_directory=test_dir_oscar + "/A/",
        method="read_csv",
        regex="data-(.*)\\.csv",
        batch_identifiers=["year_month"],
    ).get_batch(
        year_month="202112",
    )
    print(df.head())

    df = my_new_asset.update_configuration(
        base_directory=test_dir_oscar + "/A/",
        method="read_csv",
        regex="data-(.*)\\.csv",
        batch_identifiers=["year_month"],
    ).get_batch(
        "202112",
    )
    print(df.head())

    df = my_new_asset.update_configuration(
        base_directory=test_dir_oscar + "/A/",
        method="read_csv",
        regex="data-(\\d{4})(\\d{2})\\.csv",
        batch_identifiers=["year", "month"],
    ).get_batch(
        year=2021,
        month=12,
    )

    return
    # Refine the asset configuration by adding a more detailed regex
    context.sources.default_pandas_reader.add_asset(
        name="oscar_A",
        base_directory=test_dir_oscar + "/A",
        regex="file-(.*)/.csv",
        batch_identifiers=["number"],
    )
    # context.sources.default_pandas_reader.assets.oscar_A.list_batches()
    my_batch_request_3 = (
        context.sources.default_pandas_reader.assets.oscar_A.get_batch_request(number=1)
    )
    # my_validator_3 = context.sources.default_pandas_reader.assets.oscar_A.get_validator(
    #     number=1
    # )
    # my_validator_3.head()
    # my_validator_3.expect_column_values_to_be_between("x", min_value=1, max_value=2)

    # Get a batch request spanning multiple files, and use it to configure a profiler
    #!!! Need to figure out the syntax for BatchRequests that can span ranges and multiple Batches.
    #!!! This implementation strikes me as error-prone.
    # my_batch_request = context.sources.default_pandas_reader.assets.my_asset.get_batch_request()
    # assistant_result = context.assistants.onboarding.run(my_batch_request)

    # Add multiple assets
    # !!! DX TBD

    # Add a checkpoint to routinely check this in the future
    # !!! DX TBD
    # context.add_checkpoint()


# @pytest.mark.skip(reason="still broken")
def test_ZEP_scenario_2():
    context = gx.get_context(lite=True)

    print(context.sources)

    context.fancy_add_datasource(
        name="my_sqlite_db",
        module_name="great_expectations.datasource.new_sqlalchemy_datasource",
        class_name="NewSqlAlchemyDatasource",
        connection_string="sqlite:///tests/chinook.db",
    )

    # Use the built-in datasource to get a validator, runtime-style
    my_validator_1 = context.sources.my_sqlite_db.get_table("invoices")
    my_validator_1.head()
    my_validator_1.expect_column_values_to_not_be_null("BillingCountry")

    return
    my_validator_2 = (
        context.sources.default_pandas_reader.assets.my_asset.get_validator(
            filename="B.csv"
        )
    )
    my_validator_2.head()
    my_validator_2.expect_column_values_to_be_between("x", min_value=1, max_value=2)

    # Refine the asset configuration by adding a more detailed regex
    context.sources.default_pandas_reader.add_asset(
        name="my_asset",
        base_directory=test_dir_alpha,
        regex="(.*)\\.csv",
    )
    my_validator_3 = (
        context.sources.default_pandas_reader.assets.my_asset.get_validator(
            filename="B"
        )
    )
    my_validator_3.head()
    my_validator_3.expect_column_values_to_be_between("x", min_value=1, max_value=2)

    # Get a batch request spanning multiple files, and use it to configure a profiler
    #!!! Need to figure out the syntax for BatchRequests that can span ranges and multiple Batches.
    #!!! This implementation strikes me as error-prone.
    my_batch_request = (
        context.sources.default_pandas_reader.assets.my_asset.get_batch_request()
    )
    assistant_result = context.assistants.onboarding.run(my_batch_request)
