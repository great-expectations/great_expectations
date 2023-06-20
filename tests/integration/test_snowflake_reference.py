import great_expectations as gx
import os


def test_snowflake_reference():
    context = gx.get_context()

    my_connection_string: str = os.getenv(
        "SNOWFLAKE_CONNECTION_STRING"
    )

    datasource_name = "my_datasource"
    breakpoint()
    datasource = context.sources.add_or_update_sql(
        name=datasource_name, connection_string=my_connection_string, create_temp_table=False
    )

    asset_name = "my_asset"
    asset_table_name = "TPCH_SF1.CUSTOMER"

    table_asset = datasource.add_table_asset(name=asset_name, table_name=asset_table_name)

    batch_request = table_asset.build_batch_request()

    expectation_suite_name = "my_expectation_suite"
    context.add_or_update_expectation_suite(expectation_suite_name=expectation_suite_name)

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=expectation_suite_name,
    )
    validator.head()
