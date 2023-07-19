import great_expectations as gx
import os


def test_bigquery_head(empty_data_context):
    profiler = Profiler()
    profiler.start()
    context = gx.get_context()
    my_connection_string = os.getenv("BIGQUERY_CONNECTION_STRING")
    datasource_name = "my_datasource"
    datasource = context.sources.add_or_update_sql(
        name=datasource_name,
        connection_string=my_connection_string,
        create_temp_table=False,
    )
    asset_name = "my_asset"

    # taxi_data is the name of a table in GX's test database. Please replace with a table that can be accessed by using the connection string above.
    asset_table_name = "taxi_data"

    table_asset = datasource.add_table_asset(
        name=asset_name, table_name=asset_table_name
    )
    batch_request = table_asset.build_batch_request()
    expectation_suite_name = "my_expectation_suite"
    context.add_or_update_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=expectation_suite_name,
    )
    print(validator.head())
    profiler.stop()
    profiler.print()


# def test_snowflake_head(empty_data_context):
#     context = gx.get_context()
#     my_connection_string = os.getenv("SNOWFLAKE_CONNECTION_STRING")
#     datasource_name = "my_datasource"
#     datasource = context.sources.add_or_update_sql(
#     name=datasource_name, connection_string=my_connection_string
#     )
#     asset_name = "my_asset"

#     # TEST_TAXI_COPY is the name of a table in GX's test database. Please replace with a table that can be accessed by using the SNOWFLAKE_CONNECTION_STRING above.
#     asset_table_name = "TEST_TAXI_COPY"
#     table_asset = datasource.add_table_asset(name=asset_name, table_name=asset_table_name)
#     batch_request = table_asset.build_batch_request()

#     expectation_suite_name = "my_expectation_suite"
#     context.add_or_update_expectation_suite(expectation_suite_name=expectation_suite_name)
#     validator = context.get_validator(
#     batch_request=batch_request,
#     expectation_suite_name=expectation_suite_name,
#     )
#     #profiler = Profiler()
#     #profiler.start()
#     print(validator.head())
#     # profiler.stop()
#     # profiler.print()


# def test_cloud():
#     import great_expectations as gx

#     context = gx.get_context(ge_cloud_mode=True)
#     result = context.run_checkpoint(ge_cloud_id="d992ef31-a445-4de4-afc9-bdf7cb3c932c")
