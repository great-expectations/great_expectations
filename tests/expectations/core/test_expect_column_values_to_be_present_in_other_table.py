import os

import great_expectations as gx


def test_first():
    context = gx.get_context(cloud_mode=False)
    datasource_name = "my_snowflake_datasource"
    datasource = context.sources.add_snowflake(
        name=datasource_name,
        account="oca29081.us-east-1",
        user="WILL@GREATEXPECTATIONS.IO",
        password=os.environ["SNOWFLAKE_PW"],
        database="DEMO_DB",
        schema="PUBLIC",
        warehouse="COMPUTE_WH",
        role="PUBLIC",
    )
    asset_name = "my_asset"
    asset_table_name = "FOREIGN_KEY_TEST_TABLE_1"
    table_asset = datasource.add_table_asset(
        name=asset_name, table_name=asset_table_name
    )
    my_batch_request = table_asset.build_batch_request()
    batches = table_asset.get_batch_list_from_batch_request(my_batch_request)
    for batch in batches:
        print(batch.batch_spec)
