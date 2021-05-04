import great_expectations as ge

context = ge.DataContext()

batch = context.get_batch(
    batch_request=ge.core.batch.BatchRequest(
        datasource_name="taxi",
        data_connector_name="monthly",
        data_asset_name="yellow",
        data_connector_query={
            "batch_filter_parameters": {"year": "2019", "month": "02"}
        },
    )
)

print(batch.head())
