import great_expectations as gx


# Set up
context = gx.get_context()


# Connect to data
validator = context.sources.pandas_default.read_csv(
    "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv",
    asset_name="taxi_asset",
)

# Create Expectations
validator.expect_column_values_to_not_be_null("pickup_datetime")
validator.expect_column_values_to_be_between("passenger_count", auto=True)

validator.expectation_suite_name = "taxi_suite"
expectation_suite = validator.expectation_suite


# Validate data
checkpoint = gx.checkpoint.SimpleCheckpoint(
    name="my_quickstart_checkpoint",
    data_context=context,
    validations=[
        {
            "expectation_suite_name": expectation_suite.expectation_suite_name,
            "batch_request": {
                "datasource_name": "default_pandas_datasource",
                "data_asset_name": "taxi_asset",
            },
        }
    ],
)

checkpoint_result = checkpoint.run()

# View results
context.view_validation_result(checkpoint_result)
