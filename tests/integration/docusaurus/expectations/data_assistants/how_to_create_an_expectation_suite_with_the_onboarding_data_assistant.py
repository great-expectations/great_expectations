"""Example Script: How to create an Expectation Suite with the Onboarding Data Assistant

This example script is intended for use in documentation on how to use an Onboarding Data Assistant to create
an Expectation Suite.

Assert statements are included to ensure that if the behaviour shown in this script breaks it will not pass
tests and will be updated.  These statements can be ignored by users.

Comments with the tags `<snippet>` and `</snippet>` are used to ensure that if this script is updated
the snippets that are specified for use in documentation are maintained.  These comments can be ignored by users.

--documentation--
    https://docs.greatexpectations.io/docs/guides/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant
"""
import great_expectations as ge
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.core.batch import BatchRequest
from great_expectations.core.yaml_handler import YAMLHandler

yaml = YAMLHandler()

context: ge.DataContext = ge.get_context()

# Configure your datasource (if you aren't using one that already exists)

# <snippet>
datasource_config = {
    "name": "taxi_multi_batch_datasource",
    "class_name": "Datasource",
    "module_name": "great_expectations.datasource",
    "execution_engine": {
        "module_name": "great_expectations.execution_engine",
        "class_name": "PandasExecutionEngine",
    },
    "data_connectors": {
        "inferred_data_connector_all_years": {
            "class_name": "InferredAssetFilesystemDataConnector",
            "base_directory": "<PATH_TO_YOUR_DATA_HERE>",
            "default_regex": {
                "group_names": ["data_asset_name", "year", "month"],
                "pattern": "(yellow_tripdata_sample)_(\\d.*)-(\\d.*)\\.csv",
            },
        },
    },
}
# </snippet>

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the yaml above.
datasource_config["data_connectors"]["inferred_data_connector_all_years"][
    "base_directory"
] = "../data/"

context.test_yaml_config(yaml.dump(datasource_config))

# add_datasource only if it doesn't already exist in our configuration

try:
    context.get_datasource(datasource_config["name"])
except ValueError:
    context.add_datasource(**datasource_config)

# Prepare an Expectation Suite

# <snippet>
expectation_suite_name = "my_onboarding_assistant_suite"

suite = context.create_expectation_suite(
    expectation_suite_name=expectation_suite_name, overwrite_existing=True
)
# </snippet>

# Prepare a Batch Request

# <snippet>
multi_batch_all_years_batch_request: BatchRequest = BatchRequest(
    datasource_name="taxi_multi_batch_datasource",
    data_connector_name="inferred_data_connector_all_years",
    data_asset_name="yellow_tripdata_sample",
    limit=1000,
)
# </snippet>

# Run the Onboarding Assistant

# <snippet>
exclude_column_names = [
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "rate_code_id",
    "store_and_fwd_flag",
    "pickup_location_id",
    "dropoff_location_id",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
]
# </snippet>

# <snippet>
result = context.assistants.onboarding.run(
    batch_request=multi_batch_all_years_batch_request,
    # include_column_names=include_column_names,
    exclude_column_names=exclude_column_names,
    # include_column_name_suffixes=include_column_name_suffixes,
    # exclude_column_name_suffixes=exclude_column_name_suffixes,
    # semantic_type_filter_module_name=semantic_type_filter_module_name,
    # semantic_type_filter_class_name=semantic_type_filter_class_name,
    # include_semantic_types=include_semantic_types,
    # exclude_semantic_types=exclude_semantic_types,
    # allowed_semantic_types_passthrough=allowed_semantic_types_passthrough,
    # cardinality_limit_mode="few",  # case-insensitive (see documentation for other options)
    # max_unique_values=max_unique_values,
    # max_proportion_unique=max_proportion_unique,
    # column_value_uniqueness_rule={
    #     "success_ratio": 0.8,
    # },
    # column_value_nullity_rule={
    # },
    # column_value_nonnullity_rule={
    # },
    # numeric_columns_rule={
    #     "false_positive_rate": 0.1,
    #     "random_seed": 43792,
    # },
    # datetime_columns_rule={
    #     "truncate_values": {
    #         "lower_bound": 0,
    #         "upper_bound": 4481049600,  # Friday, January 1, 2112 0:00:00
    #     },
    #     "round_decimals": 0,
    # },
    # text_columns_rule={
    #     "strict_min": True,
    #     "strict_max": True,
    #     "success_ratio": 0.8,
    # },
    # categorical_columns_rule={
    #     "false_positive_rate": 0.1,
    #     "round_decimals": 3,
    # },
)
# </snippet>

# Prepare a Validator

# <snippet>
validator = context.get_validator(
    batch_request=multi_batch_all_years_batch_request,
    expectation_suite_name=expectation_suite_name,
)
# </snippet>

# Save your Expectation Suite

# <snippet>
validator.expectation_suite = result.get_expectation_suite(
    expectation_suite_name=expectation_suite_name
)
# </snippet>

# <snippet>
validator.save_expectation_suite(discard_failed_expectations=False)
# </snippet>

# Use a SimpleCheckpoint to verify that your new Expectation Suite works.

# <snippet>
checkpoint_config = {
    "class_name": "SimpleCheckpoint",
    "validations": [
        {
            "batch_request": multi_batch_all_years_batch_request,
            "expectation_suite_name": expectation_suite_name,
        }
    ],
}
# </snippet>

# <snippet>
checkpoint = SimpleCheckpoint(
    f"{validator.active_batch_definition.data_asset_name}_{expectation_suite_name}",
    context,
    **checkpoint_config,
)
checkpoint_result = checkpoint.run()

assert checkpoint_result["success"] is True
# </snippet>

# If you are using code from this script as part of a Jupyter Notebook, uncommenting and running the
# following lines will open your Data Docs for the `checkpoint`'s results:

# context.build_data_docs()
# validation_result_identifier = checkpoint_result.list_validation_result_identifiers()[0]
# context.open_data_docs(resource_identifier=validation_result_identifier)
