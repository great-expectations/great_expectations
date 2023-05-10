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
import great_expectations as gx
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.core.batch import BatchRequest
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.datasource.fluent.interfaces import DataAsset

yaml = YAMLHandler()

context = gx.get_context()

# Configure your datasource (if you aren't using one that already exists)

# <snippet name="tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant.py datasource_config">

context.sources.add_pandas_filesystem(
    "taxi_multi_batch_datasource",
    base_directory="./data",  # replace with your data directory
).add_csv_asset(
    "all_years",
    batching_regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
)

# </snippet>

# Prepare an Expectation Suite

# <snippet name="tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant.py expectation_suite">
expectation_suite_name = "my_onboarding_assistant_suite"

expectation_suite = context.add_or_update_expectation_suite(
    expectation_suite_name=expectation_suite_name
)
# </snippet>

# Prepare a Batch Request

# <snippet name="tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant.py batch_request">
all_years_asset: DataAsset = context.datasources[
    "taxi_multi_batch_datasource"
].get_asset("all_years")

multi_batch_all_years_batch_request: BatchRequest = (
    all_years_asset.build_batch_request()
)
# </snippet>

# Run the Onboarding Assistant

# <snippet name="tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant.py exclude_column_names">
exclude_column_names = [
    "VendorID",
    "pickup_datetime",
    "dropoff_datetime",
    "RatecodeID",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "congestion_surcharge",
]
# </snippet>

# <snippet name="tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant.py data_assistant_result">
data_assistant_result = context.assistants.onboarding.run(
    batch_request=multi_batch_all_years_batch_request,
    exclude_column_names=exclude_column_names,
)
# </snippet>

# Save your Expectation Suite

# <snippet name="tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant.py get_expectation_suite">
expectation_suite = data_assistant_result.get_expectation_suite(
    expectation_suite_name=expectation_suite_name
)
# </snippet>

# <snippet name="tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant.py save_expectation_suite">
context.add_or_update_expectation_suite(expectation_suite=expectation_suite)
# </snippet>

# Use a SimpleCheckpoint to verify that your new Expectation Suite works.

# <snippet name="tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant.py checkpoint_config">
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

# <snippet name="tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant.py checkpoint">
checkpoint = SimpleCheckpoint(
    f"yellow_tripdata_sample_{expectation_suite_name}",
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


# <snippet name="tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant.py plot_metrics">
data_assistant_result.plot_metrics()
# </snippet>

# <snippet name="tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant.py metrics_by_domain">
data_assistant_result.metrics_by_domain
# </snippet>

# <snippet name="tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant.py plot_expectations_and_metrics">
data_assistant_result.plot_expectations_and_metrics()
# </snippet>

# <snippet name="tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant.py show_expectations_by_domain_type">
data_assistant_result.show_expectations_by_domain_type(
    expectation_suite_name=expectation_suite_name
)
# </snippet>

# <snippet name="tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant.py show_expectations_by_expectation_type">
data_assistant_result.show_expectations_by_expectation_type(
    expectation_suite_name=expectation_suite_name
)
# </snippet>
