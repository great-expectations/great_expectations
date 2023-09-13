"""Example Script: How to create an Expectation Suite with the Missingness Data Assistant

This example script is intended for use in documentation on how to use an Missingness Data Assistant to create
an Expectation Suite.

Assert statements are included to ensure that if the behavior shown in this script breaks it will not pass
tests and will be updated.  These statements can be ignored by users.

Comments with the tags `<snippet>` and `</snippet>` are used to ensure that if this script is updated
the snippets that are specified for use in documentation are maintained.  These comments can be ignored by users.

--documentation--
    https://docs.greatexpectations.io/docs/guides/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_missingness_data_assistant
"""
import great_expectations as gx
from great_expectations.core.batch import BatchRequest
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.datasource.fluent.interfaces import DataAsset

yaml = YAMLHandler()

context = gx.get_context()

# Configure your datasource (if you aren't using one that already exists)

# <snippet name="tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_missingness_data_assistant.py datasource_config">
datasource = context.sources.add_pandas_filesystem(
    name="taxi_multi_batch_datasource",  # custom name to assign to new datasource, can be used to retrieve datasource later
    base_directory="./data",  # replace with your data directory
)
# </snippet>

# <snippet name="tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_missingness_data_assistant.py validator">
validator = datasource.read_csv(
    asset_name="all_years",  # custom name to assign to the asset, can be used to retrieve asset later
    batching_regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
)
# </snippet>

# Run the Missingness Assistant

# <snippet name="tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_missingness_data_assistant.py exclude_column_names">
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

# <snippet name="tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_missingness_data_assistant.py data_assistant_result">
data_assistant_result = context.assistants.missingness.run(
    validator=validator,
    exclude_column_names=exclude_column_names,
)
# </snippet>

# Save your Expectation Suite

# <snippet name="tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_missingness_data_assistant.py save_validator">
validator.expectation_suite = data_assistant_result.get_expectation_suite(
    expectation_suite_name="my_custom_expectation_suite_name"
)
validator.save_expectation_suite(discard_failed_expectations=False)
# </snippet>

# Use a Checkpoint to verify that your new Expectation Suite works.

# <snippet name="tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_missingness_data_assistant.py checkpoint">

checkpoint = context.add_or_update_checkpoint(
    name="yellow_tripdata_sample_all_years_checkpoint",
    validator=validator,
)
checkpoint_result = checkpoint.run()

assert checkpoint_result["success"] is True
# </snippet>

# If you are using code from this script as part of a Jupyter Notebook, uncommenting and running the
# following lines will open your Data Docs for the `checkpoint`'s results:

# context.build_data_docs()
# validation_result_identifier = checkpoint_result.list_validation_result_identifiers()[0]
# context.open_data_docs(resource_identifier=validation_result_identifier)


# <snippet name="tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_missingness_data_assistant.py plot_metrics">
data_assistant_result.plot_metrics()
# </snippet>


# <snippet name="tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_missingness_data_assistant.py show_expectations_by_expectation_type">
data_assistant_result.show_expectations_by_expectation_type()
# </snippet>
