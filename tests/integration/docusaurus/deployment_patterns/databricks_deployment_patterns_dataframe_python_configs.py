# isort:skip_file
import os
import pandas as pd

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py imports">
import great_expectations as gx
from great_expectations.checkpoint import SimpleCheckpoint

# </snippet>

from great_expectations.core.util import get_or_create_spark_application

spark = get_or_create_spark_application()

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py set up context">
context = gx.get_context()
# </snippet>

context_root_dir = os.path.join(os.getcwd(), "dbfs_temp_directory")
filename = "yellow_tripdata_sample_2019-01.csv"
data_dir = os.path.join(os.path.dirname(context_root_dir), "data")
pandas_df = pd.read_csv(os.path.join(data_dir, filename))
df = spark.createDataFrame(data=pandas_df)

assert len(pandas_df) == df.count() == 10000
assert len(pandas_df.columns) == len(df.columns) == 18

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py datasource config">
dataframe_datasource = context.sources.add_or_update_spark(
    name="my_spark_in_memory_datasource",
)
# </snippet>

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py add datasource config">
context.add_datasource(**my_spark_datasource_config)
# </snippet>

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py create batch request">
batch_request = RuntimeBatchRequest(
    datasource_name="insert_your_datasource_name_here",
    data_connector_name="insert_your_data_connector_name_here",
    data_asset_name="<YOUR_MEANGINGFUL_NAME>",  # This can be anything that identifies this data_asset for you
    batch_identifiers={
        "some_key_maybe_pipeline_stage": "prod",
        "some_other_key_maybe_run_id": f"my_run_name_{datetime.date.today().strftime('%Y%m%d')}",
    },
    runtime_parameters={"batch_data": df},  # Your dataframe goes here
)
# </snippet>
# CODE ^^^^^ ^^^^^

# ASSERTIONS vvvvv vvvvv
assert len(context.list_datasources()) == 1
assert context.list_datasources()[0]["name"] == "insert_your_datasource_name_here"
assert list(context.list_datasources()[0]["data_connectors"].keys()) == [
    "insert_your_data_connector_name_here"
]

assert sorted(
    context.get_available_data_asset_names()["insert_your_datasource_name_here"][
        "insert_your_data_connector_name_here"
    ]
) == sorted([])
# ASSERTIONS ^^^^^ ^^^^^

# 5. Create expectations
# CODE vvvvv vvvvv
# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py get validator">
expectation_suite_name = "insert_your_expectation_suite_name_here"
context.add_or_update_expectation_suite(expectation_suite_name=expectation_suite_name)
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)

print(validator.head())
# </snippet>

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py add expectations">
validator.expect_column_values_to_not_be_null(column="passenger_count")

validator.expect_column_values_to_be_between(
    column="congestion_surcharge", min_value=0, max_value=1000
)
# </snippet>

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py save suite">
validator.save_expectation_suite(discard_failed_expectations=False)
# </snippet>
# CODE ^^^^^ ^^^^^

# ASSERTIONS vvvvv vvvvv
assert context.list_expectation_suite_names() == [expectation_suite_name]
suite = context.get_expectation_suite(expectation_suite_name=expectation_suite_name)
assert len(suite.expectations) == 2
# ASSERTIONS ^^^^^ ^^^^^

# 6. Validate your data (Dataframe)
# CODE vvvvv vvvvv
# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py checkpoint config">
my_checkpoint_name = "insert_your_checkpoint_name_here"
checkpoint_config = {
    "name": my_checkpoint_name,
    "config_version": 1.0,
    "class_name": "SimpleCheckpoint",
    "run_name_template": "%Y%m%d-%H%M%S-my-run-name-template",
}
# </snippet>

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py test checkpoint config">
my_checkpoint = context.test_yaml_config(yaml.dump(checkpoint_config))
# </snippet>

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py add checkpoint config">
context.add_or_update_checkpoint(**checkpoint_config)
# </snippet>

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py run checkpoint">
checkpoint_result = context.run_checkpoint(
    checkpoint_name=my_checkpoint_name,
    validations=[
        {
            "batch_request": batch_request,
            "expectation_suite_name": expectation_suite_name,
        }
    ],
)
# </snippet>
# CODE ^^^^^ ^^^^^

# ASSERTIONS vvvvv vvvvv
assert checkpoint_result.checkpoint_config["name"] == my_checkpoint_name
assert not checkpoint_result.success
first_validation_result_identifier = (
    checkpoint_result.list_validation_result_identifiers()[0]
)
first_run_result = checkpoint_result.run_results[first_validation_result_identifier]
assert (
    first_run_result["validation_result"]["statistics"]["successful_expectations"] == 1
)
assert (
    first_run_result["validation_result"]["statistics"]["unsuccessful_expectations"]
    == 1
)
assert (
    first_run_result["validation_result"]["statistics"]["evaluated_expectations"] == 2
)
# ASSERTIONS ^^^^^ ^^^^^

# 7. Build and view Data Docs (Dataframe)
# CODE vvvvv vvvvv
# None, see guide
# CODE ^^^^^ ^^^^^
# ASSERTIONS vvvvv vvvvv
# Check that validations were written to the store
data_docs_local_site_path = os.path.join(
    root_directory, "uncommitted", "data_docs", "local_site"
)
assert sorted(os.listdir(data_docs_local_site_path)) == sorted(
    ["index.html", "expectations", "validations", "static"]
)
assert os.listdir(os.path.join(data_docs_local_site_path, "validations")) == [
    expectation_suite_name
], "Validation was not written successfully to Data Docs"

run_name = first_run_result["validation_result"]["meta"]["run_id"].run_name
assert (
    len(
        os.listdir(
            os.path.join(
                data_docs_local_site_path,
                "validations",
                expectation_suite_name,
                run_name,
            )
        )
    )
    == 1
), "Validation was not written successfully to Data Docs"
# ASSERTIONS ^^^^^ ^^^^^
