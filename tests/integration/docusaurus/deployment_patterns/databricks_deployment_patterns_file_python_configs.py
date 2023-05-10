# isort:skip_file
import os
import pathlib
import shutil

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py imports">
import great_expectations as gx
from great_expectations.checkpoint import SimpleCheckpoint

# </snippet>

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py set up context">
context = gx.get_context()
# </snippet>

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py choose base directory">
base_directory = "/dbfs/example_data/nyctaxi/tripdata/yellow/"
# </snippet>

# For this test script, change base_directory to location where test runner data is located
base_directory = "/dbfs/data/"
os.mkdirs(base_directory)
shutil.copytree(str(pathlib.Path(pathlib.Path.cwd(), "data")), base_directory)

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py add datasource">
dbfs_datasource = context.sources.add_or_update_spark_dbfs(
    name="my_spark_dbfs_datasource",
    base_directory=base_directory,
)
# </snippet>

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py choose batching regex">
batching_regex = r"yellow_tripdata_(?P<year>\d{4})-(?P<month>\d{2})\.csv.gz"
# </snippet>

# For this test script, change batching_regex to location where test runner data is located
batching_regex = r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py add data asset">
csv_asset = dbfs_datasource.add_csv_asset(
    name="yellow_tripdata",
    batching_regex=batching_regex,
    header=True,
)
# </snippet>

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py build batch request">
batch_request = csv_asset.build_batch_request()
# </snippet>

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py get validator">
expectation_suite_name = "insert_your_expectation_suite_name_here"
context.add_or_update_expectation_suite(expectation_suite_name=expectation_suite_name)
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)

print(validator.head())
# </snippet>

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py add expectations">
validator.expect_column_values_to_not_be_null(column="passenger_count")

validator.expect_column_values_to_be_between(
    column="congestion_surcharge", min_value=0, max_value=1000
)
# </snippet>

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py save suite">
validator.save_expectation_suite(discard_failed_expectations=False)
# </snippet>

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py checkpoint config">
my_checkpoint_name = "my_databricks_checkpoint"

checkpoint = SimpleCheckpoint(
    name=my_checkpoint_name,
    config_version=1.0,
    class_name="SimpleCheckpoint",
    run_name_template="%Y%m%d-%H%M%S-my-run-name-template",
    data_context=context,
)
# </snippet>

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py add checkpoint config">
context.add_or_update_checkpoint(checkpoint=checkpoint)
# </snippet>

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py run checkpoint">
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

os.rmdir("/dbfs/data/")
