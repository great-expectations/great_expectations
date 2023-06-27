# isort:skip_file
import pathlib
from pyfakefs.fake_filesystem import FakeFilesystem

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py imports">
import great_expectations as gx
from great_expectations.checkpoint import Checkpoint

# </snippet>

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py choose context_root_dir">
context_root_dir = "/dbfs/great_expectations/"
# </snippet>

context_root_dir = None

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py set up context">
context = gx.get_context(context_root_dir=context_root_dir)
# </snippet>

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py choose base directory">
base_directory = "/dbfs/example_data/nyctaxi/tripdata/yellow/"
# </snippet>

# For this test script, change base_directory to location where test runner data is located
data_directory = pathlib.Path(pathlib.Path.cwd(), "data")
base_directory = pathlib.Path("/dbfs/data/")
fs = FakeFilesystem()
fs.add_real_directory(source_path=data_directory, target_path=base_directory)

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py add datasource">
dbfs_datasource = context.sources.add_or_update_spark_dbfs(
    name="my_spark_dbfs_datasource",
    base_directory=base_directory,
)
# </snippet>

# unable to successfully mock dbfs, so using filesystem for tests
context.delete_datasource(datasource_name="my_spark_dbfs_datasource")
dbfs_datasource = context.sources.add_or_update_spark_filesystem(
    name="my_spark_dbfs_datasource",
    base_directory=data_directory,
)

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py choose batching regex">
batching_regex = r"yellow_tripdata_(?P<year>\d{4})-(?P<month>\d{2})\.csv\.gz"
# </snippet>

# For this test script, change batching_regex for location where test runner data is located
batching_regex = r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py add data asset">
csv_asset = dbfs_datasource.add_csv_asset(
    name="yellow_tripdata",
    batching_regex=batching_regex,
    header=True,
    infer_schema=True,
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
validator.expect_table_column_count_to_equal(value=18)
# </snippet>

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py save suite">
validator.save_expectation_suite(discard_failed_expectations=False)
# </snippet>

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py checkpoint config">
my_checkpoint_name = "my_databricks_checkpoint"

checkpoint = Checkpoint(
    name=my_checkpoint_name,
    run_name_template="%Y%m%d-%H%M%S-my-run-name-template",
    data_context=context,
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
    action_list=[
        {
            "name": "store_validation_result",
            "action": {"class_name": "StoreValidationResultAction"},
        },
        {"name": "update_data_docs", "action": {"class_name": "UpdateDataDocsAction"}},
    ],
)
# </snippet>

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py add checkpoint config">
context.add_or_update_checkpoint(checkpoint=checkpoint)
# </snippet>

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py run checkpoint">
checkpoint_result = checkpoint.run()
# </snippet>
