# isort:skip_file
import pandas as pd
import pathlib

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py imports">
import great_expectations as gx
from great_expectations.checkpoint import SimpleCheckpoint

# </snippet>

from great_expectations.core.util import get_or_create_spark_application

spark = get_or_create_spark_application()

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py set up context">
context = gx.get_context()
# </snippet>

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py add datasource">
dataframe_datasource = context.sources.add_or_update_spark(
    name="my_spark_in_memory_datasource",
)
csv_file_path = "/path/to/data/directory/yellow_tripdata_2020-08.csv"
# </snippet>

filename = "yellow_tripdata_sample_2020-08.csv"
csv_file_path = pathlib.Path("./data", filename).resolve()
pandas_df = pd.read_csv(csv_file_path)
df = spark.createDataFrame(data=pandas_df)

assert len(pandas_df) == df.count() == 10000
assert len(pandas_df.columns) == len(df.columns) == 18

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py add data asset">
df = spark.read.csv(csv_file_path, header=True)
dataframe_asset = dataframe_datasource.add_dataframe_asset(
    name="yellow_tripdata",
    dataframe=df,
)
# </snippet>

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py build batch request">
batch_request = dataframe_asset.build_batch_request()
# </snippet>

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

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py checkpoint config">
my_checkpoint_name = "my_databricks_checkpoint"

checkpoint = SimpleCheckpoint(
    name=my_checkpoint_name,
    config_version=1.0,
    class_name="SimpleCheckpoint",
    run_name_template="%Y%m%d-%H%M%S-my-run-name-template",
    data_context=context,
)
# </snippet>

# <snippet name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py add checkpoint config">
context.add_or_update_checkpoint(checkpoint=checkpoint)
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
