---
title: How to Use Great Expectations in Databricks
---
import Prerequisites from '../guides/connecting_to_your_data/components/prerequisites.jsx'

Great Expectations works well with many types of Databricks workflows. This guide will help you run Great Expectations in [Databricks](https://databricks.com/).

<Prerequisites></Prerequisites>

There are several ways to set up Databricks, this guide centers around an AWS deployment using Databricks Data Science & Engineering Notebooks and Jobs. If you use Databricks on GCP or Azure and there are steps in this guide that don't work for you please reach out to us.

We will cover a simple configuration to get you up and running quickly, and link to our other guides for more customized configurations.

### 1. Install Great Expectations

There are two ways to install Great Expectations:
- [Cluster scoped library](https://docs.databricks.com/libraries/cluster-libraries.html)
- [Notebook-scoped library](https://docs.databricks.com/libraries/notebooks-python-libraries.html)

Here we will install Great Expectations as a notebook-scoped library by running the following command in your notebook:
```bash
  %pip install great-expectations
  ```

After that we will take care of some imports that will be used later:
```python
from ruamel import yaml
from great_expectations.core.batch import RuntimeBatchRequest
```

### 2. Set up Great Expectations

In this guide, we will be using the [Databricks File Store (DBFS)](https://docs.databricks.com/data/databricks-file-system.html) for your Metadata Stores and [Data Docs](../reference/data_docs.md) store. 

<details>
  <summary>What is DBFS?</summary>
Paraphrased from Databricks docs: DBFS is a distributed file system mounted into a Databricks workspace and available on Databricks clusters. Files on DBFS can be written and read as if they were on a local filesystem, just by <a href="https://docs.databricks.com/data/databricks-file-system.html#local-file-apis">adding the /dbfs/ prefix to the path</a>. It is also persisted to object storage, so you won’t lose data after you terminate a cluster.
</details>

Run the following code to set up a [Data Context](../reference/data_context.md) using the appropriate defaults: 
# TODO: retrieve this code from databricks_deployment_patterns.py
```python
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig, FilesystemStoreBackendDefaults

root_directory = "/dbfs/great_expectations/"

data_context_config = DataContextConfig(
    store_backend_defaults=FilesystemStoreBackendDefaults(
        root_directory=root_directory
    ),
)
context = BaseDataContext(project_config=data_context_config)
```

### 3. Load your data

We will use our familiar NYC taxi yellow cab data, which is available as sample data in Databricks. Run the following code in your notebook to load a month of data:

```python
df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-01.csv.gz")
```

### 4. Connect to your data

We will add a [Datasource and Data Connector](../reference/datasources.md) by running the following code. In this example, we are using a `RuntimeDataConnector` so that we can validate our loaded dataframe, but instead you may use any of the other types of Data Connectors available to you (check out our documentation on "Connecting to your data").

# TODO: retrieve this code from databricks_deployment_patterns.py
# TODO: Should this be yaml instead of python? Probably for compactness.
```python
my_spark_datasource_config = {
    "name": "insert_your_datasource_name_here",
    "class_name": "Datasource",
    "execution_engine": {"class_name": "SparkDFExecutionEngine"},
    "data_connectors": {
        "insert_your_runtime_data_connector_name_here": {
            "module_name": "great_expectations.datasource.data_connector",
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": [
                "some_key_maybe_pipeline_stage",
                "some_other_key_maybe_run_id",
            ],
        }
    },
}

context.test_yaml_config(yaml.dump(my_spark_datasource_config))

context.add_datasource(**my_spark_datasource_config)
```

Next we will create a `RuntimeBatchRequest` to reference our loaded dataframe and add metadata:
# TODO: retrieve this code from databricks_deployment_patterns.py, substituting the data_asset_name
```python
batch_request_from_dataframe = RuntimeBatchRequest(
    datasource_name="insert_your_datasource_name_here",
    data_connector_name="insert_your_runtime_data_connector_name_here",
    data_asset_name="<YOUR_MEANGINGFUL_NAME>",  # This can be anything that identifies this data_asset for you
    batch_identifiers={
        "some_key_maybe_pipeline_stage": "prod",
        "some_other_key_maybe_run_id": f"my_run_name_{datetime.date.today().strftime("%Y%m%d")}",
    },
    runtime_parameters={"batch_data": df},  # Your dataframe goes here
)
```


### 4. Create expectations

Here we will use a `Validator` to interact with our batch of data and generate an `Expectation Suite` (like the method used in the CLI interactive mode notebook `great_expectations --v3-api suite new --interactive`).

First we create the suite and get a validator:
# TODO: retrieve this code from databricks_deployment_patterns.py, substituting the data_asset_name
```python
expectation_suite_name = "insert_your_expectation_suite_name_here"
context.create_expectation_suite(
    expectation_suite_name=expectation_suite_name, overwrite_existing=True
)
validator = context.get_validator(
    batch_request=batch_request_from_dataframe,
    expectation_suite_name=expectation_suite_name,
)
```

Then we use the `Validator` to add a few expectations:
# TODO: retrieve this code from databricks_deployment_patterns.py, substituting the data_asset_name
```python
validator.expect_column_values_to_not_be_null(column="passenger_count")
```
```python
validator.expect_column_values_to_be_between(column="congestion_surcharge", min_value=0, max_value=1000)
```

Finally we save our suite to our expectation store:
```python
validator.save_expectation_suite(discard_failed_expectations=False)
```

### 5. Validate your data

Here we will create and store a checkpoint with no defined validations, then pass in our dataframe at runtime.

First we create the checkpoint configuration
# TODO: retrieve this code from databricks_deployment_patterns.py, substituting the data_asset_name
```python

my_checkpoint_name = "my_checkpoint"
yaml_config = f"""
name: {my_checkpoint_name}
config_version: 1.0
class_name: SimpleCheckpoint
run_name_template: "%Y%m%d-%H%M%S-my-run-name-template"
"""
print(yaml_config)
```

Then we test our syntax using `test_yaml_config`
# TODO: retrieve this code from databricks_deployment_patterns.py, substituting the data_asset_name
```python
my_checkpoint = context.test_yaml_config(yaml_config=yaml_config)
```

If all is well, we add the checkpoint:
# TODO: retrieve this code from databricks_deployment_patterns.py, substituting the data_asset_name
```python
context.add_checkpoint(**yaml.load(yaml_config))
```

Finally we run it with a validation defined using the batch request containing a reference to our dataframe and our expectation suite name:
# TODO: retrieve this code from databricks_deployment_patterns.py, substituting the data_asset_name
```python
context.run_checkpoint(
    checkpoint_name=my_checkpoint_name,
    validations=[
        {
            "batch_request": batch_request_from_dataframe,
            "expectation_suite_name": expectation_suite_name
        }
    ]
)
```

### 6. Build and view Data Docs

Since we used a `SimpleCheckpoint`, it already contained an `UpdateDataDocsAction` so our Data Docs store will contain a new rendered validation result.

To see the full Checkpoint configuration, you can run:
# TODO: retrieve this code from databricks_deployment_patterns.py, substituting the data_asset_name
```python
print(my_checkpoint.get_substituted_config().to_yaml_str())
```

Since we used DBFS for our Data Docs store, we need to download our data docs locally to view them. If you use a different store, you can host your data docs in a place where they can be accessed directly by your team. To learn more, see our documentation on Data Docs for other locations to use e.g. [filesystem](../guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_a_filesystem.md), [s3](../guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_amazon_s3.md), [GCS](../guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.md), [ABS](../guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_azure_blob_storage.md).

Run the following databricks CLI command to download your data docs (replacing the paths as appropriate), then open the local copy of `index.html`: 
```bash
databricks fs cp -r dbfs:/great_expectations/uncommitted/data_docs/local_site/ great_expectations/uncommitted/data_docs/local_site/
```
