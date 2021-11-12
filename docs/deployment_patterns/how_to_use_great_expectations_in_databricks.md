---
title: How to Use Great Expectations in Databricks
---
import Prerequisites from '../guides/connecting_to_your_data/components/prerequisites.jsx'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import Congratulations from '../guides/connecting_to_your_data/components/congratulations.md'

Great Expectations works well with many types of Databricks workflows. This guide will help you run Great Expectations in [Databricks](https://databricks.com/).

<Prerequisites>

- Have completed Databricks setup including having a running Databricks cluster with attached notebook
- If you are using the file based version of this guide, you'll need to have DBFS set up

</Prerequisites>


There are several ways to set up Databricks, this guide centers around an AWS deployment using Databricks Data Science & Engineering Notebooks and Jobs. If you use Databricks on GCP or Azure and there are steps in this guide that don't work for you please reach out to us.

We will cover a simple configuration to get you up and running quickly, and link to our other guides for more customized configurations. For example:
  - If you want to validate files stored in DBFS select the "File" tab below.
    - If you are using a different file store (e.g. s3, GCS, ABS) take a look at our how-to guides in the "Cloud" section of "Connecting to Your Data" for example configurations. 
  - If you already have a spark dataframe loaded, select the "dataframe" tab. 

This guide parallels notebook workflows from the Great Expectations CLI, so you can optionally prototype your setup with a local sample batch before moving to Databricks. You can also use examples and code from the notebooks that the CLI generates, and indeed much of the examples that follow parallel those notebooks closely.

### 1. Install Great Expectations

Install Great Expectations as a notebook-scoped library by running the following command in your notebook:
```bash
  %pip install great-expectations
  ```

<details>
  <summary>What is a notebook-scoped library?</summary>
A notebook-scoped library is what it sounds like - "custom Python environments that are specific to a notebook." You can also install a library at the cluster or workspace level. See the <a href="https://docs.databricks.com/libraries/index.html">Databricks documentation on Libraries</a> for more information.
</details>

After that we will take care of some imports that will be used later. Choose your configuration options to show applicable imports:

<Tabs
  groupId="file-or-dataframe"
  defaultValue='file'
  values={[
  {label: 'File', value:'file'},
  {label: 'Dataframe', value:'dataframe'},
  ]}>
  <TabItem value="file">

  <Tabs
    groupId="yaml-or-python"
    defaultValue='yaml'
    values={[
    {label: 'Yaml Configs', value:'yaml'},
    {label: 'Python Configs', value:'python'},
    ]}>
  <TabItem value="yaml">

Import the following items:

```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_yaml_configs.py#L3-L10
```
  
  </TabItem>
  <TabItem value="python">

Import the following items:

```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py#L3-L10
```
  
  </TabItem>
  </Tabs>

</TabItem>

<TabItem value="dataframe">

  <Tabs
    groupId="yaml-or-python"
    defaultValue='yaml'
    values={[
    {label: 'Yaml Configs', value:'yaml'},
    {label: 'Python Configs', value:'python'},
    ]}>
  <TabItem value="yaml">

Import the following items:

```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_yaml_configs.py#L3-L13
```
  
  </TabItem>
  <TabItem value="python">

Import the following items:

```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py#L3-L13
```
  
  </TabItem>
  </Tabs>

</TabItem>
</Tabs>

### 2. Set up Great Expectations

In this guide, we will be using the [Databricks File Store (DBFS)](https://docs.databricks.com/data/databricks-file-system.html) for your Metadata Stores and [Data Docs](../reference/data_docs.md) store. This is a simple way to get up and running within the Databricks environment without configuring external resources. For other options for storing data see our "Metadata Stores" and "Data Docs" sections in the "How to Guides" for "Setting up Great Expectations."

  <details>
    <summary>What is DBFS?</summary>
    Paraphrased from the Databricks docs: DBFS is a distributed file system mounted into a Databricks workspace and available on Databricks clusters. Files on DBFS can be written and read as if they were on a local filesystem, just by <a href="https://docs.databricks.com/data/databricks-file-system.html#local-file-apis">adding the /dbfs/ prefix to the path</a>. It is also persisted to object storage, so you won’t lose data after you terminate a cluster. See the Databricks documentation for best practices including mounting object stores.
  </details>

Run the following code to set up a [Data Context](../reference/data_context.md) using the appropriate defaults: 

#### TODO: retrieve this code from databricks_deployment_patterns.py
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns.py#L1-L2
```

```python
root_directory = "/dbfs/great_expectations/"

data_context_config = DataContextConfig(
    store_backend_defaults=FilesystemStoreBackendDefaults(
        root_directory=root_directory
    ),
)
context = BaseDataContext(project_config=data_context_config)
```

### 3. Prepare your data

<Tabs
  groupId="file-or-dataframe"
  defaultValue='file'
  values={[
  {label: 'File', value:'file'},
  {label: 'Dataframe', value:'dataframe'},
  ]}>
  <TabItem value="file">

# TODO: Copy over 3 files of data instead of just the 1

We will use our familiar NYC taxi yellow cab data, which is available as sample data in Databricks. Let's copy some example csv data to our DBFS folder for easier access using [dbutils](https://docs.databricks.com/dev-tools/databricks-utils.html):

```python
dbutils.fs.cp(
  "/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-01.csv.gz",
  "/example_data/nyctaxi/tripdata/yellow/yellow_tripdata_2019-01.csv.gz"
)
```

And then unzip it using the %sh [magic command](https://docs.databricks.com/notebooks/notebooks-use.html#language-magic):
```bash
%sh gzip -d /dbfs/example_data/nyctaxi/tripdata/yellow/yellow_tripdata_2019-01.csv.gz
```

</TabItem>
<TabItem value="dataframe">

We will use our familiar NYC taxi yellow cab data, which is available as sample data in Databricks. Run the following code in your notebook to load a month of data as a dataframe:

```python
df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-01.csv.gz")
```

</TabItem>
</Tabs>



### 4. Connect to your data

<Tabs
  groupId="file-or-dataframe"
  defaultValue='file'
  values={[
  {label: 'File', value:'file'},
  {label: 'Dataframe', value:'dataframe'},
  ]}>
  <TabItem value="file">

Here we add a [Datasource and Data Connector](../reference/datasources.md) by running the following code. In this example, we are using a `InferredAssetFilesystemDataConnector` so that we can access and validate our file as a `Data Asset`, but instead you may use any of the other types of `Data Connectors`, `Partitioners`, `Splitters`, `Samplers`, `Queries` available to you (check out our documentation on "Connecting to your data" for more information).

#### TODO: retrieve this code from databricks_deployment_patterns.py

#### TODO: Change this to yaml

```python
my_spark_datasource_config = {
    "name": "insert_your_datasource_name_here",
    "class_name": "Datasource",
    "execution_engine": {"class_name": "SparkDFExecutionEngine"},
    "data_connectors": {
        "insert_your_data_connector_name_here": {
            "module_name": "great_expectations.datasource.data_connector",
            "class_name": "InferredAssetFilesystemDataConnector",
            "base_directory": "/dbfs/example_data/nyctaxi/tripdata/yellow/",
            "default_regex": {
                "pattern": r"(.*)",
                "group_names": ["data_asset_name"]
            },
        }
    },
}

context.test_yaml_config(yaml.dump(my_spark_datasource_config))

context.add_datasource(**my_spark_datasource_config)
```

Then we create a `BatchRequest` using the `DataAsset` we configured earlier to use as a sample of data when creating Expectations:
```python
batch_request = BatchRequest(
    datasource_name="insert_your_datasource_name_here",
    data_connector_name="insert_your_data_connector_name_here",
    data_asset_name="yellow_tripdata_2019-01.csv",
)
```

</TabItem>
<TabItem value="dataframe">

Here we add a [Datasource and Data Connector](../reference/datasources.md) by running the following code. In this example, we are using a `RuntimeDataConnector` so that we can access and validate our loaded dataframe, but instead you may use any of the other types of `Data Connectors`, `Partitioners`, `Splitters`, `Samplers`, `Queries` available to you (check out our documentation on "Connecting to your data" for more information).
  
#### TODO: retrieve this code from databricks_deployment_patterns.py
#### TODO: Should this be yaml instead of python? Probably for compactness.
```python
my_spark_datasource_config = {
    "name": "insert_your_datasource_name_here",
    "class_name": "Datasource",
    "execution_engine": {"class_name": "SparkDFExecutionEngine"},
    "data_connectors": {
        "insert_your_data_connector_name_here": {
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
#### TODO: retrieve this code from databricks_deployment_patterns.py, substitute the data_asset_name
```python
batch_request = RuntimeBatchRequest(
    datasource_name="insert_your_datasource_name_here",
    data_connector_name="insert_your_data_connector_name_here",
    data_asset_name="<YOUR_MEANGINGFUL_NAME>",  # This can be anything that identifies this data_asset for you
    batch_identifiers={
        "some_key_maybe_pipeline_stage": "prod",
        "some_other_key_maybe_run_id": f"my_run_name_{datetime.date.today().strftime("%Y%m%d")}",
    },
    runtime_parameters={"batch_data": df},  # Your dataframe goes here
)
```
  
</TabItem>
</Tabs>


<Congratulations />
Now let's keep going to create an Expectation Suite and validate our data.

### 5. Create Expectations

Here we will use a `Validator` to interact with our batch of data and generate an `Expectation Suite`. 

This is the same method used in the CLI interactive mode notebook accessed via `great_expectations --v3-api suite new --interactive`.

First we create the suite and get a validator:
#### TODO: retrieve this code from databricks_deployment_patterns.py
```python
expectation_suite_name = "insert_your_expectation_suite_name_here"
context.create_expectation_suite(
    expectation_suite_name=expectation_suite_name, overwrite_existing=True
)
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)
```

Then we use the `Validator` to add a few Expectations:
#### TODO: retrieve this code from databricks_deployment_patterns.py
```python
validator.expect_column_values_to_not_be_null(column="passenger_count")
```
```python
validator.expect_column_values_to_be_between(column="congestion_surcharge", min_value=0, max_value=1000)
```

Finally we save our suite to our expectation store:
#### TODO: retrieve this code from databricks_deployment_patterns.py
```python
validator.save_expectation_suite(discard_failed_expectations=False)
```

### 6. Validate your data

<Tabs
  groupId="file-or-dataframe"
  defaultValue='file'
  values={[
  {label: 'File', value:'file'},
  {label: 'Dataframe', value:'dataframe'},
  ]}>
  <TabItem value="file">

  Here we will create and store a [Checkpoint](../reference/checkpoints_and_actions.md) for our batch, which we can use to [Validate](../reference/validation.md) and run post-validation actions. Check out our docs on "Validating your data" for more info on how to customize your Checkpoints.

  First we create the Checkpoint configuration mirroring our `batch_request` configuration above and using the Expectation Suite we created:

  ```python
  checkpoint_config = """
    name: insert_your_checkpoint_name_here
    config_version: 1
    class_name: SimpleCheckpoint
    run_name_template: "%Y%m%d-%H%M%S-my-run-name-template"
    validations:
      - batch_request:
          datasource_name: insert_your_datasource_name_here
          data_connector_name: insert_your_data_connector_name_here
          data_asset_name: yellow_tripdata_2019-01.csv
        expectation_suite_name: insert_your_expectation_suite_name_here
    """
  ```

  Then we test our syntax using `test_yaml_config`:
  #### TODO: retrieve this code from databricks_deployment_patterns.py
  ```python
  context.test_yaml_config(yaml_config=checkpoint_config)
  ```

  If all is well, we add the Checkpoint:
  #### TODO: retrieve this code from databricks_deployment_patterns.py
  ```python
  context.add_checkpoint(**yaml.load(checkpoint_config))
  ```
  
  Finally we run the Checkpoint:
  #### TODO: retrieve this code from databricks_deployment_patterns.py
  ```python
  context.run_checkpoint(checkpoint_name=my_checkpoint_name)
  ```
  </TabItem>

<TabItem value="dataframe">

Here we will create and store a Checkpoint with no defined validations, then pass in our dataframe at runtime.

First we create the Checkpoint configuration
#### TODO: retrieve this code from databricks_deployment_patterns.py
```python

my_checkpoint_name = "insert_your_checkpoint_name_here"
yaml_config = f"""
name: {my_checkpoint_name}
config_version: 1.0
class_name: SimpleCheckpoint
run_name_template: "%Y%m%d-%H%M%S-my-run-name-template"
"""
print(yaml_config)
```

Then we test our syntax using `test_yaml_config`
#### TODO: retrieve this code from databricks_deployment_patterns.py
```python
my_checkpoint = context.test_yaml_config(yaml_config=yaml_config)
```
Note that we get a message that the Checkpoint contains no validations. See below, we will pass those in at runtime.

If all is well, we add the Checkpoint:
#### TODO: retrieve this code from databricks_deployment_patterns.py
```python
context.add_checkpoint(**yaml.load(yaml_config))
```

Finally we run it with a validation defined using the Batch Request containing a reference to our dataframe and our Expectation Suite name:
#### TODO: retrieve this code from databricks_deployment_patterns.py
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

</TabItem>
</Tabs>


### 7. Build and view Data Docs

Since we used a `SimpleCheckpoint`, our Checkpoint already contained an `UpdateDataDocsAction` which rendered our [Data Docs](../reference/data_docs.md) from the validation we just ran. That means our Data Docs store will contain a new rendered validation result. 

<details>
<summary>How do I customize these actions?</summary>
  Check out our docs on "Validating your data" for more info on how to customize your Checkpoints.
  
  Also, to see the full Checkpoint configuration, you can run: `print(my_checkpoint.get_substituted_config().to_yaml_str())`
</details>

Since we used DBFS for our Data Docs store, we need to download our data docs locally to view them. If you use a different store, you can host your data docs in a place where they can be accessed directly by your team. To learn more, see our documentation on Data Docs for other locations e.g. [filesystem](../guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_a_filesystem.md), [s3](../guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_amazon_s3.md), [GCS](../guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.md), [ABS](../guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_azure_blob_storage.md).

Run the following Databricks CLI command to download your data docs (replacing the paths as appropriate), then open the local copy of `index.html` to view your updated Data Docs: 
```bash
databricks fs cp -r dbfs:/great_expectations/uncommitted/data_docs/local_site/ great_expectations/uncommitted/data_docs/local_site/
```

### 8. Congratulations!
You've successfully validated your data with Great Expectations using Databricks and viewed the resulting human-readable Data Docs. Check out our other guides for more customization options and happy validating!

View the full script used in this page on GitHub:

- [databricks_deployment_patterns.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns.py)
