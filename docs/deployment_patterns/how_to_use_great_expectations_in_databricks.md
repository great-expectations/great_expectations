---
title: How to Use Great Expectations in Databricks
---
import Prerequisites from './components/deployment_pattern_prerequisites.jsx'
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
  - If you want to validate files stored in DBFS select one of the "File" tabs below. You can also [watch our video walkthrough](https://youtu.be/9mIsmyuJzhQ) of these steps.
    - If you are using a different file store (e.g. s3, GCS, ABS) take a look at our how-to guides in the "Cloud" section of "Connecting to Your Data" for example configurations.
  - If you already have a spark dataframe loaded, select one of the "Dataframe" tabs below.

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
  groupId="file-or-dataframe-pandas-or-yaml"
  defaultValue='file'
  values={[
  {label: 'File-yaml', value:'file-yaml'},
  {label: 'File-python', value:'file-python'},
  {label: 'Dataframe-yaml', value:'dataframe-yaml'},
  {label: 'Dataframe-python', value:'dataframe-python'},
  ]}>
  <TabItem value="file-yaml">

```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_yaml_configs.py#L3-L10
```

  </TabItem>

  <TabItem value="file-python">

```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py#L3-L10
```

  </TabItem>

  <TabItem value="dataframe-yaml">

```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_yaml_configs.py#L3-L13
```

  </TabItem>

  <TabItem value="dataframe-python">

```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py#L3-L13
```

  </TabItem>
</Tabs>


### 2. Set up Great Expectations

In this guide, we will be using the [Databricks File Store (DBFS)](https://docs.databricks.com/data/databricks-file-system.html) for your Metadata Stores and [Data Docs](../reference/data_docs.md) store. This is a simple way to get up and running within the Databricks environment without configuring external resources. For other options for storing data see our "Metadata Stores" and "Data Docs" sections in the "How to Guides" for "Setting up Great Expectations."

  <details>
    <summary>What is DBFS?</summary>
    Paraphrased from the Databricks docs: DBFS is a distributed file system mounted into a Databricks workspace and available on Databricks clusters. Files on DBFS can be written and read as if they were on a local filesystem, just by <a href="https://docs.databricks.com/data/databricks-file-system.html#local-file-apis">adding the /dbfs/ prefix to the path</a>. It is also persisted to object storage, so you won’t lose data after you terminate a cluster. See the Databricks documentation for best practices including mounting object stores.
  </details>

Run the following code to set up a [Data Context](../reference/data_context.md) in code using the appropriate defaults:

<details>
  <summary>What is an "in code" Data Context?</summary>
When you don't have easy access to a file system, instead of defining your Data Context via great_expectations.yml you can do so by instantiating a BaseDataContext with a config. Take a look at our how-to guide to learn more: <a href='/docs/guides/setup/configuring_data_contexts/how_to_instantiate_a_data_context_without_a_yml_file'>How to instantiate a Data Context without a yml file</a>. In Databricks, you can do either since you have access to a filesystem - we've simply shown the in code version here for simplicity.

</details>

<details>
  <summary>What do we mean by "root_directory" in the below code?</summary>
The root_directory here refers to the directory that will hold the data for your Metadata Stores (e.g. Expectations Store, Validations Store, Data Docs Store). We are using the FilesystemStoreBackendDefaults since DBFS acts sufficiently like a filesystem that we can simplify our configuration with these defaults. These are all more configurable than is shown in this simple guide, so for other options please see our "Metadata Stores" and "Data Docs" sections in the "How to Guides" for "Setting up Great Expectations."
</details>

<Tabs
  groupId="file-or-dataframe-pandas-or-yaml"
  defaultValue='file'
  values={[
  {label: 'File-yaml', value:'file-yaml'},
  {label: 'File-python', value:'file-python'},
  {label: 'Dataframe-yaml', value:'dataframe-yaml'},
  {label: 'Dataframe-python', value:'dataframe-python'},
  ]}>
  <TabItem value="file-yaml">

```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_yaml_configs.py#L21
```
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_yaml_configs.py#L26-L31
```

  </TabItem>

  <TabItem value="file-python">

```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py#L21
```
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py#L26-L31
```

  </TabItem>

  <TabItem value="dataframe-yaml">

```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_yaml_configs.py#L28
```
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_yaml_configs.py#L33-L38
```

  </TabItem>

  <TabItem value="dataframe-python">

```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py#L32
```
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py#L37-L42
```

  </TabItem>
</Tabs>

### 3. Prepare your data

<Tabs
  groupId="file-or-dataframe-pandas-or-yaml"
  defaultValue='file'
  values={[
  {label: 'File-yaml', value:'file-yaml'},
  {label: 'File-python', value:'file-python'},
  {label: 'Dataframe-yaml', value:'dataframe-yaml'},
  {label: 'Dataframe-python', value:'dataframe-python'},
  ]}>
  <TabItem value="file-yaml">

We will use our familiar NYC taxi yellow cab data, which is available as sample data in Databricks. Let's copy some example csv data to our DBFS folder for easier access using [dbutils](https://docs.databricks.com/dev-tools/databricks-utils.html):

```python
# Copy 3 months of data
for month in range(1, 4):
    dbutils.fs.cp(
      f"/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-0{month}.csv.gz",
      f"/example_data/nyctaxi/tripdata/yellow/yellow_tripdata_2019-0{month}.csv.gz"
    )
```

  </TabItem>

  <TabItem value="file-python">

We will use our familiar NYC taxi yellow cab data, which is available as sample data in Databricks. Let's copy some example csv data to our DBFS folder for easier access using [dbutils](https://docs.databricks.com/dev-tools/databricks-utils.html):

```python
# Copy 3 months of data
for month in range(1, 4):
    dbutils.fs.cp(
      f"/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-0{month}.csv.gz",
      f"/example_data/nyctaxi/tripdata/yellow/yellow_tripdata_2019-0{month}.csv.gz"
    )
```

  </TabItem>

  <TabItem value="dataframe-yaml">

We will use our familiar NYC taxi yellow cab data, which is available as sample data in Databricks. Run the following code in your notebook to load a month of data as a dataframe:

```python
df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-01.csv.gz")
```

  </TabItem>

  <TabItem value="dataframe-python">

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
  groupId="file-or-dataframe-pandas-or-yaml"
  defaultValue='file'
  values={[
  {label: 'File-yaml', value:'file-yaml'},
  {label: 'File-python', value:'file-python'},
  {label: 'Dataframe-yaml', value:'dataframe-yaml'},
  {label: 'Dataframe-python', value:'dataframe-python'},
  ]}>
  <TabItem value="file-yaml">

Here we add a [Datasource and Data Connector](../reference/datasources.md) by running the following code. In this example, we are using a `InferredAssetDBFSDataConnector` so that we can access and validate each of our files as a `Data Asset`, but instead you may use any of the other types of `Data Connectors`, `Partitioners`, `Splitters`, `Samplers`, `Queries` available to you (check out our documentation on "Connecting to your data" for more information).

<details>
  <summary>What does this configuration contain?</summary>
Here we are setting up a Datasource using a SparkDFExecutionEngine (which loads the data into a spark dataframe to process the validations). We also configure a Data Connector using a few helpful parameters. Here is a summary of some key parameters, but you can also find more information in our "Connecting to your data" docs, especially the "Core skills" and "Filesystem" sections:
  <ul>
    <li>class_name: Here we reference one of the two DBFS data connectors InferredAssetDBFSDataConnector (ConfiguredAssetDBFSDataConnector is also available) which handle the translation from /dbfs/ to dbfs:/ style paths for you. For more information on the difference between Configured/Inferred, see <a href='/docs/guides/connecting_to_your_data/how_to_choose_which_dataconnector_to_use'>How to choose which DataConnector to use.</a></li>
    <li>base_directory: Where your files are located, here we reference the file path in DBFS we copied our data to earlier.</li>
    <li>glob_directive: This allows you to select files within that base_directory that match a <a href="https://docs.python.org/3/library/glob.html">glob</a> pattern.</li>
    <li>default_regex: Here we specify the group_names corresponding to the groups in the regex defined in the pattern - we can use these later to filter so that we can apply our Checkpoint to a specific Batch (using this configuration, each file is a Batch).</li>
  </ul>
</details>

Datasource configuration:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_yaml_configs.py#L50-L69
```

Check the Datasource:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_yaml_configs.py#L92
```

Add the Datasource:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_yaml_configs.py#L94
```

Then we create a `BatchRequest` using the `DataAsset` we configured earlier to use as a sample of data when creating Expectations:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_yaml_configs.py#L96-L106
```

  </TabItem>

  <TabItem value="file-python">

Here we add a [Datasource and Data Connector](../reference/datasources.md) by running the following code. In this example, we are using a `InferredAssetDBFSDataConnector` so that we can access and validate each of our files as a `Data Asset`, but instead you may use any of the other types of `Data Connectors`, `Partitioners`, `Splitters`, `Samplers`, `Queries` available to you (check out our documentation on "Connecting to your data" for more information).

<details>
  <summary>What does this configuration contain?</summary>
Here we are setting up a Datasource using a SparkDFExecutionEngine (which loads the data into a spark dataframe to process the validations). We also configure a Data Connector using a few helpful parameters. Here is a summary of some key parameters, but you can also find more information in our "Connecting to your data" docs, especially the "Core skills" and "Filesystem" sections:
  <ul>
    <li>class_name: Here we reference one of the two DBFS data connectors InferredAssetDBFSDataConnector (ConfiguredAssetDBFSDataConnector is also available) which handle the translation from /dbfs/ to dbfs:/ style paths for you. For more information on the difference between Configured/Inferred, see <a href='/docs/guides/connecting_to_your_data/how_to_choose_which_dataconnector_to_use'>How to choose which DataConnector to use.</a></li>
    <li>base_directory: Where your files are located, here we reference the file path in DBFS we copied our data to earlier.</li>
    <li>glob_directive: This allows you to select files within that base_directory that match a <a href="https://docs.python.org/3/library/glob.html">glob</a> pattern.</li>
    <li>default_regex: Here we specify the group_names corresponding to the groups in the regex defined in the pattern - we can use these later to filter so that we can apply our Checkpoint to a specific Batch (using this configuration, each file is a Batch).</li>
  </ul>
</details>

Datasource configuration:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py#L51-L74
```

Check the Datasource:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py#L98
```

Add the Datasource:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py#L100
```

Then we create a `BatchRequest` using the `DataAsset` we configured earlier to use as a sample of data when creating Expectations:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py#L102-L112
```

  </TabItem>

  <TabItem value="dataframe-yaml">

Here we add a [Datasource and Data Connector](../reference/datasources.md) by running the following code. In this example, we are using a `RuntimeDataConnector` so that we can access and validate our loaded dataframe, but instead you may use any of the other types of `Data Connectors`, `Partitioners`, `Splitters`, `Samplers`, `Queries` available to you (check out our documentation on "Connecting to your data" for more information).

Datasource configuration:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_yaml_configs.py#L68-L80
```

Check the Datasource:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_yaml_configs.py#L82
```

Add the Datasource:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_yaml_configs.py#L84
```

Then we create a `BatchRequest` using the `DataAsset` we configured earlier to use as a sample of data when creating Expectations:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_yaml_configs.py#L86-L95
```


  </TabItem>

  <TabItem value="dataframe-python">

Here we add a [Datasource and Data Connector](../reference/datasources.md) by running the following code. In this example, we are using a `RuntimeDataConnector` so that we can access and validate our loaded dataframe, but instead you may use any of the other types of `Data Connectors`, `Partitioners`, `Splitters`, `Samplers`, `Queries` available to you (check out our documentation on "Connecting to your data" for more information).

Datasource configuration:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py#L72-L86
```

Check the Datasource:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py#L88
```

Add the Datasource:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py#L90
```

Then we create a `BatchRequest` using the `DataAsset` we configured earlier to use as a sample of data when creating Expectations:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py#L92-L101
```

  </TabItem>
</Tabs>


<Congratulations />
Now let's keep going to create an Expectation Suite and validate our data.

### 5. Create Expectations

Here we will use a `Validator` to interact with our batch of data and generate an `Expectation Suite`.

Each time we evaluate an Expectation (e.g. via `validator.expect_*`), the Expectation configuration is stored in the Validator. When you have run all of the Expectations you want for this dataset, you can call `validator.save_expectation_suite()` to save all of your Expectation configurations into an Expectation Suite for later use in a checkpoint.

This is the same method of interactive Expectation Suite editing used in the CLI interactive mode notebook accessed via `great_expectations suite new --interactive`. For more information, see our documentation on [How to create and edit Expectations with instant feedback from a sample Batch of data](../../docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data.md). You can also create Expectation Suites using a [profiler](../guides/expectations/how_to_create_and_edit_expectations_with_a_profiler.md) to automatically create expectations based on your data or [manually using domain knowledge and without inspecting data directly](../guides/expectations/how_to_create_and_edit_expectations_based_on_domain_knowledge_without_inspecting_data_directly.md). 

<Tabs
  groupId="file-or-dataframe-pandas-or-yaml"
  defaultValue='file'
  values={[
  {label: 'File-yaml', value:'file-yaml'},
  {label: 'File-python', value:'file-python'},
  {label: 'Dataframe-yaml', value:'dataframe-yaml'},
  {label: 'Dataframe-python', value:'dataframe-python'},
  ]}>
  <TabItem value="file-yaml">

First we create the suite and get a `Validator`:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_yaml_configs.py#L153-L162
```

Then we use the `Validator` to add a few Expectations:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_yaml_configs.py#L164
```
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_yaml_configs.py#L166-L168
```

Finally we save our Expectation Suite (all of the unique Expectation Configurations from each run of `validator.expect_*`) to our Expectation Store:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_yaml_configs.py#L170
```

  </TabItem>

  <TabItem value="file-python">

First we create the suite and get a `Validator`:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py#L159-L168
```

Then we use the `Validator` to add a few Expectations:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py#L170
```
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py#L172-L174
```

Finally we save our Expectation Suite (all of the unique Expectation Configurations from each run of `validator.expect_*`) to our Expectation Store:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py#L173
```

  </TabItem>
  <TabItem value="dataframe-yaml">

First we create the suite and get a `Validator`:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_yaml_configs.py#L114-L123
```

Then we use the `Validator` to add a few Expectations:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_yaml_configs.py#L125
```
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_yaml_configs.py#L127-L129
```

Finally we save our Expectation Suite (all of the unique Expectation Configurations from each run of `validator.expect_*`) to our Expectation Store:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_yaml_configs.py#L131
```

  </TabItem>

  <TabItem value="dataframe-python">

First we create the suite and get a `Validator`:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py#L120-L129
```

Then we use the `Validator` to add a few Expectations:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py#L131
```
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py#L133-L135
```

Finally we save our Expectation Suite (all of the unique Expectation Configurations from each run of `validator.expect_*`) to our Expectation Store:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py#L137
```

  </TabItem>
</Tabs>


### 6. Validate your data

<Tabs
  groupId="file-or-dataframe-pandas-or-yaml"
  defaultValue='file'
  values={[
  {label: 'File-yaml', value:'file-yaml'},
  {label: 'File-python', value:'file-python'},
  {label: 'Dataframe-yaml', value:'dataframe-yaml'},
  {label: 'Dataframe-python', value:'dataframe-python'},
  ]}>
  <TabItem value="file-yaml">

Here we will create and store a [Checkpoint](../reference/checkpoints_and_actions.md) for our batch, which we can use to [Validate](../reference/validation.md) and run post-validation actions. Check out our docs on "Validating your data" for more info on how to customize your Checkpoints.

First we create the Checkpoint configuration mirroring our `batch_request` configuration above and using the Expectation Suite we created:

```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_yaml_configs.py#L182-L200
```

Then we test our syntax using `test_yaml_config`:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_yaml_configs.py#L207
```

If all is well, we add the Checkpoint:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_yaml_configs.py#L209
```

Finally we run the Checkpoint:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_yaml_configs.py#L211-L213
```

  </TabItem>

  <TabItem value="file-python">

Here we will create and store a [Checkpoint](../reference/checkpoints_and_actions.md) for our batch, which we can use to [Validate](../reference/validation.md) and run post-validation actions. Check out our docs on "Validating your data" for more info on how to customize your Checkpoints.

First we create the Checkpoint configuration mirroring our `batch_request` configuration above and using the Expectation Suite we created:

```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py#L188-L213
```

Then we test our syntax using `test_yaml_config`:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py#L220
```

If all is well, we add the Checkpoint:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py#L222
```

Finally we run the Checkpoint:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py#L224-L226
```

  </TabItem>

  <TabItem value="dataframe-yaml">

Here we will create and store a Checkpoint with no defined validations, then pass in our dataframe at runtime.

First we create the Checkpoint configuration:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_yaml_configs.py#L142-L148
```

Then we test our syntax using `test_yaml_config`:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_yaml_configs.py#L150
```
Note that we get a message that the Checkpoint contains no validations. This is OK because we will pass them in at runtime, as we can see below when we call `context.run_checkpoint()`.

If all is well, we add the Checkpoint:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_yaml_configs.py#L152
```

Finally we run it with a validation defined using the Batch Request containing a reference to our dataframe and our Expectation Suite name:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_yaml_configs.py#L154-L162
```

  </TabItem>

  <TabItem value="dataframe-python">

Here we will create and store a Checkpoint with no defined validations, then pass in our dataframe at runtime.

First we create the Checkpoint configuration:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py#L148-L154
```

Then we test our syntax using `test_yaml_config`:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py#L156
```
Note that we get a message that the Checkpoint contains no validations. This is OK because we will pass them in at runtime, as we can see below when we call `context.run_checkpoint()`.

If all is well, we add the Checkpoint:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py#L158
```

Finally we run it with a validation defined using the Batch Request containing a reference to our dataframe and our Expectation Suite name:
```python file=../../tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py#L160-L168
```

  </TabItem>
</Tabs>


### 7. Build and view Data Docs

Since we used a `SimpleCheckpoint`, our Checkpoint already contained an `UpdateDataDocsAction` which rendered our [Data Docs](../reference/data_docs.md) from the validation we just ran. That means our Data Docs store will contain a new rendered validation result.

<details>
<summary>How do I customize these actions?</summary>
  Check out our docs on "Validating your data" for more info on how to customize your Checkpoints.

  Also, to see the full Checkpoint configuration, you can run: <code>print(my_checkpoint.get_substituted_config().to_yaml_str())</code>
</details>

Since we used DBFS for our Data Docs store, we need to download our data docs locally to view them. If you use a different store, you can host your data docs in a place where they can be accessed directly by your team. To learn more, see our documentation on Data Docs for other locations e.g. [filesystem](../guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_a_filesystem.md), [s3](../guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_amazon_s3.md), [GCS](../guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.md), [ABS](../guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_azure_blob_storage.md).

Run the following [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) command to download your data docs (replacing the paths as appropriate), then open the local copy of `index.html` to view your updated Data Docs:
```bash
databricks fs cp -r dbfs:/great_expectations/uncommitted/data_docs/local_site/ great_expectations/uncommitted/data_docs/local_site/
```

### 8. Congratulations!
You've successfully validated your data with Great Expectations using Databricks and viewed the resulting human-readable Data Docs. Check out our other guides for more customization options and happy validating!

View the full scripts used in this page on GitHub:

- [databricks_deployment_patterns_file_yaml_configs.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_yaml_configs.py)
- [databricks_deployment_patterns_file_python_configs.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py)
- [databricks_deployment_patterns_dataframe_yaml_configs.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_yaml_configs.py)
- [databricks_deployment_patterns_dataframe_python_configs.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py)
