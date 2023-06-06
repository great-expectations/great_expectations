---
title: How to Use Great Expectations in Databricks
---
import Prerequisites from './components/deployment_pattern_prerequisites.jsx'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import Congratulations from '../guides/connecting_to_your_data/components/congratulations.md'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

Great Expectations works well with many types of Databricks workflows. This guide will help you run Great Expectations in [Databricks](https://databricks.com/).

## Prerequisites

<Prerequisites>

- Have completed Databricks setup including having a running Databricks cluster with attached notebook
- If you are using the file based version of this guide, you'll need to have DBFS set up

</Prerequisites>


We will cover a basic configuration to get you up and running quickly, and link to our other guides for more customized configurations. For example:
  - If you want to validate files stored in DBFS select one of the "File" tabs below.
    - If you are using a different file store (e.g. s3, GCS, ABS) take a look at our integration guides for those respective file stores.
  - If you already have a Spark DataFrame loaded, select one of the "DataFrame" tabs below.

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
  {label: 'DataFrame', value:'dataframe'},
  ]}>
  <TabItem value="file">

```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py imports"
```

  </TabItem>

  <TabItem value="dataframe">

```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py imports"
```

  </TabItem>
</Tabs>


### 2. Set up Great Expectations

In this guide, we will be using the [Databricks File System (DBFS)](https://docs.databricks.com/data/databricks-file-system.html) for your Metadata Stores and <TechnicalTag tag="data_docs" text="Data Docs"/> store. This is a simple way to get up and running within the Databricks environment without configuring external resources. For other options for storing data see our "Metadata Stores" and "Data Docs" sections in the "How to Guides" for "Setting up Great Expectations."

  <details>
    <summary>What is DBFS?</summary>
    Paraphrased from the Databricks docs: DBFS is a distributed file system mounted into a Databricks workspace and available on Databricks clusters. Files on DBFS can be written and read as if they were on a local filesystem, just by <a href="https://docs.databricks.com/data/databricks-file-system.html#local-file-apis">adding the /dbfs/ prefix to the path</a>. It is also persisted to object storage, so you won’t lose data after you terminate a cluster. See the Databricks documentation for best practices including mounting object stores.
  </details>

Run the following code to set up a <TechnicalTag tag="data_context" text="Data Context"/> in code using the appropriate defaults:

<Tabs
  groupId="file-or-dataframe"
  defaultValue='file'
  values={[
  {label: 'File', value:'file'},
  {label: 'DataFrame', value:'dataframe'},
  ]}>
  <TabItem value="file">

```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py choose context_root_dir"
```

```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py set up context"
```

  </TabItem>

  <TabItem value="dataframe">

```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py choose context_root_dir"
```

```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py set up context"
```

  </TabItem>
</Tabs>

### 3. Prepare your data

<Tabs
  groupId="file-or-dataframe"
  defaultValue='file'
  values={[
  {label: 'File', value:'file'},
  {label: 'DataFrame', value:'dataframe'},
  ]}>
  <TabItem value="file">

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
  {label: 'DataFrame', value:'dataframe'},
  ]}>
  <TabItem value="file">

Add the Datasource:
```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py choose base directory"
```
```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py add datasource"
```

Add the Data Asset:
```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py choose batching regex"
```
```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py add data asset"
```

Then we build a `BatchRequest` using the `DataAsset` we configured earlier to use as a sample of data when creating Expectations:
```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py build batch request"
```

  </TabItem>

  <TabItem value="dataframe">

Add the Datasource:
```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py add datasource"
```

Add the Data Asset:
```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py add data asset"
```

Then we build a `BatchRequest` using the `DataAsset` we configured earlier to use as a sample of data when creating Expectations:
```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py build batch request"
```

  </TabItem>
</Tabs>


<Congratulations />
Now let's keep going to create an Expectation Suite and validate our data.

### 5. Create Expectations

Here we will use a <TechnicalTag tag="validator" text="Validator" /> to interact with our batch of data and generate an <TechnicalTag tag="expectation_suite" text="Expectation Suite" />.

Each time we evaluate an Expectation (e.g. via `validator.expect_*`), it will immediately be Validated against your data. This instant feedback helps you zero in on unexpected data very quickly, taking a lot of the guesswork out of data exploration. Also, the Expectation configuration will be stored in the Validator. When you have run all of the Expectations you want for this dataset, you can call `validator.save_expectation_suite()` to save all of your Expectation configurations into an Expectation Suite for later use in a checkpoint.

<Tabs
  groupId="file-or-dataframe"
  defaultValue='file'
  values={[
  {label: 'File', value:'file'},
  {label: 'DataFrame', value:'dataframe'},
  ]}>
  <TabItem value="file">

First we create the suite and get a `Validator`:
```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py get validator"
```

Then we use the `Validator` to add a few Expectations:
```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py add expectations"
```

Finally we save our Expectation Suite (all of the unique Expectation Configurations from each run of `validator.expect_*`) to our Expectation Store:
```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py save suite"
```

  </TabItem>

  <TabItem value="dataframe">

First we create the suite and get a `Validator`:
```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py get validator"
```

Then we use the `Validator` to add a few Expectations:
```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py add expectations"
```

Finally we save our Expectation Suite (all of the unique Expectation Configurations from each run of `validator.expect_*`) to our Expectation Store:
```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py save suite"
```

  </TabItem>
</Tabs>


### 6. Validate your data

<Tabs
  groupId="file-or-dataframe"
  defaultValue='file'
  values={[
  {label: 'File', value:'file'},
  {label: 'DataFrame', value:'dataframe'},
  ]}>
  <TabItem value="file">

Here we will create and store a <TechnicalTag tag="checkpoint" text="Checkpoint"/> for our batch, which we can use to validate and run post-validation actions. Check out our docs on "Validating your data" for more info on how to customize your Checkpoints.

First, we create the Checkpoint configuration mirroring our `batch_request` configuration above and using the Expectation Suite we created:

```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py checkpoint config"
```

Next, we add the Checkpoint:
```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py add checkpoint config"
```

Finally, we run the Checkpoint:
```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py run checkpoint"
```

  </TabItem>

  <TabItem value="dataframe">

Here we will create and store a <TechnicalTag tag="checkpoint" text="Checkpoint"/> with no defined validations, then pass in our dataframe at runtime.

First, we create the Checkpoint configuration:
```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py checkpoint config"
```

Next, we add the Checkpoint:
```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py add checkpoint config"
```

Finally, we run the Checkpoint:
```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py run checkpoint"
```

  </TabItem>
</Tabs>


### 7. Build and view Data Docs

Since we used a `SimpleCheckpoint`, our Checkpoint already contained an `UpdateDataDocsAction` which rendered our <TechnicalTag tag="data_docs" text="Data Docs"/> from the validation we just ran. That means our Data Docs store will contain a new rendered validation result.

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

Using the `displayHTML` command is another option for displaying Data Docs in a Databricks notebook. There is a restriction, though, in that clicking on a link in the displayed data documents will result in an empty page. If you wish to see some validation results, use this approach.

```python 
html = '/dbfs/great_expectations/uncommitted/data_docs/local_site/index.html'
with open(html, "r") as f:
    data = "".join([l for l in f])
displayHTML(data)
```

### 8. Congratulations!
You've successfully validated your data with Great Expectations using Databricks and viewed the resulting Data Docs. Check out our other guides for more customization options and happy validating!

View the full scripts used in this page on GitHub:

- [databricks_deployment_patterns_file_python_configs.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py)
- [databricks_deployment_patterns_dataframe_python_configs.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py)