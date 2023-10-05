---
sidebar_label: 'Get started with GX and Databricks'
title: Get started with Great Expectations and Databricks
---

import Prerequisites from '../../deployment_patterns/components/deployment_pattern_prerequisites.jsx'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

Use the information provided here to learn how you can use Great Expectations (GX) with [Databricks](https://databricks.com/).

To use GX with Databricks, you'll complete the following tasks:

- Load data
- Instantiate a <TechnicalTag tag="data_context" text="Data Context" />
- Create a <TechnicalTag tag="datasource" text="Data Source" /> and a <TechnicalTag tag="data_asset" text="Data Asset" />
- Create an <TechnicalTag tag="expectation_suite" text="Expectation Suite" />
- Validate data using a <TechnicalTag tag="checkpoint" text="Checkpoint" />

The information provided here is intended to get you up and running quickly. To validate files stored in the DBFS, select the **File** tab. If you have an existing Spark DataFrame loaded, select one of the **DataFrame** tabs. See the specific integration guides if you're using a different file store such as Amazon S3, Google Cloud Storage (GCS), or Microsoft Azure Blob Storage (ABS).

The full code used in the following examples is available on GitHub:

- [databricks_deployment_patterns_file_python_configs.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py)

- [databricks_deployment_patterns_dataframe_python_configs.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py)

## Prerequisites

<Prerequisites>

- A complete Databricks setup including a running Databricks cluster with an attached notebook
- Access to [DBFS](https://docs.databricks.com/dbfs/index.html)

</Prerequisites>


## Install GX

1. Run the following command in your notebook to install GX as a notebook-scoped library:

    ```bash
    %pip install great-expectations
    ```

  A notebook-scoped library is a custom Python environment that is specific to a notebook. You can also install a library at the cluster or workspace level. See [Databricks Libraries](https://docs.databricks.com/data/databricks-file-system.html).

2. Run the following command to import the Python configurations you'll use in the following steps:

  ```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py imports"
  ```

## Set up GX

To avoid configuring external resources, you'll use the [Databricks File System (DBFS)](https://docs.databricks.com/data/databricks-file-system.html) for your Metadata Stores and <TechnicalTag tag="data_docs" text="Data Docs"/> store.

DBFS is a distributed file system mounted in a Databricks workspace and available on Databricks clusters. Files on DBFS can be written and read as if they were on a local filesystem, just by <a href="https://docs.databricks.com/data/databricks-file-system.html#local-file-apis">adding the /dbfs/ prefix to the path</a>. It is also persisted to object storage, so you won’t lose data after you terminate a cluster. See the Databricks documentation for best practices including mounting object stores.

1. Run the following code to set up a <TechnicalTag tag="data_context" text="Data Context"/> with the default settings:

  ```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py choose context_root_dir"
  ```
2. Run the following code to instantiate your Data Context:

  ```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py set up context"
  ```

## Prepare your data

<Tabs
  groupId="file-or-dataframe"
  defaultValue='file'
  values={[
  {label: 'File', value:'file'},
  {label: 'DataFrame', value:'dataframe'},
  ]}>
  <TabItem value="file">

Run the following command with [dbutils](https://docs.databricks.com/dev-tools/databricks-utils.html) to copy existing example csv taxi data to your DBFS folder:

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

Run the following code in your notebook to load a month of existing example taxi data as a DataFrame:

```python
df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-01.csv.gz")
```

  </TabItem>
</Tabs>

## Connect to your data

<Tabs
  groupId="file-or-dataframe"
  defaultValue='file'
  values={[
  {label: 'File', value:'file'},
  {label: 'DataFrame', value:'dataframe'},
  ]}>
<TabItem value="file">

1. Run the following command to set the base directory that contains the data:

  ```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py choose base directory"
  ```

2. Run the following command to create our <TechnicalTag tag="datasource" text="Data Source" />:

  ```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py add datasource"
  ```

3. Run the following command to set the [batching regex](https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/fluent/data_assets/how_to_organize_batches_in_a_file_based_data_asset/#create-a-batching_regex):

  ```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py choose batching regex"
  ```

4. Run the following command to create a <TechnicalTag tag="data_asset" text="Data Asset" /> with the Data Source:

  ```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py add data asset"
  ```

5. Run the following command to build a <TechnicalTag tag="batch_request" text="Batch Request" /> with the <TechnicalTag tag="data_asset" text="Data Asset" /> you configured earlier:

  ```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py build batch request"
  ```

</TabItem>
<TabItem value="dataframe">

1. Run the following command to create the Data Source:

  ```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py add datasource"
  ```

2. Run the following command to create a <TechnicalTag tag="data_asset" text="Data Asset" /> with the Data Source:

  ```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py add data asset"
  ```

3. Run the following command to build a <TechnicalTag tag="batch_request" text="Batch Request" /> with the <TechnicalTag tag="data_asset" text="Data Asset" /> you configured earlier:

  ```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py build batch request"
  ```

</TabItem>
</Tabs>

## Create Expectations

You'll use a <TechnicalTag tag="validator" text="Validator" /> to interact with your batch of data and generate an <TechnicalTag tag="expectation_suite" text="Expectation Suite" />.

Every time you evaluate an Expectation with `validator.expect_*`, it is immediately Validated against your data. This instant feedback helps you identify unexpected data and removes the guesswork from data exploration. The Expectation configuration is stored in the Validator. When you are finished running the Expectations on the dataset, you can use `validator.save_expectation_suite()` to save all of your Expectation configurations into an Expectation Suite for later use in a checkpoint.

1. Run the following command to create the suite and get a `Validator`:

  ```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py get validator"
  ```

2. Run the following command to use the `Validator` to add a few Expectations:

  ```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py add expectations"
  ```

3. Run the following command to save your Expectation Suite (all the unique Expectation Configurations from each run of `validator.expect_*`) to your Expectation Store:

  ```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py save suite"
  ```

## Validate your data

You'll create and store a <TechnicalTag tag="checkpoint" text="Checkpoint"/> for your batch, which you can use to validate and run post-validation actions.

1. Run the following command to create the Checkpoint configuration that uses your Data Context, passes in your Batch Request (your data) and your Expectation Suite (your tests):

  ```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py checkpoint config"
  ```

2. Run the following command to save the Checkpoint:

  ```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py add checkpoint config"
  ```

3. Run the following command to run the Checkpoint:

  ```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py run checkpoint"
  ```

  Your Checkpoint configuration includes the `store_validation_result` and `update_data_docs` actions. The `store_validation_result` action saves your validation results from the Checkpoint run and allows the results to be persisted for future use. The  `update_data_docs` action builds Data Docs files for the validations run in the Checkpoint.

  To learn more about Data validation and customizing Checkpoints, see [Validate Data:Overview ](https://docs.greatexpectations.io/docs/guides/validation/validate_data_overview).

  To view the full Checkpoint configuration, run: `print(checkpoint.get_config().to_yaml_str())`.

## Build and view Data Docs

Your Checkpoint contained an `UpdateDataDocsAction`, so your <TechnicalTag tag="data_docs" text="Data Docs" /> have already been built from the validation you ran and your Data Docs store contains a new rendered validation result.

Because you used the DBFS for your Data Docs store, you need to download your Data Docs locally to view them. If you use a different store, you can host your data docs in a place where they can be accessed directly by your organization. 

Run the following [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) command to download your data docs and open the local copy of `index.html` to view your updated Data Docs:

```bash
databricks fs cp -r dbfs:/great_expectations/uncommitted/data_docs/local_site/ great_expectations/uncommitted/data_docs/local_site/
```

The `displayHTML` command is another option for displaying Data Docs in a Databricks notebook. There is a restriction, though, in that clicking a link in the displayed data documents returns an empty page. To view some validation results, use this method. For example:

```python 
html = '/dbfs/great_expectations/uncommitted/data_docs/local_site/index.html'
with open(html, "r") as f:
    data = "".join([l for l in f])
displayHTML(data)
```

## Next steps

Now that you've created and saved a Data Context, Data Source, Data Asset, Expectation Suite, and Checkpoint, see [Validate data with Expectations and Checkpoints](https://docs.greatexpectations.io/docs/guides/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint) 
to create a script to run the Checkpoint without the need to recreate your Data Assets and Expectations. To move Databricks notebooks to production, see [Software Engineering Best Practices With Databricks Notebooks](https://www.databricks.com/blog/2022/06/25/software-engineering-best-practices-with-databricks-notebooks.html) from Databricks.