---
title: How to Use Great Expectations in Databricks
sidebar_label: "Databricks"
description: "Use Great Expectations in Databricks"
sidebar_custom_props: { icon: 'img/integrations/databricks_icon.png' }
---

import Prerequisites from '../../deployment_patterns/components/deployment_pattern_prerequisites.jsx'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import Congratulations from '../../guides/connecting_to_your_data/components/congratulations.md'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

This guide will help you run an end-to-end workflow with Great Expectations in [Databricks](https://databricks.com/).

You will:
  - Load data
  - Instantiate a <TechnicalTag tag="data_context" text="Data Context" />
  - Create a <TechnicalTag tag="datasource" text="Datasource" /> & <TechnicalTag tag="data_asset" text="Data Asset" />
  - Create an <TechnicalTag tag="expectation_suite" text="Expectation Suite" />
  - Validate data using a <TechnicalTag tag="checkpoint" text="Checkpoint" />

## Prerequisites

<Prerequisites>

- Have completed Databricks setup including having a running Databricks cluster with attached notebook
- Have access to [DBFS](https://docs.databricks.com/dbfs/index.html)

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

After that we will take care of some imports that will be used later:

```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py imports"
```

### 2. Set up Great Expectations

In this guide, we will be using the [Databricks File System (DBFS)](https://docs.databricks.com/data/databricks-file-system.html) for your Metadata Stores and <TechnicalTag tag="data_docs" text="Data Docs"/> store. This is a simple way to get up and running within the Databricks environment without configuring external resources. For other options for storing data see our "Metadata Stores" and "Data Docs" sections in the "How to Guides" for "Setting up Great Expectations."

  <details>
    <summary>What is DBFS?</summary>
    Paraphrased from the Databricks docs: DBFS is a distributed file system mounted into a Databricks workspace and available on Databricks clusters. Files on DBFS can be written and read as if they were on a local filesystem, just by <a href="https://docs.databricks.com/data/databricks-file-system.html#local-file-apis">adding the /dbfs/ prefix to the path</a>. It is also persisted to object storage, so you won’t lose data after you terminate a cluster. See the Databricks documentation for best practices including mounting object stores.
  </details>

Run the following code to set up a <TechnicalTag tag="data_context" text="Data Context"/> in code using the appropriate defaults:

```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py choose context_root_dir"
```

```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py set up context"
```

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

Then we build a <TechnicalTag tag="batch_request" text="Batch Request" /> using the <TechnicalTag tag="data_asset" text="Data Asset" /> we configured earlier to use as a sample of data when creating Expectations:
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

Then we build a <TechnicalTag tag="batch_request" text="Batch Request" /> using the <TechnicalTag tag="data_asset" text="Data Asset" /> we configured earlier to use as a sample of data when creating Expectations:
```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py build batch request"
```

  </TabItem>
</Tabs>


<Congratulations />
Now let's keep going to create an Expectation Suite and validate our data.

### 5. Create Expectations

Here we will use a <TechnicalTag tag="validator" text="Validator" /> to interact with our batch of data and generate an <TechnicalTag tag="expectation_suite" text="Expectation Suite" />.

Each time we evaluate an Expectation (e.g. via `validator.expect_*`), it will immediately be Validated against your data. This instant feedback helps you zero in on unexpected data very quickly, taking a lot of the guesswork out of data exploration. Also, the Expectation configuration will be stored in the Validator. When you have run all of the Expectations you want for this dataset, you can call `validator.save_expectation_suite()` to save all of your Expectation configurations into an Expectation Suite for later use in a checkpoint.

First we create the suite and get a `Validator`:
```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py get validator"
```

Then we use the `Validator` to add a few Expectations:
```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py add expectations"
```

Finally we save our Expectation Suite (all of the unique Expectation Configurations from each run of `validator.expect_*`) to our Expectation Store:
```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py save suite"
```

### 6. Validate your data

Here we will create and store a <TechnicalTag tag="checkpoint" text="Checkpoint"/> for our batch, which we can use to validate and run post-validation actions. Check out our docs on "Validating your data" for more info on how to customize your Checkpoints.

First, we create the Checkpoint configuration utilizing our data context, passing in our Batch Request (our data) and our Expectation Suite (our tests):
```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py checkpoint config"
```

Next, we save the Checkpoint:
```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py add checkpoint config"
```

Finally, we run the Checkpoint:
```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py run checkpoint"
```

<details>
<summary>Checkpoint actions?</summary>

  In our Checkpoint configuration, we've included two important actions: `store_validation_result` & `update_data_docs`.

  `store_validation_result` saves your validation results from this Checkpoint run, allowing these results to be persisted for further use.

  `update_data_docs` builds Data Docs files for the validations run in this Checkpoint.

  Check out [our docs on Validating your data](https://docs.greatexpectations.io/docs/guides/validation/validate_data_overview) for more info on how to customize your Checkpoints.

  Also, to see the full Checkpoint configuration, you can run: <code>print(my_checkpoint.get_substituted_config().to_yaml_str())</code>
</details>

### 7. Build and view Data Docs

Since our Checkpoint contained an `UpdateDataDocsAction`, our <TechnicalTag tag="data_docs" text="Data Docs" /> have already been built from the validation we just ran. That means our Data Docs store will contain a new rendered validation result.

Since we used DBFS for our Data Docs store, we need to download our data docs locally to view them. If you use a different store, you can host your data docs in a place where they can be accessed directly by your team. To learn more, see our documentation on Data Docs for other locations e.g. [filesystem](../../guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_a_filesystem.md), [s3](../../guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_amazon_s3.md), [GCS](../../guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.md), [ABS](../../guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_azure_blob_storage.md).

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

You've successfully validated your data with Great Expectations using Databricks and viewed the resulting Data Docs. Check out our other guides for more customization options and happy validating!

### 8. What's next?

Now that you've created and saved a Data Context, Datasource, Data Asset, Expectation Suite, and Checkpoint, you can follow [our documentation on Checkpoints](https://docs.greatexpectations.io/docs/guides/validation/how_to_validate_data_by_running_a_checkpoint) 
to create a script to run this checkpoint without having to re-create your Assets & Expectations. For more on productionalizing Databricks notebooks, see [this guide](https://www.databricks.com/blog/2022/06/25/software-engineering-best-practices-with-databricks-notebooks.html) from Databricks.

View the full scripts used in this page on GitHub:

- [databricks_deployment_patterns_file_python_configs.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py)
- [databricks_deployment_patterns_dataframe_python_configs.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_dataframe_python_configs.py)