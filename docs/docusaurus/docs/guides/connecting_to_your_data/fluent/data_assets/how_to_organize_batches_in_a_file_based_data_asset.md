---
title: How to organize Batches in a file-based Data Asset
tag: [how-to, connect to data]
description: A technical guide demonstrating how to organize Batches of data in a file-based Data Asset.
keywords: [Great Expectations, Data Asset, Batch Request, fluent configuration method, GCS, Google Cloud Storage, AWS S3, Amazon Web Services S3, Azure Blob Storage, Local Filesystem]
---

import TechnicalTag from '/docs/term_tags/_tag.mdx';


<!-- ## Introduction -->

<!-- ## Prerequisites -->
import Prerequisites from '/docs/components/_prerequisites.jsx'

<!-- ### Import GX and instantiate a Data Context -->
import ImportGxAndInstantiateADataContext from '/docs/components/setup/data_context/_import_gx_and_instantiate_a_data_context.md'

<!-- ### 1. Create a `batching_regex` -->
import TipFilesystemDatasourceNestedSourceDataFolders from '/docs/components/connect_to_data/filesystem/_tip_filesystem_datasource_nested_source_data_folders.md'

<!-- ## Next steps -->
import AfterCreateAndConfigureDataAsset from '/docs/components/connect_to_data/next_steps/_after_create_and_configure_data_asset.md'

In this guide we will demonstrate the ways in which Batches can be organized in a file-based Data Asset.  We will discuss how to use a regular expression to indicate which files should be returned as Batches.  We will also show how to add Batch Sorters to a Data Asset in order to specify the order in which Batches are returned.

## Prerequisites

<Prerequisites>

- A working installation of Great Expectations
- A Datasource that connects to a location with source data files

</Prerequisites>


<details>
<summary>

### If you still need to set up and install GX...

</summary>

Please reference the appropriate one of these guides:
- [How to install GX locally](/docs/guides/setup/installation/local)
- [How to set up GX to work with data on AWS S3](/docs/guides/setup/optional_dependencies/cloud/how_to_set_up_gx_to_work_with_data_on_aws_s3)
- [How to set up GX to work with data in Azure Blob Storage](/docs/guides/setup/optional_dependencies/cloud/how_to_set_up_gx_to_work_with_data_in_abs)
- [How to set up GX to work with data on GCS](/docs/guides/setup/optional_dependencies/cloud/how_to_set_up_gx_to_work_with_data_on_gcs)

</details>

<details>
<summary>

### If you still need to connect a Datasource to the location of your source data files...

</summary>

Please reference the appropriate one of these guides:

#### Local Filesystems
- [How to connect to one or more files using Pandas](/docs/guides/connecting_to_your_data/fluent/filesystem/how_to_connect_to_one_or_more_files_using_pandas)
- [How to connect to one or more files using Spark](/docs/guides/connecting_to_your_data/fluent/filesystem/how_to_connect_to_one_or_more_files_using_spark)

#### Google Cloud Storage
- [How to connect to data on GCS using Pandas](/docs/guides/connecting_to_your_data/fluent/cloud/how_to_connect_to_data_on_gcs_using_pandas)
- [How to connect to data on GCS using Spark](/docs/guides/connecting_to_your_data/fluent/cloud/how_to_connect_to_data_on_gcs_using_spark)

#### Azure Blob Storage
- [How to connect to data in Azure Blob Storage using Pandas](/docs/guides/connecting_to_your_data/fluent/cloud/how_to_connect_to_data_on_azure_blob_storage_using_pandas)
- [How to connect to data in Azure Blob Storage using Spark](/docs/guides/connecting_to_your_data/fluent/cloud/how_to_connect_to_data_on_azure_blob_storage_using_spark)

#### Amazon Web Services S3
- [How to connect to data on Amazon Web Services S3 using Pandas](/docs/guides/connecting_to_your_data/fluent/cloud/how_to_connect_to_data_on_s3_using_pandas)
- [How to connect to data on Amazon Web Services S3 using Spark](/docs/guides/connecting_to_your_data/fluent/cloud/how_to_connect_to_data_on_s3_using_spark)

</details>

:::caution Datasources defined with the block-config method

If you are using a Datasource that was created with the advanced block-config method please follow the appropriate guide from:
- [how to configure a Spark Datasource with the block-config method](/docs/0.15.50/guides/connecting_to_your_data/datasource_configuration/how_to_configure_a_spark_datasource)
- [how to configure a Pandas Datasource with the block-config method](/docs/0.15.50/guides/connecting_to_your_data/datasource_configuration/how_to_configure_a_pandas_datasource)

:::

## Steps

### 1. Import GX and instantiate a Data Context

<ImportGxAndInstantiateADataContext />

### 2. Retrieve a file-based Datasource

For this guide, we will use a previously defined Datasource named `"my_datasource"`.  For purposes of our demonstration, this Datasource is a Pandas Filesystem Datasource that uses a folder named "data" as its `base_folder`.

To retrieve this Datasource, we will supply the `get_datasource(...)` method of our Data Context with the name of the Datasource we wish to retrieve:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/organize_batches_in_pandas_filesystem_datasource.py my_datasource"
```

### 3. Create a `batching_regex`

In a file-based Data Asset, any file that matches a provided regular expression (the `batching_regex` parameter) will be included as a Batch in the Data Asset.  Therefore, to organize multiple files into Batches in a single Data Asset we must define a regular expression that will match one or more of our source data files.

For this example, our Datasource points to a folder that contains the following files:
- "yellow_tripdata_sample_2019-03.csv"
- "yellow_tripdata_sample_2020-07.csv"
- "yellow_tripdata_sample_2021-02.csv"

To create a `batching_regex` that matches multiple files, we will include a named group in our regular expression:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/organize_batches_in_pandas_filesystem_datasource.py my_batching_regex"
```

In the above example, the named group "`year`" will match any four numeric characters in a file name.  This will result in each of our source data files matching the regular expression.

:::tip Why use named groups?

By naming the group in your `batching_regex` you make it something you can reference in the future.  When requesting data from this Data Asset, you can use the names of your regular expression groups to limit the Batches that are returned.

For more information, please see: [How to request data from a Data Asset](/docs/guides/connecting_to_your_data/fluent/batch_requests/how_to_request_data_from_a_data_asset)

:::

<TipFilesystemDatasourceNestedSourceDataFolders />

For more information on how to format regular expressions, we recommend referencing [Python's official how-to guide for working with regular expressions](https://docs.python.org/3/howto/regex.html).

### 4. Add a Data Asset using the `batching_regex`

Now that we have put together a regular expression that will match one or more of the files in our Datasource's `base_folder`, we can use it to create our Data Asset.  Since the files in this particular Datasource's `base_folder` are csv files, we will use the `add_pandas_csv(...)` method of our Datasource to create the new Data Asset:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/organize_batches_in_pandas_filesystem_datasource.py my_asset"
```

:::tip What if I don't provide a `batching_regex`?

If you choose to omit the `batching_regex` parameter, your Data Asset will automatically use the regular expression `".*"` to match all files.

:::

### 5. (Optional) Add Batch Sorters to the Data Asset

We will now add a Batch Sorter to our Data Asset.  This will allow us to explicitly state the order in which our Batches are returned when we request data from the Data Asset.  To do this, we will pass a list of sorters to the `add_sorters(...)` method of our Data Asset.

The items in our list of sorters will correspond to the names of the groups in our `batching_regex` that we want to sort our Batches on.  The names are prefixed with a `+` or a `-` depending on if we want to sort our Batches in ascending or descending order based on the given group.

When there are multiple named groups, we can include multiple items in our sorter list and our Batches will be returned in the order specified by the list: sorted first according to the first item, then the second, and so forth.

In this example we have two named groups, `"year"` and `"month"`, so our list of sorters can have up to two elements.  We will add an ascending sorter based on the contents of the regex group `"year"` and a descending sorter based on the contents of the regex group `"month"`:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/organize_batches_in_pandas_filesystem_datasource.py add_sorters"
```

### 6. Use a Batch Request to verify the Data Asset works as desired

To verify that our Data Asset will return the desired files as Batches, we will define a quick Batch Request that will include all the Batches available in the Data asset.  Then we will use that Batch Request to get a list of the returned Batches.

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/organize_batches_in_pandas_filesystem_datasource.py my_batch_list"
```

Because a Batch List contains a lot of metadata, it will be easiest to verify which files were included in the returned Batches if we only look at the `batch_spec` of each returned Batch:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/organize_batches_in_pandas_filesystem_datasource.py print_batch_spec"
```

## Next steps

Now that you have further configured a file-based Data Asset, you may want to look into:

### Requesting Data from a Data Asset
- [How to request data from a Data Asset](/docs/guides/connecting_to_your_data/fluent/batch_requests/how_to_request_data_from_a_data_asset)

### Using Data Assets to create Expectations
- [Use a Data Asset to create Expectations while interactively evaluating a set of data](/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data)
- [Use the Onboarding Data Assistant to evaluate one or more Batches of data and create Expectations](/docs/guides/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant)




