---
sidebar_label: "Connect to filesystem Source Data"
title: "Connect to filesystem Source Data"
id: connect_filesystem_source_data
description: Connect to Source Data stored in filesystem files.
toc_min_heading_level: 2
toc_max_heading_level: 2
---

import Prerequisites from '/docs/components/_prerequisites.jsx'
import Introduction from '/docs/components/connect_to_data/filesystem/_intro_connect_to_one_or_more_files_pandas_or_spark.mdx'
import ImportGxAndInstantiateADataContext from '/docs/components/setup/data_context/_import_gx_and_instantiate_a_data_context.md'
import InfoUsingPandasToConnectToDifferentFileTypes from '/docs/components/connect_to_data/filesystem/_info_using_pandas_to_connect_to_different_file_types.mdx'
import AfterCreateValidator from '/docs/components/connect_to_data/next_steps/_after_create_validator.md'
import InfoFilesystemDatasourceRelativeBasePaths from '/docs/components/connect_to_data/filesystem/_info_filesystem_datasource_relative_base_paths.md'
import TipFilesystemDatasourceNestedSourceDataFolders from '/docs/components/connect_to_data/filesystem/_tip_filesystem_datasource_nested_source_data_folders.md'
import TipFilesystemDataAssetWhatIfBatchingRegexMatchesMultipleFiles from '/docs/components/connect_to_data/filesystem/_tip_filesystem_data_asset_if_batching_regex_matches_multiple_files.md'
import TipUsingPandasToConnectToDifferentFileTypes from '/docs/components/connect_to_data/filesystem/_info_using_pandas_to_connect_to_different_file_types.mdx'
import DefiningMultipleDataAssets from '/docs/components/connect_to_data/filesystem/_defining_multiple_data_assets.md'
import AfterCreateNonSqlDatasource from '/docs/components/connect_to_data/next_steps/_after_create_non_sql_datasource.md'
import BatchingRegexExplaination from '/docs/components/connect_to_data/cloud/_batching_regex_explaination.mdx'
import PrereqInstallGxWithDependencies from '/docs/components/prerequisites/_gx_installed_with_abs_dependencies.md'
import AbsFluentAddDataAssetConfigKeys from '/docs/components/connect_to_data/cloud/_abs_fluent_data_asset_config_keys.mdx'
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

Use the information provided here to connect to Source Data stored on Amazon S3, Google Cloud Storage (GCS), Microsoft Azure Blob Storage, or local filesystems. Great Expectations (GX) uses the term Source Data when referring to data in its original format, and the term Source Data System when referring to the storage location for Source Data.

<Tabs
  groupId="connect-filesystem-source-data"
  defaultValue='amazon'
  values={[
  {label: 'Amazon S3', value:'amazon'},
  {label: 'Microsoft Azure Blob Storage', value:'azure'},
  {label: 'Google Cloud Storage', value:'gcs'},
  {label: 'Filesystem', value:'filesystem'},
  ]}>
<TabItem value="amazon">

## Amazon S3 Source Data

Connect to Source Data on Amazon S3.

<Tabs
  groupId="connect-amazon-source-data"
  defaultValue='pandas'
  values={[
  {label: 'pandas ', value:'pandas'},
  {label: 'Spark', value:'spark'},
  ]}>
<TabItem value="pandas">

The following examples connect to .csv data. However, GX supports most of the Pandas read methods.

### Prerequisites

<Prerequisites>

- [An installation of GX set up to work with S3](/docs/guides/setup/optional_dependencies/cloud/how_to_set_up_gx_to_work_with_data_on_aws_s3)
- Access to data on a S3 bucket

</Prerequisites> 

### Import GX and instantiate a Data Context

<ImportGxAndInstantiateADataContext />

### Create a Data Source

The following information is required when you create an Amazon S3 Data Source:

- `name`: The Data Source name. In the following examples, this is `"my_s3_datasource"`

- `bucket_name`: The Amazon S3 bucket name.

- `boto3_options`: Optional. Additional options for the Data Source. In the following examples, the default values are used.

1. Run the following Python code to define `name`, `bucket_name` and `boto3_options`:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_s3_using_pandas.py define_add_pandas_s3_args"
    ```

    :::tip Additional options for `boto3_options`

    The parameter `boto3_options` allows you to pass the following information:
    - `endpoint_url`: specifies an S3 endpoint.  You can use an environment variable such as `"${S3_ENDPOINT}"` to securely include this in your code.  The string `"${S3_ENDPOINT}"` will be replaced with the value of the corresponding environment variable.
    - `region_name`: Your AWS region name.

    :::

2. Run the following Python code to pass `name`, `bucket_name`, and `boto3_options` as parameters when you create your Data Source::

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_s3_using_pandas.py create_datasource"
    ```

### Add data to the Data Source as a Data Asset

Run the following Python code:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_s3_using_pandas.py add_asset"
```

<BatchingRegexExplaination storage_location_type="S3 bucket" />

### Next steps

<AfterCreateNonSqlDatasource />

</TabItem>
<TabItem value="spark">

The following examples connect to .csv data.

### Prerequisites

<Prerequisites>

- [An installation of GX set up to work with S3](/docs/guides/setup/optional_dependencies/cloud/how_to_set_up_gx_to_work_with_data_on_aws_s3)
- Access to data on a S3 bucket

</Prerequisites> 

### Import GX and instantiate a Data Context

<ImportGxAndInstantiateADataContext />

### Create a Data Source

The following information is required when you create an Amazon S3 Data Source:

- `name`: The Data Source name. In the following examples, this is `"my_s3_datasource"`

- `bucket_name`: The Amazon S3 bucket name.

- `boto3_options`: Optional. Additional options for the Data Source. In the following examples, the default values are used.

1. Run the following Python code to define `name`, `bucket_name`, and `boto3_options`:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_s3_using_spark.py define_add_spark_s3_args"
    ```

    :::tip Additional options for `boto3_options`

    The parameter `boto3_options` allows you to pass the following information:
    - `endpoint_url`: Specifies an S3 endpoint.  You can use an environment variable such as `"${S3_ENDPOINT}"` to securely include this in your code.  The string `"${S3_ENDPOINT}"` will be replaced with the value of the corresponding environment variable.
    - `region_name`: Your AWS region name.

    :::

2. Run the following Python code to pass `name`, `bucket_name`, and `boto3_options` as parameters when you create your Data Source::

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_s3_using_spark.py create_datasource"
    ```

### Add data to the Data Source as a Data Asset

Run the following Python code:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_s3_using_spark.py add_asset"
```

<BatchingRegexExplaination storage_location_type="S3 bucket" />

### Next steps

<AfterCreateNonSqlDatasource />

</TabItem>
</Tabs>
</TabItem>
<TabItem value="azure">

## Microsoft Azure Blob Storage

Connect to Source Data on Microsoft Azure Blob Storage.

<Tabs
  groupId="connect-azure-source-data"
  defaultValue='pandas'
  values={[
  {label: 'pandas ', value:'pandas'},
  {label: 'Spark', value:'spark'},
  ]}>
<TabItem value="pandas">

<Introduction execution_engine='Pandas' />

### Prerequisites

<Prerequisites>

- <PrereqInstallGxWithDependencies />
- Access to data in Azure Blob Storage

</Prerequisites> 

### Import GX and instantiate a Data Context

<ImportGxAndInstantiateADataContext />

### Create a Data Source

The following information is required when you create a Microsoft Azure Blob Storage Data Source:

- `name`: The Data Source name. In the following examples, this is `"my_datasource"`.

- `azure_options`: Authentication settings.

1. Run the following Python code to define `name` and `azure_options`:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_azure_blob_storage_using_pandas.py define_add_pandas_abs_args"
    ```
2. Run the following Python code to pass `name` and `azure_options` as parameters when you create your Data Source:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_azure_blob_storage_using_pandas.py create_datasource"
    ```

    :::tip Where did that connection string come from?
    In the previous example, the value for `account_url` is substituted for the contents of the `AZURE_STORAGE_CONNECTION_STRING` key you configured when you [installed GX and set up your Azure Blob Storage dependencies](/docs/guides/setup/optional_dependencies/cloud/how_to_set_up_gx_to_work_with_data_in_abs).
    :::

### Add data to the Data Source as a Data Asset

<AbsFluentAddDataAssetConfigKeys />

2. Run the following Python code to create the Data Asset:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_azure_blob_storage_using_pandas.py add_asset"
    ```

<AbsBatchingRegexExample />

### Next steps

<AfterCreateNonSqlDatasource />

</TabItem>
<TabItem value="spark">

<Introduction execution_engine='Spark' />

### Prerequisites

<Prerequisites>

- <PrereqInstallGxWithDependencies />
- Access to data in Azure Blob Storage

</Prerequisites> 

### Import GX and instantiate a Data Context

<ImportGxAndInstantiateADataContext />

### Create a Data Source

The following information is required when you create a Microsoft Azure Blob Storage Data Source:

- `name`: The Data Source name. In the following examples, this is `"my_datasource"`.

- `azure_options`: Authentication settings.

1. Run the following Python code to define `name` and `azure_options`:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_azure_blob_storage_using_spark.py define_add_spark_abs_args"
    ```
2. Run the following Python code to pass `name` and `azure_options` as parameters when you create your Data Source:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_azure_blob_storage_using_spark.py create_datasource"
    ```

    :::tip Where did that connection string come from?
    In the previous example, the value for `account_url` is substituted for the contents of the `AZURE_STORAGE_CONNECTION_STRING` key you configured when you [installed GX and set up your Azure Blob Storage dependencies](/docs/guides/setup/optional_dependencies/cloud/how_to_set_up_gx_to_work_with_data_in_abs).
    :::

### Add data to the Data Source as a Data Asset

<AbsFluentAddDataAssetConfigKeys />

2. Run the following Python code to create the Data Asset:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_azure_blob_storage_using_spark.py add_asset"
    ```

<AbsBatchingRegexExample />

### Next steps

<AfterCreateNonSqlDatasource />

</TabItem>
</Tabs>
</TabItem>
<TabItem value="gcs">

## GCS Source Data

Connect to Source Data on GCS.

<Tabs
  groupId="connect-gcs-source-data"
  defaultValue='pandas'
  values={[
  {label: 'pandas ', value:'pandas'},
  {label: 'Spark', value:'spark'},
  ]}>
<TabItem value="pandas">

The following examples connect to .csv data. However, GX supports most of the Pandas read methods.

### Prerequisites

<Prerequisites>

- [An installation of GX set up to work with GCS](/docs/guides/setup/optional_dependencies/cloud/how_to_set_up_gx_to_work_with_data_on_gcs)
- Access to data in a GCS bucket

</Prerequisites> 

### Import GX and instantiate a Data Context

<ImportGxAndInstantiateADataContext />

### Create a Data Source

The following information is required when you create a GCS Data Source:

- `name`: The Data Source name. In the following examples, this is `"my_gcs_datasource"`.

- `bucket_or_name`: The GCS bucket or instance name.

- `gcs_options`: Optional. Additional options for the Data Source. In the following examples, the default values are used.

1. Run the following Python code to define `name`, `bucket_or_name`, and `gcs_options`:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_gcs_using_pandas.py define_add_pandas_gcs_args"
    ```

2. Run the following Python code to pass `name`, `bucket_or_name`, and `gcs_options` as parameters when you create your Data Source:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_gcs_using_pandas.py create_datasource"
    ```

### Add GCS data to the Data Source as a Data Asset

Run the following Python code:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_gcs_using_pandas.py add_asset"
```

<BatchingRegexExplaination storage_location_type="GCS bucket" />

### Next steps

<AfterCreateNonSqlDatasource />

### Related documentation

For more information on Google Cloud and authentication, see the following:

* [gcloud CLI Tutorial](https://cloud.google.com/storage/docs/reference/libraries)
* [GCS Python API Docs](https://googleapis.dev/python/storage/latest/index.html)

</TabItem>
<TabItem value="spark">

Use Spark to connect to Source Data stored on GCS.  The following examples connect to .csv data.

### Prerequisites

<Prerequisites>

- [An installation of GX set up to work with GCS](/docs/guides/setup/optional_dependencies/cloud/how_to_set_up_gx_to_work_with_data_on_gcs)
- Access to data on a GCS bucket

</Prerequisites> 

### 1. Import GX and instantiate a Data Context

<ImportGxAndInstantiateADataContext />

### Create a Data Source

The following information is required when you create a GCS Data Source:

- `name`: The Data Source name. In the following examples, this is `"my_gcs_datasource"`.

- `bucket_or_name`: The GCS bucket or instance name.

- `gcs_options`: Optional. Additional options for the Data Source. In the following examples, the default values are used.

1. Run the following Python code to define `name`, `bucket_or_name`, and `gcs_options`:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_gcs_using_spark.py define_add_spark_gcs_args"
    ```

2. Run the following Python code to pass `name`, `bucket_or_name`, and `gcs_options` as parameters when you create your Data Source:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_gcs_using_spark.py create_datasource"
    ```

### Add GCS data to the Data Source as a Data Asset

Run the following Python code:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_gcs_using_spark.py add_asset"
```

:::info Optional parameters: `header` and `infer_schema`

In the previous example there are two optional parameters.  If the file does not have a header line, the `header` parameter can be left out as it will default to `false`.  If you do not want GX to infer the schema of your file, you can exclude the `infer_schema` parameter as it also defaults to `false`.

:::

<BatchingRegexExplaination storage_location_type="GCS bucket" />

### Next steps

<AfterCreateNonSqlDatasource />

### Related documentation

For more information on Google Cloud and authentication, see the following:

* [gcloud CLI Tutorial](https://cloud.google.com/storage/docs/reference/libraries)
* [GCS Python API Docs](https://googleapis.dev/python/storage/latest/index.html)

</TabItem>
</Tabs>
</TabItem>
<TabItem value="filesystem">

## Filesystem Source Data

Connect to Source Data on a filesystem.

<Tabs
  groupId="connect-filesystem-source-data"
  defaultValue='single'
  values={[
  {label: 'Single file with pandas ', value:'single'},
  {label: 'Multiple files with pandas', value:'multiple'},
  {label: 'Multiple files with Spark ', value:'spark'},
  ]}>
<TabItem value="single">

<Introduction execution_engine="Pandas" />

### Prerequisites

<Prerequisites requirePython = {false} requireInstallation = {true} requireDataContext = {true} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- Access to source data stored in a filesystem

</Prerequisites> 

### Import the GX module and instantiate a Data Context

<ImportGxAndInstantiateADataContext />

### Specify a file to read into a Data Asset

Run the following Python code to read the data in individual files directly into a Validator with Pandas:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_quickly_connect_to_a_single_file_with_pandas.py get_validator"
```

<InfoUsingPandasToConnectToDifferentFileTypes this_example_file_extension="csv"/>

### Next steps

<AfterCreateValidator />

</TabItem>
<TabItem value="multiple">

<Introduction execution_engine="Pandas" />

### Prerequisites

<Prerequisites requirePython = {false} requireInstallation = {true} requireDataContext = {true} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- Access to source data stored in a filesystem

</Prerequisites> 

### Import the GX module and instantiate a Data Context

<ImportGxAndInstantiateADataContext />

### Create a Data Source

The following information is required when you create a Filesystem Data Source:

- `name`: The Data Source name.

- `base_directory`: The path to the folder containing the files the Data Source connects to.

1. Run the following Python code to define `name` and `base_directory` and store the information in the Python variables `datasource_name` and `path_to_folder_containing_csv_files`:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_one_or_more_files_using_pandas.py define_add_pandas_filesystem_args"
    ```

<InfoFilesystemDatasourceRelativeBasePaths />

2. Run the following Python code to pass `name` and `base_directory` as parameters when you create your Data Source:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_one_or_more_files_using_pandas.py create_datasource"
    ```

<TipFilesystemDatasourceNestedSourceDataFolders />

### Add a Data Asset to the Data Source

A Data Asset requires the following information to be defined:

- `name`: The Data Asset name. Helpful when you define multiple Data Assets in the same Data Source.

- `batching_regex`: A regular expression that matches the files to be included in the Data Asset.

<TipFilesystemDataAssetWhatIfBatchingRegexMatchesMultipleFiles />

1. Run the following Python code to define `name` and `batching_regex` and store the information in the Python variables `asset_name` and `batching_regex`:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_one_or_more_files_using_pandas.py define_add_csv_asset_args"
    ```

2. Run the following Python code to pass `name` and `batching_regex` as parameters when you create your Data Asset:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_one_or_more_files_using_pandas.py add_asset"
    ```

    <TipUsingPandasToConnectToDifferentFileTypes this_example_file_extension="csv" />


### Add additional files as Data Assets (Optional)

<DefiningMultipleDataAssets />

### Next steps

<AfterCreateNonSqlDatasource />

### Related documentation

For more information on Pandas `read_*` methods, see [the Pandas Input/output documentation](https://pandas.pydata.org/docs/reference/io.html).

</TabItem>
<TabItem value="spark">

<Introduction execution_engine="Spark" />

### Prerequisites

<Prerequisites requirePython = {false} requireInstallation = {true} requireDataContext = {true} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- Access to source data stored in a filesystem

</Prerequisites> 

### Import the GX module and instantiate a Data Context

<ImportGxAndInstantiateADataContext />

### Create a Data Source

The following information is required when you create a Filesystem Data Source:

- `name`: The Data Source name.

- `base_directory`: The path to the folder containing the files the Data Source connects to.

1. Run the following Python code to define `name` and `base_directory` and store the information in the Python variables `datasource_name` and `path_to_folder_containing_csv_files`:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_one_or_more_files_using_spark.py define_add_spark_filesystem_args"
    ```

    <InfoFilesystemDatasourceRelativeBasePaths />

2. Run the following Python code to pass `name` and `base_directory` as parameters when you create your Data Source:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_one_or_more_files_using_spark.py create_datasource"
    ```

    <TipFilesystemDatasourceNestedSourceDataFolders />

### Add a Data Asset to the Data Source

A Data Asset requires the following information to be defined:

- `name`: The Data Asset name. Helpful when you define multiple Data Assets in the same Data Source.

- `batching_regex`: A regular expression that matches the files to be included in the Data Asset.

<TipFilesystemDataAssetWhatIfBatchingRegexMatchesMultipleFiles />

1. Run the following Python code to define `name` and `batching_regex` and store the information in the Python variables `asset_name` and `batching_regex`:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_one_or_more_files_using_spark.py define_add_csv_asset_args"
    ```

    In addition, the argument `header` informs the Spark `DataFrame` reader that the files contain a header column, while the argument `infer_schema` instructs the Spark `DataFrame` reader to make a best effort to determine the schema of the columns automatically.

2. Run the following Python code to pass `name` and `batching_regex` and the optional `header` and `infer_schema` arguments as parameters when you create your Data Asset:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_one_or_more_files_using_spark.py add_asset"
    ```

### Add additional files as Data Assets (Optional)

<DefiningMultipleDataAssets />

### Next steps

<AfterCreateNonSqlDatasource />

</TabItem>
</Tabs>
</TabItem>
</Tabs>

## Related documentation

For more information about storing credentials for use with GX, see [How to configure credentials](/docs/guides/setup/configuring_data_contexts/how_to_configure_credentials).
