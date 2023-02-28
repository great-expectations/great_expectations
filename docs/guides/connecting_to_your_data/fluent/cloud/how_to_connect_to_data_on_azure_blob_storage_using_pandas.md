---
title: How to connect to data on Azure Blob Storage using Pandas
tag: [how-to, connect to data]
description: A brief how-to guide covering ...
keywords: [Great Expectations, Azure Blob Storage, Pandas]
---

<!-- Import statements start here. -->
import Prerequisites from '/docs/components/_prerequisites.jsx'

import AfterCreateNonSqlDatasource from '/docs/components/connect_to_data/next_steps/_after_create_non_sql_datasource.md'

## Introduction

In this guide we will demonstrate how to use Pandas to connect to data stored in Azure Blob Storage.  In our examples, we will specifically be connecting to `.csv` files.  However, Great Expectations supports most types of files that Pandas has read methods for.  There will be instructions for connecting to different types of files in the [Additional information](#additional-information) portion of this guide.

## Prerequisites

<Prerequisites requirePython = {false} requireInstallation = {true} requireDataContext = {true} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- Access to data in Azure Blob Storage
- A passion for data quality

</Prerequisites> 

## Steps

### 1. Configure necessary credentials

Great Expectations provides three options for configuring your GCS credentials:
- Use the `gcloud` command line tool and `GOOGLE_APPLICATION_CREDENTIALS` environment variable
  - This is the default option and what was used throughout this guide
- Passing a `filename` argument to the optional `gcs_options` dictionary
  - This argument should contain a specific filepath that leads to your credentials `.json` file
  - This method utilizes `google.oauth2.service_account.Credentials.from_service_account_file` under the hood.
- Passing an `info` argument to the optional `gcs_options` dictionary
  - This argument should contain the actual JSON data from your credentials file in the form of a string.
  - This method utilizes `google.oauth2.service_account.Credentials.from_service_account_info` under the hood.

Please note that if you use the `filename` or `info` options you must supply these options to any GX objects that interact with GCS (i.e, your Datasource's Execution Engine).  The `gcs_options` dictionary is also responsible for storing any `**kwargs` you wish to pass to the GCS `storage.Client()` connection object (i.e. `project`),

For more details regarding storing credentials for use with Great Expectations see: [How to configure credentials](/docs/guides/setup/configuring_data_contexts/how_to_configure_credentials.md)

For more details regarding authentication, please visit the following:
* [gcloud CLI Tutorial](https://cloud.google.com/storage/docs/reference/libraries)
* [GCS Python API Docs](https://googleapis.dev/python/storage/latest/index.html)

### 2. Import GX and instantiate a Data Context

```python Python code
import great_expectations as gx

context = gx.get_context()
```


### 3. Create a Datasource

```python Python code
datasource = context.???
```

### 4. Add GCS data to the Datasource as a Data Asset

```python
csv_file_name = "taxi_data.csv"
data_asset = datasource.add_csv_asset(asset_name="MyTaxiDataAsset", regex=csv_file_name)
```

Your Data Asset will connect to all files that match the regex that you provide.  Each matched file will become a Batch inside your Data Asset.

For example:

Let's say that you have a filesystem Datasource pointing to a base folder that contains the following files:
- "taxi_data_2019.csv"
- "taxi_data_2020.csv"
- "taxi_data.2021.csv"

If you define a Data Asset using the full file name with no regex groups, such as `"taxi_data_2019.csv"` your Data Asset will contain only one Batch, which will correspond to that file.

However, if you define a partial file name with a regex group, such as `"taxi_data_{?<year>\d{{4}}}.csv"` your Data Asset will contain 3 Batches, one corresponding to each matched file.

## Next steps

<AfterCreateNonSqlDatasource />

## Additional information

<!-- TODO: Add this once we have a script.
### Code examples

To see the full source code used for the examples in this guide, please reference the following scripts in our GitHub repository:
- [script_name.py](https://path/to/the/script/on/github.com)
-->

### GX Python APIs

For more information on the GX Python objects and APIs used in this guide, please reference the following pages of our public API documentation:

- [`python_object`](/docs/link/to/corresponding/object/in/api/reference/pages.md)
- [`PythonObject.python_command(...)`](/docs/link/to/corresponding/api/reference/page.md#header_for_corresponding_command)
- [`PythonModule.other_python_command(...)`](/docs/link/to/corresponding/other_api/reference/page.md#header_for_corresponding_command)

### External APIs

For more information on Google Cloud and authentication, please visit the following:
* [gcloud CLI Tutorial](https://cloud.google.com/storage/docs/reference/libraries)
* [GCS Python API Docs](https://googleapis.dev/python/storage/latest/index.html)

### Related reading

For more details regarding storing credentials for use with GX, please see our guide: [How to configure credentials](/docs/guides/setup/configuring_data_contexts/how_to_configure_credentials.md)