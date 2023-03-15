---
title: How to quickly connect to a single file using Pandas
tag: [how-to, connect to data]
description: A technical guide on using Pandas to immediately connect GX with the data in a single source file.
keywords: [Great Expectations, Pandas, Filesystem]
---

<!-- Import statements start here. -->
import Prerequisites from '/docs/components/_prerequisites.jsx'

<!-- Introduction -->
import Introduction from '/docs/components/connect_to_data/filesystem/_intro_connect_to_one_or_more_files_pandas_or_spark.mdx'

<!-- ### 1. Import GX and instantiate a Data Context -->
import ImportGxAndInstantiateADataContext from '/docs/components/setup/data_context/_import_gx_and_instantiate_a_data_context.md'

import InfoUsingPandasToConnectToDifferentFileTypes from '/docs/components/connect_to_data/filesystem/_info_using_pandas_to_connect_to_different_file_types.mdx'

<!-- Next steps -->
import AfterCreateValidator from '/docs/components/connect_to_data/next_steps/_after_create_validator.md'

## Introduction

<Introduction execution_engine="Pandas" />

## Prerequisites

<Prerequisites requirePython = {false} requireInstallation = {true} requireDataContext = {true} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- Access to source data stored in a filesystem
- A passion for data quality

</Prerequisites> 

## Steps

### 1. Import the Great Expectations module and instantiate a Data Context

<ImportGxAndInstantiateADataContext />

### 2. Specify a file to read into a Data Asset

Great Expectations supports reading the data in individual files directly into a Validator using Pandas.  To do this, we will run the code:

```python title="Python code"
validator = context.datasources.pandas_default.read_csv(
    filepath_or_buffer="https://raw.githubusercontent.com/great_expectations/taxi_data.csv"
)
```

<InfoUsingPandasToConnectToDifferentFileTypes this_example_file_extension="'.csv'"/>

### 3. Add a Data Asset to the Datasource

A Data Asset requires two pieces of information to be defined:
- `name`: The name by which you will reference the Data Asset (for when you have defined multiple Data Assets in the same Datasource)
- `batching_regex`: A regular expression that matches the files to be included in the Data Asset

<TipFilesystemDataAssetWhatIfBatchingRegexMatchesMultipleFiles />

For this example, we will define these two values in advance by storing them in the Python variables `asset_name` and (since we are connecting to NYC taxi data in this example) `taxi_batching_regex`:

```python title="Python code"
asset_name = "MyTaxiDataAsset"
taxi_batching_regex = "yellow_tripdata_sample_2023_01\.csv"
```

Once we have determined those two values, we will pass them in as parameters when we create our Data Asset:

```python title="Python code"
data_asset = datasource.add_csv_asset(
    name=asset_name, batching_regex=taxi_batching_regex
)
```

<TipUsingPandasToConnectToDifferentFileTypes this_example_file_extension=".csv" />


## Next steps

Now that you have a Validator, you can immediately move on to creating Expectations.  For more information, please see:

<AfterCreateValidator />
