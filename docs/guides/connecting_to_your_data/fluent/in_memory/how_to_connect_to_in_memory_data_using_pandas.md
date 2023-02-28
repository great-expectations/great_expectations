---
title: How to connect to one or more files using Pandas
tag: [how-to, connect to data]
description: A brief how-to guide covering ...
keywords: [Great Expectations, Pandas, Filesystem]
---

<!-- Import statements start here. -->
import Prerequisites from '/docs/components/_prerequisites.jsx'

## Introduction

In this guide we will demonstrate how to use Pandas to connect to data stored in a filesystem.  In our examples, we will specifically be connecting to `.csv` files.  However, Great Expectations supports most types of files that Pandas has read methods for.  There will be instructions for connecting to different types of files in the [Additional information](#additional-information) portion of this guide.

## Prerequisites

<Prerequisites requirePython = {false} requireInstallation = {true} requireDataContext = {true} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- Access to source data stored in a filesystem
- A passion for data quality

</Prerequisites> 

## Steps

### 1. Import the Great Expectations module

```python title="Python code"
import great_expectations as gx
```

### 2. Instantiate a Data Context

```python title="Python code"
context = gx.get_context
```

### 3. Create a Datasource

:::note Example Data

For this example, we are using a Pandas Dataframe as our source data.

The Pandas Dataframe we are using in this step's examples is defined with:

```python title="Python code"
import Pandas as pd

df = pd.DataFrame(
  {
    "a": [1, 2, 3],
    "b": [4, 5, 6]
  }
)
```

:::

```python title="Python code"
datasource = context.datasources.pandas_default.read_dataframe(dataframe=df)
```

### 4. Add a Data Asset to the Datasource

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

:::tip Using Pandas to connect to different file types

In this example, we are connecting to a `.csv` file.  However, Great Expectations supports connecting to most types of files that Pandas has `read_*` methods for.  

Because you will be using Pandas to connect to these files, the specific `read_*` methods that will be available to you will be determined by your currently installed version of Pandas.  

For more information on which Pandas `read_*` methods are available to you, please reference [the official Pandas Input/Output documentation](https://pandas.pydata.org/docs/reference/io.html) for the version of Pandas that you have installed.

In the GX Python API, `read_*` methods will require the same parameters as the corresponding Pandas `read_*` method, with one caveat: In Great Expectations, you will also be required to provide a value for an `asset_name` parameter.

:::


### 5. Repeat step 4 as needed to add additional files as Data Assets

## Next steps

Now that you have connected to your data, you may want to look into:
- How to request a Batch of data from a Datasource
- How to create Expectations while interactively evaluating a set of data
- How to use a Data Assistant to evaluate data

## Additional information

<!-- TODO: Add this once we have a script.
### Code examples

To see the full source code used for the examples in this guide, please reference the following scripts in our GitHub repository:
- [script_name.py](https://path/to/the/script/on/github.com)
-->

### GX Python APIs

For more information on the GX Python objects and APIs used in this guide, please reference the following pages of our public API documentation:

- `get_context`
- `add_datasource`
- `Datasource`
  - `add_csv_asset`

### External APIs

For more information on Pandas `read_*` methods, please reference [the official Pandas Input/Output documentation](https://pandas.pydata.org/docs/reference/io.html).

<!-- TODO: Enable this and update links after the conceptual guides are revised
### Related reading

For more information on the concepts and reasoning employed by this guide, please reference the following informational guides:

- [What does a Datasource do behind the scenes?](/docs/corresponding/link.md)
- [What are use the use cases for single vs multiple Batch Data Assets?](/docs/link/to/conceptual/guide.md)
-->