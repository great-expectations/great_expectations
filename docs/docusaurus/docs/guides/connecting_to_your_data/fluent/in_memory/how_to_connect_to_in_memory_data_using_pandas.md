---
title: How to connect to one or more files using Pandas
tag: [how-to, connect to data]
description: A brief how-to guide covering ...
keywords: [Great Expectations, Pandas, Filesystem]
---

<!-- Import statements start here. -->
import Prerequisites from '/docs/components/_prerequisites.jsx'

<!-- ### 1. Import GX and instantiate a Data Context -->
import ImportGxAndInstantiateADataContext from '/docs/components/setup/data_context/_import_gx_and_instantiate_a_data_context.md'

## Introduction

In this guide we will demonstrate how to use Pandas to connect to data stored in a filesystem.  In our examples, we will specifically be connecting to `.csv` files.  However, Great Expectations supports most types of files that Pandas has read methods for.

## Prerequisites

<Prerequisites requirePython = {false} requireInstallation = {true} requireDataContext = {true} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- Access to data that can be read into a Pandas dataframe
- A passion for data quality

</Prerequisites> 

## Steps

### 1. Import the Great Expectations module and instantiate a Data context

<ImportGxAndInstantiateADataContext />

### 2. Create a Datasource

To access our in-memory data, we will create a Pandas Datasource:

```python title="Python code"
datasource = context.sources.add_pandas(name="my_pandas_datasource")
```

### 3. Read your source data into a Pandas Dataframe

For this example, we will read a `.parquet` file into a Pandas Dataframe.  We will then leverage Pandas to create a set of sampled data, which we will use in the rest of this guide.

The code to create the Pandas Dataframe we are using in this guide is defined with:

```python title="Python code"
import Pandas as pd

full_dataframe = pd.read_parquet("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-11.parquet")
sampled_dataframe = full_dataframe.sample(frac=0.05)
```

### 4. Add a Data Asset to the Datasource

A Pandas Dataframe Data Asset can be defined with two elements:
- `name`: The name by which the Datasource will be referenced in the future
- `dataframe`: An in-memory Pandas Dataframe containing the data to access

We will use the `sampled_dataframe` from the previous step as our `dataframe` value.  For the `name` parameter, we will define a name in advance by storing it in the Python variable `asset_name`:

```python title="Python code"
asset_name="TaxiDataframe"
```

Now that we have the `name` and `dataframe` for our Data Asset, we can create the Data Asset with the code:

```python
data_asset = datasource.add_dataframe(name=asset_name, dataframe=sampled_dataframe)
```

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