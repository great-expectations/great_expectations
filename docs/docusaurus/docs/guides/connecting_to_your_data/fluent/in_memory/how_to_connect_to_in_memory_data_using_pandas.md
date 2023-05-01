---
title: How to connect to in-memory data using Pandas
tag: [how-to, connect to data]
description: A technical guide on connecting Great Expectations to a Pandas in-memory DataFrame.
keywords: [Great Expectations, Pandas, Filesystem]
---

<!-- Import statements start here. -->
import Prerequisites from '/docs/components/_prerequisites.jsx'

<!-- ### 1. Import GX and instantiate a Data Context -->
import ImportGxAndInstantiateADataContext from '/docs/components/setup/data_context/_import_gx_and_instantiate_a_data_context.md'

<!-- ## Next steps -->
import AfterCreateInMemoryDataAsset from '/docs/components/connect_to_data/next_steps/_after_create_in_memory_data_asset.md'

## Introduction

In this guide we will demonstrate how to connect to an in-memory Pandas DataFrame.  Pandas can read many types of data into its DataFrame class, but in our example we will use data originating in a parquet file.

## Prerequisites

<Prerequisites requirePython = {false} requireInstallation = {true} requireDataContext = {true} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- Access to data that can be read into a Pandas DataFrame

</Prerequisites> 

## Steps

### 1. Import the Great Expectations module and instantiate a Data Context

<ImportGxAndInstantiateADataContext />

### 2. Create a Datasource

To access our in-memory data, we will create a Pandas Datasource:

```python title="Python code"
datasource = context.sources.add_pandas(name="my_pandas_datasource")
```

### 3. Read your source data into a Pandas DataFrame

For this example, we will read a parquet file into a Pandas DataFrame, which we will then use in the rest of this guide.

The code to create the Pandas DataFrame we are using in this guide is defined with:

```python title="Python code"
import pandas as pd

dataframe = pd.read_parquet("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-11.parquet")
```

### 4. Add a Data Asset to the Datasource

A Pandas DataFrame Data Asset can be defined with two elements:
- `name`: The name by which the Datasource will be referenced in the future
- `dataframe`: A Pandas DataFrame containing the data

We will use the `dataframe` from the previous step as the corresponding parameter's value.  For the `name` parameter, we will define a name in advance by storing it in a Python variable:

```python title="Python code"
name = "taxi_dataframe"
```

Now that we have the `name` and `dataframe` for our Data Asset, we can create the Data Asset with the code:

```python title="Python code"
data_asset = datasource.add_dataframe_asset(name=name, dataframe=dataframe)
```

## Next steps

Now that you have connected to your data, you may want to look into:

<AfterCreateInMemoryDataAsset />

## Additional information

<!-- TODO: Add this once we have a script.
### Code examples

To see the full source code used for the examples in this guide, please reference the following scripts in our GitHub repository:
- [script_name.py](https://path/to/the/script/on/github.com)
-->

### External APIs

For more information on Pandas read methods, please reference [the official Pandas Input/Output documentation](https://pandas.pydata.org/docs/reference/io.html).

<!-- TODO: Enable this and update links after the conceptual guides are revised
### Related reading

For more information on the concepts and reasoning employed by this guide, please reference the following informational guides:

- [What does a Datasource do behind the scenes?]
- [What are use the use cases for single vs multiple Batch Data Assets?]
-->