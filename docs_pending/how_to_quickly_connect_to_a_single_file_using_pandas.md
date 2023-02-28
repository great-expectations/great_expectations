---
title: How to quickly connect to a single file using Pandas
tag: [how-to,connect to data]
description: A brief how-to guide covering ...
keywords: [Great Expectations,Pandas,Filesystem]
---

<!-- Import statements start here. -->
import Prerequisites from '/docs/components/_prerequisites.jsx'

## Introduction

In this guide we will use Pandas to quickly connect to the data found within a single `.csv` file. 

This process is ideal for performing data exploration and analysis on a single file.  With the returned object you can immediately begin creating new Expectations from a single file and evaluating that file against them.

If you wish to connect to multiple files, or if you wish to connect to data for use with Expectations you have previously created, please reference:
- How to connect to one or more files using Pandas

## Prerequisites

<Prerequisites requirePython = {false} requireInstallation = {true} requireDataContext = {true} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- Access to source data stored in a filesystem
- A Python script or interpreter to work in
  - We recommend using a Jupyter Notebook for the best of both
- A passion for data quality

</Prerequisites> 

## Steps

### 1. Import Great Expectations

First, we import the Great Expectations module.

```python title="Python code"
import great_expectations as gx
```

### 2. Initialize a Data Context

Once we have imported the Great Expectations module, we can instantiate our Data Context.

```python title="Python code"
context = gx.get_context()
```

### 3. Define the data to connect to in the default Datasource

Using our Data Context, we can now instantiate a Validator by specifying a file to read.  For purposes of our example, we will say that our path is stored in a variable named `path_to_our_csv`.  The value of this variable can be a string representation of a file path, or a `Path` object from Python's `pathlib` module.

In this example, our file is being hosted in GitHub.  You may update this value to be the path to a local file on your system, instead.

```python title="Python code"
path_to_our_csv = "https://raw.githubusercontent.com/great_expectations/taxi_data.csv"
datasource = context.datasources.pandas_default
data_asset = datasource.read_csv(path=path_to_our_csv)
validator = context.datasources.pandas_default.read_csv(path=path_to_our_csv)
```

:::info Connecting to other file types

Other file types than `.csv` are supported.  The code used will be similar to that shown above.  To connect to other file types, simply replace `read_csv(...)` with an appropriate method for another file type.

Since you are using Pandas to connect to the file, the available reader methods will depend on the version of Pandas you have installed.  To see exactly what is available to you, please reference [the official Pandas Input/Output API documentation] for your version of Pandas.  Most `read_*` methods are supported by Great Expectations.

:::

## Next steps

Now that you have a Validator, you may want to look into:
- How to interactively create Expectations from a Validator

## Additional information

<!-- TODO: Add me once we have a script under test.
### Code examples

To see the full source code used for the examples in this guide, please reference the following script in our GitHub repository:
- [script_name.py](https://path/to/the/script/on/github.com)
-->

### GX Python APIs

For more information on the GX Python objects and APIs used in this guide, please reference the following pages of our public API documentation:

- [`python_object`](/docs/link/to/corresponding/object/in/api/reference/pages.md)
- [`PythonObject.python_command(...)`](/docs/link/to/corresponding/api/reference/page.md#header_for_corresponding_command)
- [`PythonModule.other_python_command(...)`](/docs/link/to/corresponding/other_api/reference/page.md#header_for_corresponding_command)

### External APIs

For more information on the `ruamel` module, please reference [`ruamel`'s official documentation](https://link/to/corresponding/docs.html).

### Related reading

For more information on the concepts and reasoning employed by this guide, please reference the following informational guides:

- [Why are Jupyter Notebooks the recommended interface for GX scripting?](/docs/link/to/conceptual/guide.md)
- [What does a Datasource do behind the scenes?](/docs/corresponding/link.md)