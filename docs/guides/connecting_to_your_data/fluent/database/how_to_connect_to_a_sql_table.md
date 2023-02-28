---
title: How to connect to a SQL table
tag: [how-to, connect to data]
description: A brief how-to guide covering ...
keywords: [Great Expectations, SQL]
---

<!-- Import statements start here. -->
import Prerequisites from '/docs/components/_prerequisites.jsx'

## Introduction

## Prerequisites

<Prerequisites requirePython = {false} requireInstallation = {false} requireDataContext = {false} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- Source data stored in a SQL database
- A passion for data quality

</Prerequisites> 

## Steps

### 1. Import GX and instantiate a Data Context

```python title="Python code"
import great_expectations as gx

context = gx.get_context()
```

### 2. Determine your connection string


You can use either environment variables or a key in `config_variables.yml` to safely store any passwords needed by your connection string.  After defining your password in one of those ways, you can reference it in your connection string like this:

```python title="Python code"
connection_string="postgresql+psycopg2://username:${MY_PASSWORD}@localhost/test"
```
In the above example `MY_PASSWORD` would be the name of the environment variable or the key to the value in `config_variables.yml` that corresponds to your credentials.

If you include a password as plain text in your connection string when you define your Datasource, GX will automatically strip it out, add it to `config_variables.yml` and substitute it with a variable as was shown above.

For purposes of this guide's examples, we will store our connection string in the variable `sql_connection_string` with plain text credentials:

```python title="Python code"
sql_connection_string = "postgresql+psycopg2://username:my_password@localhost/test"
```

### 3. Create a SQL Datasource

```python title="Python code"
datasource = context.sources.add_sql(name="my_datasource", connection_string=sql_connection_string)
```

### 4. Add a table to the Datasource as a Data Asset

```python title="Python code"
table_asset = datasource.add_table_asset(name="my_asset", table_name="yellow_tripdata_sample")
```

### 5. (Optional) Add a Splitter to the table to divide it into Batches

```python title="Python code"
table_asset.add_year_and_month_splitter(column_name="pickup_datetime")
```

:::tip Splitters and Batch Identifiers

When requesting data from a table Data Asset you can use the command `table_asset.batch_request_options_template()` to see how to specify your Batch Request.  This will include the Batch Identifier keys that your splitter has added to your table Data Asset.

::: 

### 6. (Optional) Add a Batch Sorter to the table

When requesting data, Batches are returned as a list.  By adding a sorter to your table Data Asset you can define the order in which Batches appear in that list.  This will allow you to request a specific Batch by its list index rather than by its Batch Identifiers.

```python title="Python code"
table_asset.add_sorters(["-year", "+month"])
```

### 7. (Optional) Repeat steps 4-6 as needed to add additional tables

If you wish to connect to additional tables in the same SQL Database, simply repeat the steps above to add them as table Data Assets.

## Next steps

Now that you have a SQL Datasource, you may be interested in:
- How to configure a SQL Data Asset to split its data into multiple Batches
- How to configure a SQL Data Asset to provide a sampling of its full data
- How to request a Batch of data from a Datasource
- How to create Expectations while interactively evaluating a set of data
- How to use a Data Assistant to evaluate data

## Additional information

### Code examples

To see the full source code used for the examples in this guide, please reference the following scripts in our GitHub repository:
- [script_name.py](https://path/to/the/script/on/github.com)

### Terminal API and reference links

For more information on the CLI commands used in this guide, please reference the following CLI API guides:

- [`command as entered in the terminal`](/docs/cli/relevant_command.md)
- [`other command as entered in the terminal`](/docs/cli/other_relevant_command.md)

For more information on the `boto3` command line tool used by this guide, please see the [official documentation for `boto3`](https:/corresponding/link.com).

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