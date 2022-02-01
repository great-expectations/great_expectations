---
title: How to connect to an MSSQL database
---
import Prerequisites from '../components/prerequisites.jsx'
import WhereToRunCode from '../components/where_to_run_code.md'
import NextSteps from '../components/next_steps.md'
import Congratulations from '../components/congratulations.md'
import DatabaseCredentials from '../components/adding_database_credentials.md'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide will help you connect to data in a MSSQL database.
This will allow you to validate and explore your data.

<Prerequisites>

- Have access to data in a MSSQL database

</Prerequisites>

## Steps

### 1. Choose how to run the code in this guide

<WhereToRunCode />

### 2. Install required ODBC drivers

Follow guides from Microsoft according to your operating system:

- [Installing Microsoft ODBC driver for MacOS](https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos)
- [Installing Microsoft ODBC driver for Linux](https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server)

### 3. Install required dependencies

First, install the necessary dependencies for Great Expectations to connect to your MSSQL database by running the following in your terminal:

```console
pip install sqlalchemy pyodbc
```

### 4. Add credentials

<DatabaseCredentials />

For this guide we will use a `connection_string` like this:

```
mssql+pyodbc://<USERNAME>:<PASSWORD>@<HOST>:<PORT>/<DATABASE>?driver=<DRIVER>&charset=utf&autocommit=true
```

### 5. Instantiate your project's DataContext

Import these necessary packages and modules.

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/database/mssql_yaml_example.py#L3-L6
```

Load your DataContext into memory using the `get_context()` method.

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/database/mssql_yaml_example.py#L20
```

### 6. Configure your Datasource

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>
  <TabItem value="yaml">

Put your connection string in this template:

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/database/mssql_yaml_example.py#L22-L36
```
Run this code to test your configuration.
```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/database/mssql_yaml_example.py#L45
```

</TabItem>
<TabItem value="python">

Put your connection string in this template:

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/database/mssql_python_example.py#L22-L39
```
Run this code to test your configuration.
```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/database/mssql_python_example.py#L45
```

</TabItem>
</Tabs>

You will see your database tables listed as `Available data_asset_names` in the output of `test_yaml_config()`.

Feel free to adjust your configuration and re-run `test_yaml_config()` as needed.

### 7. Save the Datasource configuration to your DataContext

Save the configuration into your `DataContext` by using the `add_datasource()` function.

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>
  <TabItem value="yaml">

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/database/mssql_yaml_example.py#L47
```

</TabItem>
<TabItem value="python">

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/database/mssql_python_example.py#L47
```

</TabItem>
</Tabs>

### 8. Test your new Datasource

Verify your new Datasource by loading data from it into a `Validator` using a `BatchRequest`.

<Tabs
  defaultValue='runtime_batch_request'
  values={[
  {label: 'Using a SQL query', value:'runtime_batch_request'},
  {label: 'Using a table name', value:'batch_request'},
  ]}>
  <TabItem value="runtime_batch_request">

Here is an example of loading data by specifying a SQL query.

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/database/mssql_yaml_example.py#L50-L63
```

  </TabItem>

  <TabItem value="batch_request">

Here is an example of loading data by specifying an existing table name.

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/database/mssql_yaml_example.py#L69-L80
```

  </TabItem>
</Tabs>

<Congratulations />

## Additional Notes

To view the full scripts used in this page, see them on GitHub:

- [mssql_yaml_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/database/mssql_yaml_example.py)
- [mssql_python_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/database/mssql_python_example.py)

## Next Steps

<NextSteps />
