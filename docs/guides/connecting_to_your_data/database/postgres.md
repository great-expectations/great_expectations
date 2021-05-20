---
title: How to connect to your data in a postgresql database
---
import Prerequisites from '../components/prerequisites.jsx'
import WhereToRunCode from '../components/where_to_run_code.md'
import NextSteps from '../components/next_steps.md'
import Congratulations from '../components/congratulations.md'


import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide will help you connect to data in a Postgresql database.
This will allow you to validate and explore your data.

<Prerequisites>

- Have access to data in a Postgres database

</Prerequisites>

<WhereToRunCode />

## Steps

### 1. Install required dependencies

First, install the necessary dependencies for Great Expectations to connect to your postgres database.

```console
pip install sqlalchemy psycopg2
```

### 2. Add credentials

Great Expectations provides multiple methods of using credentials for accessing databases.
Options include using an file not checked into source control, environment variables, and using a cloud secret store.
Please read the article [Credential storage and usage options](../advanced/database_credentials) for instructions on alternatives.

For this guide we will use a `connection_string` like this:

```
postgresql+psycopg2://<USERNAME>:<PASSWORD>@<HOST>:<PORT>/<DATABASE>
```   

### 3. `[🍏 CORE SKILL ICON]` Instantiate your project's DataContext

Import these necessary packages and modules.

```python file=../../../../integration/code/connecting_to_your_data/database/postgres.py#L1-L3
```

Load your DataContext into memory using the `get_context()` method.

```python file=../../../../integration/code/connecting_to_your_data/database/postgres.py#L15
```

### 4. Configure your Datasource

<Tabs
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'python', value:'python'},
  ]}>
  <TabItem value="yaml">

Put your connection string in this template:

```python file=../../../../integration/code/connecting_to_your_data/database/postgres.py#L17-L31
```
</TabItem>
<TabItem value="python">

Put your connection string in this template:

```python
datasource_config = {
    'name': 'my_postgres_datasource',
    'class_name': 'Datasource',
    'execution_engine': {
        'class_name': 'SqlAlchemyExecutionEngine',
        'connection_string': 'postgresql+psycopg2://<USERNAME>:<PASSWORD>@<HOST>:<PORT>/<DATABASE>'
    },
    'data_connectors': {
        'default_runtime_data_connector_name': {
            'class_name': 'RuntimeDataConnector',
            'batch_identifiers': ['default_identifier_name']
        },
        'default_inferred_data_connector_name': {
              'class_name': 'InferredAssetSqlDataConnector',
              'name': 'whole_table'
        }
    }
}
```

</TabItem>

</Tabs>

### 5. Save the Datasource configuration to your DataContext

Save the configuration into your `DataContext` by using the `add_datasource()` function.

```python file=../../../../integration/code/connecting_to_your_data/database/postgres.py#L37
```

:::warning TODO
Using this method secrets may be stored in a `great_expectations.yml` file risking leakage via source control.

**Ideas**
- Use the convenience function the `datasource new` notebooks use?
- Port said function to the data context itself? (probably!)
:::

### 6. Test your new Datasource

Verify your new Datasource by loading data from it into a `Validator` using a `BatchRequest`.

<Tabs
  defaultValue='runtime_batch_request'
  values={[
  {label: 'Using a SQL query', value:'runtime_batch_request'},
  {label: 'Using a table name', value:'batch_request'},
  ]}>
  <TabItem value="runtime_batch_request">

Here is an example of loading data by specifying a SQL query.

```python file=../../../../integration/code/connecting_to_your_data/database/postgres.py#L40-L54
```

  </TabItem>

  <TabItem value="batch_request">

Here is an example of loading data by specifying an existing table name.

```python file=../../../../integration/code/connecting_to_your_data/database/postgres.py#L57-L70
```

  </TabItem>
</Tabs>

<Congratulations />

## Additional Notes

To view the full script [see it on GitHub](https://github.com/great-expectations/great_expectations/blob/knoxpod/integration/code/connecting_to_your_data/database/postgres.py)

## Next Steps

<NextSteps />
