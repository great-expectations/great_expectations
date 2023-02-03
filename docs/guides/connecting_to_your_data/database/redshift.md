---
title: How to connect to a Redshift database
---
import Prerequisites from '../components/prerequisites.jsx'
import WhereToRunCode from '../components/where_to_run_code.md'
import NextSteps from '../components/next_steps.md'
import Congratulations from '../components/congratulations.md'
import DatabaseCredentials from '../components/adding_database_credentials.md'
import RedshiftCredentials from './components/_redshift_credentials.md'
import RedshiftDependencies from './components/_redshift_dependencies.md'
import RedshiftDatasourceConfiguration from './components/_datasource_redshift_configuration.md'
import RedshiftDatasourceTest from './components/_datasource_redshift_test.md'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

This guide will help you connect to data in a Redshift database.
This will allow you to <TechnicalTag tag="validation" text="Validate" /> and explore your data.

<Prerequisites>

- Have access to data in a Redshift database

</Prerequisites>

## Steps

### 1. Choose how to run the code in this guide

<WhereToRunCode />

### 2. Install required dependencies

<RedshiftDependencies />

### 3. Add credentials

<DatabaseCredentials />

<RedshiftCredentials />

### 4. Instantiate your project's DataContext

Import these necessary packages and modules.

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/database/redshift_yaml_example.py#L3-L6
```

Load your DataContext into memory using the `get_context()` method.

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/database/redshift_yaml_example.py#L26
```

### 5. Configure your Datasource

<RedshiftDatasourceConfiguration />

### 6. Save the Datasource configuration to your DataContext

Save the configuration into your `DataContext` by using the `add_datasource()` function.

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>

<TabItem value="yaml">

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/database/redshift_yaml_example.py#L53
```

</TabItem>

<TabItem value="python">

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/database/redshift_python_example.py#L53
```

</TabItem>

</Tabs>

### 7. Test your new Datasource

<RedshiftDatasourceTest />



<Congratulations />

## Additional Notes

To view the full scripts used in this page, see them on GitHub:

- [redshift_yaml_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/database/redshift_yaml_example.py)
- [redshift_python_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/database/redshift_python_example.py)

## Next Steps

<NextSteps />
