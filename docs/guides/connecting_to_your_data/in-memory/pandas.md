---
title: How to connect to in-memory data in a Pandas dataframe
---

import NextSteps from '../components/next_steps.md'
import Congratulations from '../components/congratulations.md'
import Prerequisites from '../components/prerequisites.jsx'
import WhereToRunCode from '../components/where_to_run_code.md'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide will help you connect to your data that is an in-memory Pandas dataframe.
This will allow you to validate and explore your data.

<Prerequisites>

- Have access to data that can be loaded as a Pandas dataframe

</Prerequisites>

## Steps

### 1. Choose how to run the code in this guide

<WhereToRunCode />

### 2. `[🍏 CORE SKILL ICON]` Instantiate your project's DataContext

Import these necessary packages and modules.

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/in-memory/pandas_yaml_example.py#L1-L6
```

Load your DataContext into memory using the `get_context()` method.

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/in-memory/pandas_yaml_example.py#L8
```


### 3. Configure your Datasource

Using this example configuration we configure a `RuntimeDataConnector` as part of our Datasource, which will take in our in-memory frame.:

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'python', value:'python'},
  ]}>
  <TabItem value="yaml">

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/in-memory/pandas_yaml_example.py#L10-L22
```

Run this code to test your configuration.

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/in-memory/pandas_yaml_example.py#L24
```

**Note**: Since the Datasource does not have data passed-in until later, the output will show that no `data_asset_names` are currently available. This is to be expected.

</TabItem>
<TabItem value="python">

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/in-memory/pandas_python_example.py#L10-L25
```

Run this code to test your configuration.

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/in-memory/pandas_python_example.py#L27
```

**Note**: Since the Datasource does not have data passed-in until later, the output will show that no `data_asset_names` are currently available. This is to be expected.

</TabItem>
</Tabs>


### 4. Save the Datasource configuration to your DataContext

Save the configuration into your `DataContext` by using the `add_datasource()` function.

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'python', value:'python'},
  ]}>
  <TabItem value="yaml">

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/in-memory/pandas_yaml_example.py#L26
```

</TabItem>
<TabItem value="python">

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/in-memory/pandas_python_example.py#L29
```

</TabItem>
</Tabs>

### 5. (Optional) Create Example Data

In most cases you will already have an in-memory dataframe already. For demonstrative purposes, here is a simple dataframe with 3 columns and 3 rows.

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/in-memory/pandas_yaml_example.py#L29
```

Which looks like this:

```bash
df

   a  b  c
0  1  2  3
1  4  5  6
2  7  8  9
```

### 6. Test your new Datasource

Verify your new Datasource by loading your dataframe into a `Validator` using a `BatchRequest` with the dataframe passed in as `batch_data`.


```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/in-memory/pandas_yaml_example.py#L32-L40
```

Then load data into the `Validator`.
```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/in-memory/pandas_yaml_example.py#L42-L48
```

<Congratulations />

## Additional Notes

To view the full scripts used in this page, see them on GitHub:

- [pandas_yaml_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/in-memory/pandas_yaml_example.py)
- [pandas_python_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/in-memory/pandas_python_example.py)

## Next Steps

<NextSteps />
