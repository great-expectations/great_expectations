---
title: How to configure a RuntimeDataConnector
---
import Prerequisites from '../connecting_to_your_data/components/prerequisites.jsx'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide demonstrates how to configure a RuntimeDataConnector and only applies to the V3 (Batch Request) API. A `RuntimeDataConnector` allows you to specify a Batch using a Runtime Batch Request, which is used to create a Validator. A Validator is the key object used to create Expectations and validate datasets.

<Prerequisites>

- [Understand the basics of Datasources in the V3 (Batch Request) API](../../reference/datasources.md)
- Learned how to configure a [Data Context using test_yaml_config](../setup/configuring_data_contexts/how_to_configure_datacontext_components_using_test_yaml_config.md)

</Prerequisites>

A RuntimeDataConnector is a special kind of [Data Connector](../../reference/datasources.md) that enables you to use a RuntimeBatchRequest to provide a [Batch's](../../reference/datasources.md#batches) data directly at runtime. The RuntimeBatchRequest can wrap an in-memory dataframe, a filepath, or a SQL query, and must include batch identifiers that uniquely identify the data (e.g. a `run_id` from an AirFlow DAG run). The batch identifiers that must be passed in at runtime are specified in the RuntimeDataConnector's configuration.

## Steps

### 1. Instantiate your project's DataContext

Import these necessary packages and modules:

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>
<TabItem value="yaml">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_runtimedataconnector.py#L3-L4
```

</TabItem>
<TabItem value="python">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_runtimedataconnector.py#L1-L4
```

</TabItem>
</Tabs>

### 2. Set up a Datasource

All of the examples below assume you’re testing configuration using something like:

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>
<TabItem value="yaml">

```python
datasource_yaml = """
name: taxi_datasource
class_name: Datasource
execution_engine:
  class_name: PandasExecutionEngine
data_connectors:
  <DATACONNECTOR NAME GOES HERE>:
    <DATACONNECTOR CONFIGURATION GOES HERE>
"""
context.test_yaml_config(yaml_config=datasource_config)
```

</TabItem>
<TabItem value="python">

```python
datasource_config = {
    "name": "taxi_datasource",
    "class_name": "Datasource",
    "module_name": "great_expectations.datasource",
    "execution_engine": {
        "module_name": "great_expectations.execution_engine",
        "class_name": "PandasExecutionEngine",
    },
    "data_connectors": {
        "<DATACONNECTOR NAME GOES HERE>": {
          "<DATACONNECTOR CONFIGURATION GOES HERE>"
        },
    },
}
context.test_yaml_config(yaml.dump(datasource_config))
```

</TabItem>
</Tabs>

If you’re not familiar with the `test_yaml_config` method, please check out: [How to configure Data Context components using test_yaml_config](../setup/configuring_data_contexts/how_to_configure_datacontext_components_using_test_yaml_config.md)

### 3. Add a RuntimeDataConnector to a Datasource configuration

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>
<TabItem value="yaml">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_runtimedataconnector.py#L9-L21
```

</TabItem>
<TabItem value="python">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_runtimedataconnector.py#L26-L40
```

</TabItem>
</Tabs>

Once the RuntimeDataConnector is configured you can add your datasource using:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_runtimedataconnector.py#L48-L48
```

At runtime, you would get a Validator from the Data Context by first defining a `RuntimeBatchRequest`:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_runtimedataconnector.py#L50-L57
```

and then passing that request into `context.get_validator`:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_runtimedataconnector.py#L64-L68
```
