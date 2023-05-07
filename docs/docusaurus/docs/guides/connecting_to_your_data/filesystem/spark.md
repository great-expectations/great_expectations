---
title: How to connect to data on a filesystem using Spark
---

import NextSteps from '../components/next_steps.md'
import Congratulations from '../components/congratulations.md'
import Prerequisites from '../components/prerequisites.jsx'
import WhereToRunCode from '../components/where_to_run_code.md'
import SparkDataContextNote from '../components/spark_data_context_note.md'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

This guide will help you connect to your data stored on a filesystem using Spark. This will allow you to <TechnicalTag tag="validation" text="Validate" /> and explore your data.

## Prerequisites

<Prerequisites>

- Have access to a working Spark installation
- Have access to data on a filesystem

</Prerequisites>

## Steps

### 1. Choose how to run the code in this guide

<WhereToRunCode />

### 2. 💡 Instantiate your project's DataContext

Import these necessary packages and modules.

```python name="tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_yaml_example.py imports"
```

<SparkDataContextNote />

Please proceed only after you have instantiated your `DataContext`.

### 3. Configure your Datasource

Using this example configuration, add in your path to a directory that contains some of your data:

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>
  <TabItem value="yaml">

```python name="tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_yaml_example.py yaml"
```

Run this code to test your configuration.
```python name="tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_yaml_example.py test_yaml_config"
```

</TabItem>
<TabItem value="python">

```python name="tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_python_example.py python"
```

Run this code to test your configuration.

```python name="tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_python_example.py test_yaml_config"
```

</TabItem>
</Tabs>

If you specified a path containing CSV files you will see them listed as `Available data_asset_names` in the output of `test_yaml_config()`.

Feel free to adjust your configuration and re-run `test_yaml_config()` as needed.

### 4. Save the Datasource configuration to your DataContext

Save the configuration into your <TechnicalTag tag="data_context" text="Data Context" /> by using the `add_datasource()` function.

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>

<TabItem value="yaml">

```python name="tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_yaml_example.py test_yaml_config"
```

</TabItem>
<TabItem value="python">

```python name="tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_python_example.py add_datasource"
```

</TabItem>
</Tabs>

### 5. Test your new Datasource

Verify your new <TechnicalTag tag="datasource" text="Datasource" /> by loading data from it into a <TechnicalTag tag="validator" text="Validator" /> using a <TechnicalTag tag="batch_request" text="Batch Request" />.

<Tabs
  defaultValue='runtime_batch_request'
  values={[
  {label: 'Specify a path to single CSV', value:'runtime_batch_request'},
  {label: 'Specify a data_asset_name', value:'batch_request'},
  ]}>
  <TabItem value="runtime_batch_request">

Add the path to your CSV in the `path` key under `runtime_parameters` in your `BatchRequest`.
```python name="tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_yaml_example.py runtime_batch_request"
```

Then load data into the Validator.

```python name="tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_yaml_example.py runtime_batch_request validator"
```

  </TabItem>
  <TabItem value="batch_request">

Add the name of the <TechnicalTag tag="data_asset" text="Data Asset" /> to the `data_asset_name` in your `BatchRequest`.

```python name="tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_yaml_example.py batch_request"
```

Then load data into the `Validator`.
```python name="tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_yaml_example.py batch_request validator"
```

  </TabItem>
</Tabs>


<Congratulations />

## Additional Notes

#### How to read-in multiple CSVs as a single Spark Dataframe

More advanced configuration for reading in CSV files through the `SparkDFExecutionEngine` is possible through the `batch_spec_passthrough` parameter.  `batch_spec_passthrough` allows for reader-methods to be directly specified, 
and backend-specific `reader_options` to be passed through to the actual reader-method, in this case `spark.read.csv()`. The following example shows how `batch_spec_passthrough` parameters can be added to the `BatchRequest`. However,
the same parameters can be added to the Datasource configuration at the DataConnector level. 

If you have a directory with 3 CSV files with each file having 10,000 lines each: 

```bash
  taxi_data_files/yellow_tripdata_sample_2019-1.csv
  taxi_data_files/yellow_tripdata_sample_2019-2.csv
  taxi_data_files/yellow_tripdata_sample_2019-3.csv
```

You could write a `BatchRequest` that reads in the entire folder as a single Spark Dataframe by specifying the `reader_method` to be `csv`, `header` to be set to `True` in the `reader_options`. 

```python name="tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_python_example.py batch_request directory"
```

Once that step is complete, then we can confirm that our Validator contains a <TechnicalTag tag="batch" text="Batch" /> with the expected 30,000 lines. 

```python name="tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_python_example.py batch_request directory validator"
```

To view the full scripts used in this page, see them on GitHub:

- [spark_yaml_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_yaml_example.py)
- [spark_python_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_python_example.py)

## Next Steps

<NextSteps />
