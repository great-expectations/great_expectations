---
title: How to pass an in-memory DataFrame to a Checkpoint
---

import Prerequisites from '../../../guides/connecting_to_your_data/components/prerequisites.jsx';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

import InProgress from '@site/docs/components/warnings/_in_progress.md'

<InProgress />

This guide will help you pass an in-memory DataFrame to an existing <TechnicalTag tag="checkpoint" text="Checkpoint" />. This is especially useful if you already have your data in memory due to an existing process such as a pipeline runner.


<Prerequisites>

- [Configured a Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_quickly_instantiate_a_data_context).

</Prerequisites>

## Steps

### 1. Set up Great Expectations
#### Import the required libraries and load your DataContext



```python name="tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py imports"
```

If you have an existing configured DataContext in your filesystem in the form of a `great_expectations.yml` file, you can load it like this:

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py get_context"
```

If you do not have a filesystem to work with, you can load your DataContext following the instructions in [How to instantiate a Data Context without a yml file](../../setup/configuring_data_contexts/how_to_instantiate_a_data_context_without_a_yml_file.md).

### 2. Connect to your data
#### Ensure your DataContext contains a Datasource with a RuntimeDataConnector

In order to pass in a DataFrame at runtime, your `great_expectations.yml` should contain a <TechnicalTag tag="datasource" text="Datasource" /> configured with a `RuntimeDataConnector`. If it does not, you can add a new Datasource using the code below:

<Tabs
  groupId="yaml-or-python-or-CLI"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  {label: 'CLI', value:'cli'},
  ]}>

<TabItem value="yaml">

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py datasource_yaml"
```

</TabItem>
<TabItem value="python">

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py datasource_config"
```

</TabItem>
<TabItem value="cli">

```python name=""tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py datasource_new_cli"
```

After running the <TechnicalTag tag="cli" text="CLI" /> command above, choose option 1 for "Files on a filesystem..." and then select whether you will be passing a Pandas or Spark DataFrame. Once the Jupyter Notebook opens, change the `datasource_name` to "taxi_datasource" and run all cells to save your Datasource configuration.

</TabItem>
</Tabs>

### 3. Create Expectations and Validate your data
#### Create a Checkpoint and pass it the DataFrame at runtime

You will need an <TechnicalTag tag="expectation_suite" text="Expectation Suite" /> to <TechnicalTag tag="validation" text="Validate" /> your data against. If you have not already created an Expectation Suite for your in-memory DataFrame, reference [How to create and edit Expectations with instant feedback from a sample Batch of data](../../expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data.md) to create your Expectation Suite.

For the purposes of this guide, we have created an empty suite named `my_expectation_suite` by running:

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py add_expectation_suite"
```

We will now walk through two examples for configuring a `Checkpoint` and passing it an in-memory DataFrame at runtime.

#### Example 1: Pass only the `batch_request`'s missing keys at runtime

If we configure a `SimpleCheckpoint` that contains a single `batch_request` in `validations`:

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>

<TabItem value="yaml">

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py checkpoint_config_yaml_missing_keys"
```

</TabItem>
<TabItem value="python">

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py checkpoint_config_python_missing_keys"
```

</TabItem>
</Tabs>

We can then pass the remaining keys for the in-memory DataFrame (`df`) and it's associated `batch_identifiers` at runtime using `batch_request`:

```python
df = pd.read_csv("<PATH TO DATA>")
```

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py run_checkpoint"
```

#### Example 2: Pass a complete `RuntimeBatchRequest` at runtime

If we configure a `SimpleCheckpoint` that does not contain any `validations`:

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>

<TabItem value="yaml">

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py checkpoint_config_yaml_missing_batch_request"
```

</TabItem>
<TabItem value="python">

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py checkpoint_config_python_missing_batch_request"
```

</TabItem>
</Tabs>

We can pass one or more `RuntimeBatchRequest`s into `validations` at runtime. Here is an example that passes multiple `batch_request`s into `validations`:

```python
df_1 = pd.read_csv("<PATH TO DATA 1>")
df_2 = pd.read_csv("<PATH TO DATA 2>")
```

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py runtime_batch_request"
```

## Additional Notes
To view the full script used in this page, see it on GitHub:
- [how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py](https://github.com/great-expectations/great_expectations/tree/develop/tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py)
