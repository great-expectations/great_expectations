---
title: How to pass an in-memory DataFrame to a Checkpoint
---

import Prerequisites from '../../../guides/connecting_to_your_data/components/prerequisites.jsx';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

This guide will help you pass an in-memory DataFrame to an existing Checkpoint. This is especially useful if you already have your data in memory due to an existing process such as a pipeline runner.


<Prerequisites>

- [Configured a Data Context](../../../tutorials/getting_started/initialize_a_data_context.md).

</Prerequisites>

## Steps

### 1. Set up Great Expectations
#### Import the required libraries and load your DataContext



```python file=../../../../tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py#L2-L6
```

If you have an existing configured DataContext in your filesystem in the form of a `great_expectations.yml` file, you can load it like this:

```python file=../../../../tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py#L11
```

If you do not have a filesystem to work with, you can load your DataContext following the instructions in [How to instantiate a Data Context without a yml file](../../setup/configuring_data_contexts/how_to_instantiate_a_data_context_without_a_yml_file.md).

### 2. Connect to your data
#### Ensure your DataContext contains a Datasource with a RuntimeDataConnector

In order to pass in a DataFrame at runtime, your `great_expectations.yml` should contain a Datasource configured with a `RuntimeDataConnector`. If it does not, you can add a new Datasource using the code below:

<Tabs
  groupId="yaml-or-python-or-CLI"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  {label: 'CLI', value:'cli'},
  ]}>

<TabItem value="yaml">

```python file=../../../../tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py#L15-L28
```

</TabItem>
<TabItem value="python">

```python file=../../../../tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py#L34-L49
```

</TabItem>
<TabItem value="cli">

```python file=../../../../tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py#L59
```

After running the CLI command above, choose option 1 for "Files on a filesystem..." and then select whether you will be passing a Pandas or Spark DataFrame. Once the Jupyter Notebook opens, change the `datasource_name` to "taxi_datasource" and run all cells to save your Datasource configuration.

</TabItem>
</Tabs>

### 3. Create Expectations and Validate your data
#### Create a Checkpoint and pass it the DataFrame at runtime

You will need an Expectation Suite to validate your data against. If you have not already created an Expectation Suite for your in-memory DataFrame, reference [How to create and edit Expectations with instant feedback from a sample Batch of data](../../expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data.md) to create your suite.

For the purposes of this guide, we have created an empty suite named `my_expectation_suite` by running:

```python file=../../../../tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py#L68
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

```python file=../../../../tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py#L72-L83
```

</TabItem>
<TabItem value="python">

```python file=../../../../tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py#L89-L104
```

</TabItem>
</Tabs>

We can then pass the remaining keys for the in-memory DataFrame (`df`) and it's associated `batch_identifiers` at runtime using `batch_request`:

```python
df = pd.read_csv("<PATH TO DATA>")
```

```python file=../../../../tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py#L118-L126
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

```python file=../../../../tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py#L133-L139
```

</TabItem>
<TabItem value="python">

```python file=../../../../tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py#L145-L151
```

</TabItem>
</Tabs>

We can pass one or more `RuntimeBatchRequest`s into `validations` at runtime. Here is an example that passes multiple `batch_request`s into `validations`:

```python
df_1 = pd.read_csv("<PATH TO DATA 1>")
df_2 = pd.read_csv("<PATH TO DATA 2>")
```

```python file=../../../../tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py#L169-L191
```

## Additional Notes
To view the full script used in this page, see it on GitHub:
- [how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py](https://github.com/great-expectations/great_expectations/tree/develop/tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py)
