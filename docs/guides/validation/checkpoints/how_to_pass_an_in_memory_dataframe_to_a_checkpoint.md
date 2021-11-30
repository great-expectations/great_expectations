---
title: How to pass an in-memory DataFrame to a Checkpoint
---

import Prerequisites from '../../../guides/connecting_to_your_data/components/prerequisites.jsx';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide will help you pass an in-memory DataFrame to an existing Checkpoint.
This is especially useful if you already have your data in memory due to an existing process such as a pipeline runner.


<Prerequisites>

- Configured a [Data Context](../../../tutorials/getting_started/initialize_a_data_context.md).
- Configured an [Expectation Suite](../../../tutorials/getting_started/create_your_first_expectations.md).
- Configured a [Checkpoint](./how_to_create_a_new_checkpoint)

</Prerequisites>

## Steps

### 1. Import the required libraries and load your DataContext

```python file=../../../../tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py#L1-L7
```

### 2. Ensure your DataContext contains a Datasource with a RuntimeDataConnector

In order to pass in a DataFrame at runtime, your `great_expectations.yml` should contain a Datasource configured with a `RuntimeDataConnector`. If it does not, you can add a new Datasource using the code below:

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>
<TabItem value="yaml">

```python file=../../../../tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py#L10-L23
```

</TabItem>
<TabItem value="python">

```python file=../../../../tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py#L28-L43
```

</TabItem>
</Tabs>

### 3. Create a Checkpoint and pass it the DataFrame at runtime

You will need an Expectation Suite to validate your data against. For the purposes of this example, we have created an empty suite named `my_expectation_suite`:

```python file=../../../../tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py#L53
```

#### Example 1: Pass only the complementary keys at runtime 

If we configure a `SimpleCheckpoint` that contains a single `batch_request` in `validations`:

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>
<TabItem value="yaml">

```python file=../../../../tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py#L56-L67
```

</TabItem>
<TabItem value="python">

```python file=../../../../tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py#L72-L87
```

</TabItem>
</Tabs>

We can then pass the remaining keys for the in-memory DataFrame (`df`) and it's associated `batch_identifiers` at runtime using `batch_request`:

```python file=../../../../tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py#L97-L107
```

#### Example 2: Pass a complete RuntimeBatchRequest at runtime

If we configure a `SimpleCheckpoint` that does not contain any `validations`:

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>
<TabItem value="yaml">

```python file=../../../../tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py#L113-L119
```

</TabItem>
<TabItem value="python">

```python file=../../../../tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py#L124-L130
```

</TabItem>
</Tabs>

We can pass one or more `RuntimeBatchRequest`s into `validations` at runtime. Here is an example that passes multiple `batch_request`s into `validations`:

```python
df_1 = pd.read_csv("<PATH TO DATA 1>")
df_2 = pd.read_csv("<PATH TO DATA 2>")
```

```python file=../../../../tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py#L143-L165
```

### Additional Notes
To view the full script used in this page, see it on GitHub:
- [how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py](https://github.com/great-expectations/great_expectations/tree/develop/tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py)
