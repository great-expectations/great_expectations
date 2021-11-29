---
title: How to pass an in-memory DataFrame to a Checkpoint
---

import Prerequisites from '../../../guides/connecting_to_your_data/components/prerequisites.jsx';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide will help you pass an in-memory DataFrame to an existing Checkpoint.
This is especially useful if you already have your data in-memory due to an existing process such as a pipeline runner.


<Prerequisites>

- Configured a [Data Context](../../../tutorials/getting_started/initialize_a_data_context.md).
- Configured an [Expectations Suite](../../../tutorials/getting_started/create_your_first_expectations.md).
- Configured a [Checkpoint](./how_to_create_a_new_checkpoint)

</Prerequisites>

## Steps

### 1. Ensure your DataContext contains a Datasource with a RuntimeDataConnector

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

## Additional notes

