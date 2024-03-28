---
title: Manage Checkpoints
---
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import StepRequestADataContext from '../../_core_components/common_steps/_request_a_data_context.md';

A Checkpoint associates one or more Validation Definitions with a list of Actions to perform based on the Validation Results returned by each Validation Definition.  Actions can include things such as updating Data Docs with the new Validation Results or sending alerts when validations fail.

## Create a Checkpoint

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Import the GX library and the Checkpoint class:

```python title="Python" name="/core/validate_data/checkpoints/_examples/create_a_checkpoint.py import statements"
```

2. Get a Data Context

3. Get the Validation Definition or Definitions to add to the Checkpoint.

4. Create the Checkpoint:

```python title="Python" name="/core/validate_data/checkpoints/_examples/create_a_checkpoint.py create checkpoint"
```

5. Add the Checkpoint to the Data Context:

```python title="Python" name="/core/validate_data/checkpoints/_examples/create_a_checkpoint.py add checkpoint"
```

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python title="Python" name="/core/validate_data/checkpoints/_examples/create_a_checkpoint.py full script"
```

</TabItem>

</Tabs>

## List available Checkpoints

<Tabs>

<TabItem value="procedure" label="Procedure">

1. <StepRequestADataContext/>.

  In this example the variable `context` is your Data Context.

2. Use the Data Context to list the names of the available Checkpoints:

  ```python title="Python" name="/core/validate_data/checkpoints/_examples/list_available_checkpoints.py print checkpoint names"
  ```

3. Optional. Filter the list of Checkpoints using a list comprehension and attribute comparison.

  In this example, the list of available Checkpoints is filtered to include only those Checkpoints that are associated with a specific Data Asset:

  ```python title="Python" name="/core/validate_data/checkpoints/_examples/list_available_checkpoints.py filter checkpoints list"
  ```
  
</TabItem>

<TabItem value="sample_code" label="Sample code">

```python title="Python" name="/core/validate_data/checkpoints/_examples/list_available_checkpoints.py full example code"
```

</TabItem>

</Tabs>

## Get an existing Checkpoint by name

## Add or remove Actions from a Checkpoint

## Add or remove Validation Definitions from a Checkpoint

## Delete a Checkpoint

## Run a Checkpoint