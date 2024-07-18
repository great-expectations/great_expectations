---
title: Manage Checkpoints
---
import InProgress from '../../_core_components/_in_progress.md';

import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import StepRequestADataContext from '../../_core_components/common_steps/_request_a_data_context.md';

You can use Checkpoints to group Validation Definitions and run them with shared parameters and automated Actions.

At runtime, a Checkpoint can take in dictionaries that filter the Batches in each Validation Definition and modify the parameters of the Expectations that will be validated against them. The parameters apply to every Validation Definition in the Checkpoint.  Therefore, the Validation Definitions grouped in a Checkpoint should have Batch Definitions that accept the same Batch filtering criteria. In addition, the Expectation Suites in each Validation Definition should also share a list of valid parameters.

After a Checkpoint receives Validation Results from a Validation Definition, it executes a list of Actions. The returned Validation Results determine what task is performed for each Action. Actions can include updating Data Docs with the new Validation Results or sending alerts when validations fail.  The Actions list is executed once for each Validation Definition in a Checkpoint.

If the list of Actions for a Checkpoint is empty, the Checkpoint can still run. Its validation results are saved to the Data Context, but no tasks are executed.

## Create a Checkpoint

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Import the GX library and the `Checkpoint` class:

  ```python title="Python" name="/core/validate_data/checkpoints/_examples/create_a_checkpoint.py import statements"
  ```

2. <StepRequestADataContext/>.

  In this example the variable `context` is your Data Context.

3. [Get the Validation Definition](/core/_validate_data/validation_definitions/manage_validation_definitions.md#get-a-validation-definition-by-name) [or Definitions](/core/validate_data/validation_definitions/manage_validation_definitions.md#get-validation-definitions-by-attributes) to add to the Checkpoint.

  In this example the variable `validation_definitions` is a list object containing a single Validation Definition.

4. Optional. Determine the Actions to add to the Checkpoint.

  In this example, the variable `actions` is a list of two actions. The first updates your Data Docs with the results of the Validation Definition. The second sends a Slack notification if any of the Expectations in the Validation Definition failed:

  ```python title="Python" name="/core/validate_data/checkpoints/_examples/create_a_checkpoint.py determine actions"
  ```

  :::tip 
  
  [A list of available Actions](/reference/api/checkpoint/Checkpoint_class.mdx) is available with the Checkpoint class in the GX API documentation.
  
  :::


5. Create the Checkpoint:

  ```python title="Python" name="/core/validate_data/checkpoints/_examples/create_a_checkpoint.py create checkpoint"
  ```

6. Add the Checkpoint to the Data Context:

  ```python title="Python" name="/core/validate_data/checkpoints/_examples/create_a_checkpoint.py add checkpoint"
  ```

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python showLineNumbers title="Python" name="/core/validate_data/checkpoints/_examples/create_a_checkpoint.py full script"
```

</TabItem>

</Tabs>

## List available Checkpoints

<Tabs>

<TabItem value="procedure" label="Procedure">

1. <StepRequestADataContext/>.

  In this example the variable `context` is your Data Context.

2. Use the Data Context to retrieve and print the names of the available Checkpoints:

  ```python title="Python" name="/core/validate_data/checkpoints/_examples/list_available_checkpoints.py print checkpoint names"
  ```
  
</TabItem>

<TabItem value="sample_code" label="Sample code">

```python showLineNumbers title="Python" name="/core/validate_data/checkpoints/_examples/list_available_checkpoints.py full example code"
```

</TabItem>

</Tabs>

## Get a Checkpoint by name

<Tabs>

<TabItem value="procedure" label="Procedure">

1. <StepRequestADataContext/>.

  In this example the variable `context` is your Data Context.

2. Use the Data Context to request the Checkpoint:

  ```python title="Python" name="/core/validate_data/checkpoints/_examples/get_checkpoint_by_name.py get checkpoint by name"
  ```

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python showLineNumbers title="Python" name="/core/validate_data/checkpoints/_examples/get_checkpoint_by_name.py full example script"
```

</TabItem>

</Tabs>

## Get Checkpoints by attributes

<Tabs>

<TabItem value="procedure" label="Procedure">

1. <StepRequestADataContext/>.

  In this example the variable `context` is your Data Context.

2. Determine the filter attributes.

  Checkpoints associate a list of one or more Validation Definitions with a list of Actions to perform.  The attributes used to filter the results include the attributes for the Validation Definitions, their Batch Definitions and Expectation Suites, and the Checkpoint Actions.

3. Use a list comprehension to return all Checkpoints that match the filtered attributes.

  For example, you can retrieve all Checkpoints that send alerts through Slack by filtering on the Actions of each Checkpoint:

  ```python title="Python" name="/core/validate_data/checkpoints/_examples/get_checkpoints_by_attributes.py filter checkpoints list on actions"
  ```

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python showLineNumbers title="Python" name="/core/validate_data/checkpoints/_examples/get_checkpoints_by_attributes.py full example code"
```

</TabItem>

</Tabs>

## Update a Checkpoint

<Tabs>

<TabItem value="procedure" label="Procedure">

1. <StepRequestADataContext/>.

  In this example the variable `context` is your Data Context.

2. [Get the Checkpoint to update](#get-a-checkpoint-by-name).

  In this example the variable `checkpoint` is the Checkpoint that is updated.

3. Overwrite the Checkpoint values with updated lists.

  In this example, the Checkpoint Validation Definitions and Actions receive updates:

  ```python title="Python" name="/core/validate_data/checkpoints/_examples/update_a_checkpoint.py full update values"
  ```

4. Save the changes to the Data Context:

  ```python title="Python" name="/core/validate_data/checkpoints/_examples/update_a_checkpoint.py full save updates"
  ```

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python showLineNumbers title="Python" name="/core/validate_data/checkpoints/_examples/update_a_checkpoint.py full example script"
```

</TabItem>

</Tabs>

## Delete a Checkpoint

<Tabs>

<TabItem value="procedure" label="Procedure">

1. <StepRequestADataContext/>.

  In this example the variable `context` is your Data Context.

2. Use the Data Context to delete the Checkpoint:

  ```python title="Python" name="/core/validate_data/checkpoints/_examples/delete_a_checkpoint.py delete checkpoint"
  ```

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python showLineNumbers title="Python" name="/core/validate_data/checkpoints/_examples/delete_a_checkpoint.py full example script"
```

</TabItem>

</Tabs>

## Run a Checkpoint

<InProgress/>