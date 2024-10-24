---
title: Run a Checkpoint
---
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PrereqPythonInstalled from '../_core_components/prerequisites/_python_installation.md';
import PrereqGxInstalled from '../_core_components/prerequisites/_gx_installation.md';
import PrereqPreconfiguredDataContext from '../_core_components/prerequisites/_preconfigured_data_context.md';
import PrereqCheckpoint from '../_core_components/prerequisites/_checkpoint.md';

Running a Checkpoint will cause it to validate all of its Validation Definitions.  It will then execute its Actions based on the results returned from those Validation Definitions.  Finally, the Validation Results will be returned by the Checkpoint.

At runtime, a Checkpoint can take in a `batch_parameters` dictionary that selects the Batch to validate from each Validation Definition.  A Checkpoint will also accept an `expectation_parameters` dictionary that provides values for the parameters of the any Expectations that have been configured to accept parameters at runtime.

<h2>Prerequisites</h2>
- <PrereqPythonInstalled/>.
- <PrereqGxInstalled/>.
- <PrereqPreconfiguredDataContext/>.
- <PrereqCheckpoint/>.

### Procedure

<Tabs 
   queryString="procedure"
   defaultValue="instructions"
   values={[
      {value: 'instructions', label: 'Instructions'},
      {value: 'sample_code', label: 'Sample code'}
   ]}
>

<TabItem value="instructions" label="Instructions">

In this procedure your Data Context is assumed to be stored in the variable `context` and your Checkpoint is assumed to be stored in the variable `checkpoint`.

1. Optional. Define Batch parameters.

   Batch parameters are used to specify a Batch of data for retrieval from a Batch Definition.  Batch parameters are provided as a dictionary to the `batch_parameters` argument of a Checkpoint's `run(...)` method.

   The Batch parameters accepted by a Validation Definition are determined by its Batch Definition.

   Batch parameters apply to every Validation Definition in the Checkpoint.  Therefore, the Validation Definitions grouped in a Checkpoint should have Batch Definitions that accept the same Batch filtering criteria.
   
   For more information and examples of how to configure a Batch Definition to accept Batch Parameters and how to format a `batch_parameters` dictionary, see [Connect to data using SQL: Create a Batch Definition](/core/connect_to_data/sql_data/sql_data.md?batch_definition=partitioned#create-a-batch-definition).

   If Batch parameters are not set, each Validation Definition will run on the default Batch determined by its Batch Definition.

2. Optional. Define Expectation Parameters.

   To pass parameters to Expectations at runtime the Expectation must be configured to find parameter values through a dictionary lookup.  This is done when the Expectation is created.

   You then pass a dictionary to the `expectation_parameters` argument of a Checkpoint's `run` method.  The contents of this dictionary consist of keys that were defined for parameters when the Checkpoint's Expectations were created, paired with the values that should be used for the corresponding parmeters when the Checkpoint runs.

   Below is an example of an `ExpectColumnMaxToBeBetween` Expectation that is set to accept parameters at runtime:

   ```python title="Python" name="docs/docusaurus/docs/core/trigger_actions_based_on_results/_examples/run_a_checkpoint.py - example Expectation"
   ```
   
   And this is an `expectation_parameters` dictionary that provides those parameters:
   
   ```python title="Python" name="docs/docusaurus/docs/core/trigger_actions_based_on_results/_examples/run_a_checkpoint.py - define Expectation Parameters"
   ```

   If none of the Expectations in a Validation Definition are configured to accept runtime Expectation parameters, the `expectation_parameters` argument can be omitted from the Checkpoint's `run(...)` method.
   
   For more information on configuring an Expectation to accept runtime parameters, how to set the lookup key for an Expectation's parameters, and additional examples of how to format an `expectation_parameters` dictionary see the runtime guidance under [Create an Expectation](/core/define_expectations/create_an_expectation.md).

3. Run the Checkpoint.

   A Checkpoint is executed through its `run(...)` method.  Pass the Batch and Expectation parameters to the `batch_parameters` and `expectation_parameters` arguments of the Checkpoint's ` run(...)` method:

   ```python title="Python" name="docs/docusaurus/docs/core/trigger_actions_based_on_results/_examples/run_a_checkpoint.py - run a Checkpoint"
   ```
   
   After the Checkpoint runs it will pass the Validation Results that are generated to its Actions and execute them.  Finally, the Validation Results will be returned by the `run(...)` method. 

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python title="Python" name="docs/docusaurus/docs/core/trigger_actions_based_on_results/_examples/run_a_checkpoint.py - full code example"
```

</TabItem>

</Tabs>