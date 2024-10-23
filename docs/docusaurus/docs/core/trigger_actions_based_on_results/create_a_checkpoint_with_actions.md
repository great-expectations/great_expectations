---
title: Create a Checkpoint with Actions
---
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PrereqPythonInstalled from '../_core_components/prerequisites/_python_installation.md';
import PrereqGxInstalled from '../_core_components/prerequisites/_gx_installation.md';
import PrereqPreconfiguredDataContext from '../_core_components/prerequisites/_preconfigured_data_context.md';
import PrereqValidationDefinition from '../_core_components/prerequisites/_validation_definition.md';

A Checkpoint executes one or more Validation Definitions and then performs a set of Actions based on the Validation Results each Validation Definition returns.

<h2>Prerequisites</h2>

- <PrereqPythonInstalled/>.
- <PrereqGxInstalled/>.
- <PrereqPreconfiguredDataContext/>. In this guide the variable `context` is assumed to contain your Data Context.
- <PrereqValidationDefinition/>.

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

1. Retrieve the Validation Definitions the Checkpoint will run.

   In the following example your Data Context has already been stored in the variable `context` and an existing Validation Definition is retrieved from it:

   ```python title="Python" name="docs/docusaurus/docs/core/trigger_actions_based_on_results/_examples/create_a_checkpoint_with_actions.py - create a Validation Definitions list"
   ```

2. Determine the Actions that the Checkpoint will automate.

   After a Checkpoint receives Validation Results from running a Validation Definition, it executes a list of Actions. The returned Validation Results determine what task is performed for each Action. Actions can include updating Data Docs with the new Validation Results or sending alerts when validations fail.  The Actions list is executed once for each Validation Definition in a Checkpoint.

   Actions can be found in the `great_expectations.checkpoint` module.  All Action class names end with `*Action`.

   The following is an example of how to create an Action list that will trigger a `SlackNotificationAction` and an `UpdateDataDocsAction`:

   ```python title="Python" name="docs/docusaurus/docs/core/trigger_actions_based_on_results/_examples/create_a_checkpoint_with_actions.py - define an Action list"
   ```

   In the above example, string substitution is used to pull the value of `slack_token` from an environment variable.  For more information on securely storing credentials and access tokens see [Configure credentials](/core/configure_project_settings/configure_credentials/configure_credentials.md).

   If the list of Actions for a Checkpoint is empty, the Checkpoint can still run. Its Validation Results are saved to the Data Context, but no tasks are executed.

3. Optional. Choose the Result Format

   When a Checkpoint is created you can adjust the verbosity of the Validation Results it generates by setting a Result Format.  A Checkpoint's Result Format will be applied to all Validation Results in the Checkpoint every time they are run.  By default, a Checkpoint uses a `SUMMARY` result format: it indicates the success or failure of each Expectation in a Validation Definition, along with a partial set of the observed values and metrics that indicate why the Expectation succeeded or failed.

   For more information on configuring a Result Format, see [Choose a Result Format](/core/trigger_actions_based_on_results/choose_a_result_format/choose_a_result_format.md).

5. Create the Checkpoint.

   The Checkpoint class is available from the `great_expectations` module. You instantiate a Checkpoint by providing the lists of Validation Definitions and Actions that you previously created, as well as a unique name for the Checkpoint, to the Checkpoint class. The Checkpoint's Result Format can optionally be set, as well:

   ```python title="Python" name="docs/docusaurus/docs/core/trigger_actions_based_on_results/_examples/create_a_checkpoint_with_actions.py - create a Checkpoint"
   ```

6. Add the Checkpoint to your Data Context.

   Once you create a Checkpoint you should save it to your Data Context for future use: 

   ```python title="Python" name="docs/docusaurus/docs/core/trigger_actions_based_on_results/_examples/create_a_checkpoint_with_actions.py - save the Checkpoint to the Data Context"
   ```
   
   When you add your Checkpoint to your Data Context you will be able to retrieve it elsewhere in your code by replacing the value for `checkpoint_name` and executing:

   ```python title="Python" name="docs/docusaurus/docs/core/trigger_actions_based_on_results/_examples/create_a_checkpoint_with_actions.py - retrieve a Checkpoint from the Data Context"
   ```

   With a File Data Context or a GX Cloud Data Context you will also be able to retrieve your Checkpoint in future Python sessions.

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python title="Python" name="docs/docusaurus/docs/core/trigger_actions_based_on_results/_examples/create_a_checkpoint_with_actions.py - full code example" 
```

</TabItem>

</Tabs>
