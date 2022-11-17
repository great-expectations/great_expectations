---
title: How to validate data by running a Checkpoint
---

import Prerequisites from '../../guides/connecting_to_your_data/components/prerequisites.jsx';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

This guide will help you <TechnicalTag tag="validation" text="Validate" /> your data by running a <TechnicalTag tag="checkpoint" text="Checkpoint" />.

As stated in the Getting Started Tutorial [Step 4: Validate data](../../tutorials/getting_started/tutorial_validate_data.md), the best way to Validate data in production with Great Expectations is using a <TechnicalTag tag="checkpoint" text="Checkpoint" />. The advantage of using a Checkpoint is ease of use, due to its principal capability of combining the existing configuration in order to set up and perform the Validation:
- <TechnicalTag tag="expectation_suite" text="Expectation Suites" />
- <TechnicalTag tag="data_connector" text="Data Connectors" />
- <TechnicalTag tag="batch_request" text="Batch Requests" />
- <TechnicalTag tag="action" text="Actions" />
 
Otherwise, configuring these validation parameters would have to be done via the API.  A Checkpoint encapsulates this "boilerplate" and ensures that all components work in harmony together.  Finally, running a configured Checkpoint is a one-liner, as described below.

<Prerequisites>

- [Configured a Data Context](../../tutorials/getting_started/tutorial_setup.md#create-a-data-context).
- [Configured an Expectations Suite](../../tutorials/getting_started/tutorial_create_expectations.md).
- [Configured a Checkpoint](./checkpoints/how_to_create_a_new_checkpoint.md)

</Prerequisites>

You can run the Checkpoint from the <TechnicalTag tag="cli" text="CLI" /> in a Terminal shell or using Python.

<Tabs
  groupId="terminal-or-python"
  defaultValue='terminal'
  values={[
  {label: 'Terminal', value:'terminal'},
  {label: 'Python', value:'python'},
  ]}>

<TabItem value="terminal">

## Steps

### 1. Run your Checkpoint

Checkpoints can be run like applications from the command line by running:

```bash
great_expectations checkpoint run my_checkpoint
Validation failed!
```

### 2. Observe the output

The output of your validation will tell you if all validations passed or if any failed.

## Additional notes

This command will return posix status codes and print messages as follows:

    +-------------------------------+-----------------+-----------------------+
    | **Situation**                 | **Return code** | **Message**           |
    +-------------------------------+-----------------+-----------------------+
    | all validations passed        | 0               | Validation succeeded! |
    +-------------------------------+-----------------+-----------------------+
    | one or more validation failed | 1               | Validation failed!    |
    +-------------------------------+-----------------+-----------------------+


</TabItem>
<TabItem value="python">

## Steps


### 1. Generate the Python script

From your console, run the CLI command:

```bash
great_expectations checkpoint script my_checkpoint
```

After the command runs, you will see a message about where the Python script was created similar to the one below:

```bash
A Python script was created that runs the checkpoint named: `my_checkpoint`
  - The script is located in `great_expectations/uncommitted/run_my_checkpoint.py`
  - The script can be run with `python great_expectations/uncommitted/run_my_checkpoint.py`
```

### 2. Open the script

The script that was produced should look like this:

```python
"""
This is a basic generated Great Expectations script that runs a Checkpoint.

Checkpoints are the primary method for validating batches of data in production and triggering any followup actions.

A Checkpoint facilitates running a validation as well as configurable Actions such as updating Data Docs, sending a
notification to team members about Validation Results, or storing a result in a shared cloud storage.

Checkpoints can be run directly without this script using the `great_expectations checkpoint run` command.  This script
is provided for those who wish to run Checkpoints in Python.

Usage:
- Run this file: `python great_expectations/uncommitted/run_my_checkpoint.py`.
- This can be run manually or via a scheduler such, as cron.
- If your pipeline runner supports Python snippets, then you can paste this into your pipeline.
"""
import sys

from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.data_context import DataContext

data_context: DataContext = DataContext(
    context_root_dir="/path/to/great_expectations"
)

result: CheckpointResult = data_context.run_checkpoint(
    checkpoint_name="my_checkpoint",
    batch_request=None,
    run_name=None,
)

if not result["success"]:
    print("Validation failed!")
    sys.exit(1)

print("Validation succeeded!")
sys.exit(0)
```

### 3. Run the script

This Python script can then be invoked directly using Python:

```python
python great_expectations/uncommitted/run_my_checkpoint.py
```

Alternatively, the above Python code can be embedded in your pipeline.

## Additional Notes

- Other arguments to the `DataContext.run_checkpoint()` method may be required, depending on the amount and specifics of the Checkpoint configuration previously saved in the configuration file of the Checkpoint with the corresponding `name`.
- The dynamically specified Checkpoint configuration, provided to the runtime as arguments to `DataContext.run_checkpoint()` must complement the settings in the Checkpoint configuration file so as to comprise a properly and sufficiently configured Checkpoint with the given `name`.
- Please see [How to configure a new Checkpoint using test_yaml_config](./checkpoints/how_to_configure_a_new_checkpoint_using_test_yaml_config.md) for more Checkpoint configuration examples (including the convenient templating mechanism) and `DataContext.run_checkpoint()` invocation options.

</TabItem>
</Tabs>

