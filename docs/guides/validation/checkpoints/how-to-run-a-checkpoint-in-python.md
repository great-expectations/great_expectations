---
title: How to run a Checkpoint in Python
---
import Prerequisites from '../../../guides/connecting_to_your_data/components/prerequisites.jsx'

This guide will help you run a Checkpoint in Python.
This is useful if your pipeline environment or orchestration engine does not have shell access.

<Prerequisites>

- Created at least one [Checkpoint](./how-to-create-a-new-checkpoint)

</Prerequisites>

Steps
-----

1. First, generate the Python script with the command:

```bash
great_expectations --v3-api checkpoint script my_checkpoint
```

2. Next, you will see a message about where the Python script was created like:

```bash
A Python script was created that runs the checkpoint named: `my_checkpoint`
  - The script is located in `great_expectations/uncommitted/run_my_checkpoint.py`
  - The script can be run with `python great_expectations/uncommitted/run_my_checkpoint.py`
```

3. Next, open the script which should look like this:

```python
"""
This is a basic generated Great Expectations script that runs a Checkpoint.

Checkpoints are the primary method for validating batches of data in production and triggering any followup actions.

A Checkpoint facilitates running a validation as well as configurable Actions such as updating Data Docs, sending a
notification to team members about validation results, or storing a result in a shared cloud storage.

See also [How to configure a new Checkpoint using test_yaml_config](./how-to-configure-a-new-checkpoint-using-test_yaml_config) for more information about the Checkpoints and how to configure them in your Great Expectations environment.

Checkpoints can be run directly without this script using the `great_expectations checkpoint run` command.  This script
is provided for those who wish to run Checkpoints in Python.

Usage:
- Run this file: `python great_expectations/uncommitted/run_chk.py`.
- This can be run manually or via a scheduler such, as cron.
- If your pipeline runner supports Python snippets, then you can paste this into your pipeline.
"""
import sys

from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.data_context import DataContext

data_context: DataContext = DataContext(
    context_root_dir="/Users/talgluck/Documents/ge_main/quagga/UAT/DataContexts/cli_testing/ge_suite/v3_many_suites_pandas_filesystem_v3_config/great_expectations"
)

result: CheckpointResult = data_context.run_checkpoint(
    checkpoint_name="chk",
    batch_request=None,
    run_name=None,
)

if not result["success"]:
    print("Validation failed!")
    sys.exit(1)

print("Validation succeeded!")
sys.exit(0)
```

4. This Python script can then be invoked directly using Python `python great_expectations/uncommitted/run_my_checkpoint.py`
or the Python code can be embedded in your pipeline.

  Other arguments to the `DataContext.run_checkpoint()` method may be required, depending on the amount and specifics of the Checkpoint configuration previously saved in the configuration file of the Checkpoint with the corresponding `name`.  The dynamically specified Checkpoint configuration, provided to the runtime as arguments to `DataContext.run_checkpoint()` must complement the settings in the Checkpoint configuration file so as to comprise a properly and sufficiently configured Checkpoint with the given `name`.

Please see [How to configure a new Checkpoint using test_yaml_config](./how-to-configure-a-new-checkpoint-using-test_yaml_config) for additional Checkpoint configuration and `DataContext.run_checkpoint()` examples.
