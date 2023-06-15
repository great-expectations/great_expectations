---
title: Run a Checkpoint to validate data 
---

import Prerequisites from '../../guides/connecting_to_your_data/components/prerequisites.jsx';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

This guide will help you <TechnicalTag tag="validation" text="Validate" /> your data by running a <TechnicalTag tag="checkpoint" text="Checkpoint" />.

The best way to Validate data with Great Expectations is using a Checkpoint. Checkpoints identify what <TechnicalTag tag="expectation_suite" text="Expectation Suites" /> to run against which <TechnicalTag tag="data_asset" text="Data Asset" /> and <TechnicalTag tag="batch" text="Batch" /> (described by a <TechnicalTag tag="batch_request" text="Batch Requests" />), and what <TechnicalTag tag="action" text="Actions" /> to take based on the results of those tests.

Succinctly: Checkpoints are used to test your data and take action based on the results.

## Prerequisites

<Prerequisites>

- [Configured a Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_quickly_instantiate_a_data_context)
- [Configured an Expectations Suite](/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data)
- [Configured a Checkpoint](./checkpoints/how_to_create_a_new_checkpoint.md)

</Prerequisites>

You can run the Checkpoint from the <TechnicalTag tag="cli" text="CLI" /> in a Terminal shell or using Python.

<Tabs
  groupId="terminal-or-python"
  defaultValue='python'
  values={[
  {label: 'Python', value:'python'},
  {label: 'Terminal', value:'terminal'},
  ]}>

<TabItem value="python">

If you already have created and saved a Checkpoint, then the following code snippet will retrieve it from your context and run it:

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_validate_data_by_running_a_checkpoint.py checkpoint script"
```

If you do not have a Checkpoint, the pre-requisite guides mentioned above will take you through the necessary steps. Alternatively, this concise example below shows how to connect to data, create an expectation suite using a validator, and create a checkpoint (saving everything to the <TechnicalTag tag="data_context" text="Data Context" /> along the way).

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_validate_data_by_running_a_checkpoint.py setup"
```

</TabItem>
<TabItem value="terminal">

If you have already created and saved a Checkpoint, then you can run the Checkpoint using the CLI.

```bash
great_expectations checkpoint run my_checkpoint
```

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
</Tabs>

