---
title: Create a new Checkpoint
---

import TechnicalTag from '@site/docs/term_tags/_tag.mdx';
import Preface from './components_how_to_create_a_new_checkpoint/_preface.mdx'
import StepsForCheckpoints from './components_how_to_create_a_new_checkpoint/_steps_for_checkpoints_.mdx'
import AdditionalResources from './components_how_to_create_a_new_checkpoint/_additional_resources.mdx'

<Preface />

<StepsForCheckpoints />

## Create a Checkpoint

The following code examples use the SimpleCheckpoint class. To modify the following code for your use case, replace `batch_request` and `expectation_suite_name` with your own paremeters.

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_create_a_new_checkpoint.py create checkpoint batch_request"
```

There are other configuration options for more advanced deployments. See [How to configure a new Checkpoint using test_yaml_config](../../../guides/validation/checkpoints/how_to_configure_a_new_checkpoint_using_test_yaml_config.md).


## Run your Checkpoint (Optional)

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_create_a_new_checkpoint.py run checkpoint batch_request"
```

The returned `checkpoint_result` contains information about the checkpoint run.

## Build Data Docs (Optional)

Run the following Python code to build <TechnicalTag tag="data_docs" text="Data Docs" /> with the latest checkpoint run results:

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_create_a_new_checkpoint.py build data docs"
```

## Store your Checkpoint (Optional)

Run the following Python code to store your Checkpoint for later use:

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_create_a_new_checkpoint.py add checkpoint"
```

Run the following Python code to retrieve the Checkpoint:

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_create_a_new_checkpoint.py get checkpoint"
```

## Related documentation
<AdditionalResources />
