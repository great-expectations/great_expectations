---
title: Create a new Checkpoint
---

import TechnicalTag from '@site/docs/term_tags/_tag.mdx';
import Preface from './components_how_to_create_a_new_checkpoint/_preface.mdx'
import StepsForCheckpoints from './components_how_to_create_a_new_checkpoint/_steps_for_checkpoints_.mdx'
import AdditionalResources from './components_how_to_create_a_new_checkpoint/_additional_resources.mdx'

<Preface />

<StepsForCheckpoints />

## Steps

### 1. Create a Checkpoint

In this guide, we will use the SimpleCheckpoint class, which takes care of some defaults.

To modify this code sample for your use case, replace the `batch_request` and `expectation_suite_name` with your own.

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_create_a_new_checkpoint.py create checkpoint batch_request"
```

Note: There are other configuration options for more advanced deployments, please refer to [How to configure a new Checkpoint using test_yaml_config](../../../guides/validation/checkpoints/how_to_configure_a_new_checkpoint_using_test_yaml_config.md) for more details.


### 2. (Optional) Run your Checkpoint

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_create_a_new_checkpoint.py run checkpoint batch_request"
```

The returned `checkpoint_result` contains information about the checkpoint run.

### 3. (Optional) Build Data Docs

You can build <TechnicalTag tag="data_docs" text="Data Docs" /> with the latest checkpoint run result included by running:

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_create_a_new_checkpoint.py build data docs"
```

### 4. (Optional) Store your Checkpoint

If you want to store your Checkpoint for later use:

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_create_a_new_checkpoint.py add checkpoint"
```

And retrieve via the name we set earlier:

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_create_a_new_checkpoint.py get checkpoint"
```

## Additional Resources
<AdditionalResources />
