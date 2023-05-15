---
title: How to pass an in-memory DataFrame to a Checkpoint
---

import Prerequisites from '../../../guides/connecting_to_your_data/components/prerequisites.jsx';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

This guide will help you pass an in-memory DataFrame to an existing <TechnicalTag tag="checkpoint" text="Checkpoint" />. This is especially useful if you already have your data in memory due to an existing process such as a pipeline runner.

<Prerequisites>

- [Configured a Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_quickly_instantiate_a_data_context).

</Prerequisites>

## Steps

### 1. Set up Great Expectations
#### Import the required libraries and load your DataContext

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py setup"
```

### 2. Read a DataFrame and create a Checkpoint

The following example uses the `read_*` method on the PandasDatasource to directly return a <TechnicalTag tag="validator" text="Validator" />. To use Validators to interactively build an Expectation Suite, see [How to create Expectations interactively in Python](/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data).
The Validator can be passed directly to a SimpleCheckpoint

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py read_dataframe"
```

Alternatively, you can use `add_*` methods to add the asset and then retrieve a <TechnicalTag tag="batch_request" text="Batch Request" />. This method is consistent with how other Data Assets work, and can integrate in-memory data with other Batch Request workflows and configurations.

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py add_dataframe"
```

In both examples, `batch_metadata` is an optional parameter that can associate meta-data with the batch or DataFrame. When you work with DataFrames, this can help you distinguish Validation results.

## Additional Notes
To view the full script used in this page, see it on GitHub:
- [how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py](https://github.com/great-expectations/great_expectations/tree/develop/tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py)
