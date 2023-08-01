import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

As the examples use a `SimpleCheckpoint`, the Checkpoint already contains an `UpdateDataDocsAction` which renders the <TechnicalTag tag="data_docs" text="Data Docs"/> from the generated Validation Results. The Data Docs store contains a new entry for the rendered Validation Result.

:::tip 

For more information on Actions that Checkpoints can perform and how to add them, see [Configure Actions](../../../../docs/guides/validation/validation_actions/actions_lp.md).

:::

Run the following code to view the new entry for the rendered Validation Result:

```python
context.open_data_docs()
```