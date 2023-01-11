import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

Since we used a `SimpleCheckpoint`, our Checkpoint already contained an `UpdateDataDocsAction` which rendered our <TechnicalTag tag="data_docs" text="Data Docs"/> from the Validation Results we just generated. That means our Data Docs store will contain a new entry for the rendered Validation Result.

:::tip 

For more information on Actions that Checkpoints can perform and how to add them, please see [our guides on Actions](../../../../docs/guides/validation/index.md#actions).

:::

Viewing this new entry is as simple as running:

```python
context.open_data_docs()
```