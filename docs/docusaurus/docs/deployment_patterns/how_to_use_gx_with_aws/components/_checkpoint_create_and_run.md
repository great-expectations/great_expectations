import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

Here we will create and store a <TechnicalTag tag="checkpoint" text="Checkpoint"/> for our Batch, which we can use to validate and run post-validation <TechnicalTag tag="action" text="Actions" />.

Checkpoints are a robust resource that can be preconfigured with a Batch Request and Expectation Suite or take them in as parameters at runtime.  They can also execute numerous Actions based on the Validation Results that are returned when the Checkpoint is run.

This guide will demonstrate using a `SimpleCheckpoint` that takes in a Batch Request and Expectation Suite as parameters for the `context.run_checkpoint(...)` command.

:::tip 

To pre-configure a Checkpoint with a Batch Request and Expectation Suite, see [Manage Checkpoints](../../../../docs/guides/validation/checkpoints/checkpoint_lp.md)

:::