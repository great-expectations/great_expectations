---
title: How to Validate data without a Checkpoint
---
import Prerequisites from '../../../guides/connecting_to_your_data/components/prerequisites.jsx';

:::caution ATTENTION
As part of the new modular expectations API in Great Expectations, Validation Operators have evolved into Class-Based Checkpoints. This means running a Validation without a Checkpoint is no longer supported in Great Expectations version 0.13.8 or later.   For more context, please read our [documentation on Checkpoints](../../../terms/checkpoint.md) and our [documentation on Actions](../../../terms/action.md).

This guide originally demonstrated how to load an Expectation Suite and Validate data without using a Checkpoint. That used to be suitable for environments or workflows where a user does not want to or cannot create a Checkpoint, e.g. in a [hosted environment](../../../deployment_patterns/how_to_instantiate_a_data_context_hosted_environments.md). However, this workflow is no longer supported. 

As an alternative, you can instead run Validations by using a Checkpoint that is configured and initialized entierly in-memory, as demonstrated in our guide on [How to validate data with an in-memory Checkpoint](./how_to_validate_data_with_an_in_memory_checkpoint.md).
:::