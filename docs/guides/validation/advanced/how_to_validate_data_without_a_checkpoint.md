---
title: How to validate data without a Checkpoint
---
import Prerequisites from '../../../guides/connecting_to_your_data/components/prerequisites.jsx';

This guide demonstrates how to load an Expectation Suite and validate data without using a [Checkpoint](../checkpoints/how_to_create_a_new_checkpoint.md). This might be suitable for environments or workflows where a user does not want to or cannot create a Checkpoint, e.g. in a [hosted environment](../../../deployment_patterns/how_to_instantiate_a_data_context_hosted_environments.md).

:::caution ATTENTION
As part of the new modular expectations API in Great Expectations, Validation Operators have evolved into Class-Based Checkpoints. This means running a Validation without a Checkpoint is no longer supported in Great Expectations version 0.13.8 or later. Please read [Checkpoints and Actions](../../../reference/checkpoints_and_actions.md) to learn more.
