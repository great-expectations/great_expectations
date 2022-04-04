---
title: How to Validate data without a Checkpoint
---
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

:::caution ATTENTION
As part of the new modular expectations API in Great Expectations, Validation Operators have evolved into Class-Based <TechnicalTag tag="checkpoint" text="Checkpoints" />. This means running a Validation without a Checkpoint is no longer supported in Great Expectations version 0.13.8 or later. Please read [Checkpoints and Actions](../../../reference/checkpoints_and_actions.md) to learn more.
:::

This guide originally demonstrated how to load an <TechnicalTag tag="expectation_suite" text="Expectation Suite" /> and <TechnicalTag tag="validate" text="Validate" /> data without using a <TechnicalTag tag="checkpoint" text="Checkpoint" />. As that process might have been suitable for environments or workflows where a user does not want to or cannot create a Checkpoint, e.g. in a [hosted environment](../../../deployment_patterns/how_to_instantiate_a_data_context_hosted_environments.md).  However, this workflow is no longer supported as of Great Expectations version 0.13.8 or later.

Instead, the recommended workflow is to use an in-memory instance of a Checkpoint loaded from a YAML configuration or Python dictionary that is also defined in-memory.  Documentation for this process is forthcoming.

