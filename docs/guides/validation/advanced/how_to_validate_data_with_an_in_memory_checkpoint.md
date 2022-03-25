---
title: How to Validate data with an in-memory Checkpoint
---
import Prerequisites from '../../../guides/connecting_to_your_data/components/prerequisites.jsx';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

This guide will demonstrate how to Validate data using a Checkpoint that is configured and run entirely in-memory.  This workflow is appropriate for environments or workflows where a user does not want to or cannot use a Checkpoint Store, e.g. in a [hosted environment](../../../deployment_patterns/how_to_instantiate_a_data_context_hosted_environments.md). This process is very similar to 


<Prerequisites>

- Have a Data Context
- Have an Expectation Suite
- Have a Datasource
- Have a basic understanding of Checkpoints

</Prerequisites>

:::note
Reading our guide on [Deploying Great Expectations in a hosted environment without file system or CLI](../../../deployment_patterns/how_to_instantiate_a_data_context_hosted_environments.md) is recommended for guidance on the setup, connecting to data, and creating expectations steps that take place prior to this process.
:::

## Steps

### 1. Scaffold your script

The recommended method for creating a Checkpoint is to use the CLI to open a Jupyter Notebook which contains code scaffolding to assist you with the process.  Since that option is not available (this guide is assuming that your need for an in-memory Checkpoint is due to being unable to use the CLI or access a filesystem) you will have to provide that scaffolding yourself.

In the script that you are defining and executing your Checkpoint in, enter the following code:

```python

```

### 2. Edit your checkpoint

The Checkpoint configuration in the above scaffolding uses placeholders for the <TechnicalTag tag="batch" text="Batch" /> of data and <TechnicalTag tag="expectation_suite" text="Expectation Suite" /> that the Checkpoint will use.  You will need to replace these placeholders with your own Batch and Expectation Suite.  You can further edit the configuration to add additional entries under the `validations` key.  Alternatively, you can even replace this configuration entirely and build one from scratch.  If you choose to build a configuration from scratch, you may wish to reference [our documentation on Checkpoint configurations](../../../terms/checkpoint.md#checkpoint-configuration) as you do. 

### 3.

### 4.