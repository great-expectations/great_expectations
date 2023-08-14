---
title: Validator
---

import TechnicalTag from '../term_tags/_tag.mdx';

A Validator is the object responsible for running an <TechnicalTag relative="../" tag="expectation_suite" text="Expectation Suite" /> against data.

The Validator is the core functional component of Great Expectations. Validators don't require additional configuration.  Provide one with an Expectation Suite and a Batch Request, and it will function.

## Relationship to other objects

Validators are responsible for running an Expectation Suite against a <TechnicalTag relative="../" tag="batch_request" text="Batch Request" />.  <TechnicalTag relative="../" tag="checkpoint" text="Checkpoints" />, in particular, use them for this purpose.  However, you can also use your <TechnicalTag relative="../" tag="data_context" text="Data Context" /> to get a Validator to use outside a Checkpoint. 

## Use cases

When connecting to Data, it is often useful to verify that you have configured your <TechnicalTag relative="../" tag="datasource" text="Data Source" /> correctly.  To verify a new Data Source, you can load data from it into a Validator using a Batch Request.  Fore examples of this workflow, see [Connect to source data](../guides/connecting_to_your_data/connect_to_data_lp.md).

When creating Expectations for an Expectation Suite, most workflows will have you use a Validator.  You can see this in our guide on how to create and edit Expectations with a DataAssistant or a Custom Profiler.

Checkpoints utilize a Validator when running an Expectation Suite against a Batch Request.  This process is entirely handled for you by the Checkpoint; you will not need to create or configure the Validator in question. 

## Access

Validators are not typically saved.  Instead, they are instantiated when needed.  If you need a Validator outside a Checkpoint (for example, to create Expectations interactively in a Jupyter Notebook) you will use one that is created for that purpose.

## Create

You can create a Validator through the `get_validator(...)` command of a Data Context.

## Configure

Creating a Validator with the `get_validator(...)` method will require you to provide an Expectation Suite and a Batch Request.  Other than these parameters, there is no configuration needed for Validators.
