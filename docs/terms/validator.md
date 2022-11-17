---
title: Validator
---
import UniversalMap from '/docs/images/universal_map/_universal_map.mdx';
import TechnicalTag from '../term_tags/_tag.mdx';
import ConnectHeader from '/docs/images/universal_map/_um_connect_header.mdx';
import CreateHeader from '/docs/images/universal_map/_um_create_header.mdx';
import ValidateHeader from '/docs/images/universal_map/_um_validate_header.mdx';


<UniversalMap setup='inactive' connect='active' create='active' validate='active'/> 

## Overview

### Definition

A Validator is the object responsible for running an <TechnicalTag relative="../" tag="expectation_suite" text="Expectation Suite" /> against data.

### Features and promises

The Validator is the core functional component of Great Expectations. 

### Relationship to other objects

Validators are responsible for running an Expectation Suite against a <TechnicalTag relative="../" tag="batch_request" text="Batch Request" />.  <TechnicalTag relative="../" tag="checkpoint" text="Checkpoints" />, in particular, use them for this purpose.  However, you can also use your <TechnicalTag relative="../" tag="data_context" text="Data Context" /> to get a Validator to use outside a Checkpoint. 

## Use cases

<ConnectHeader/>

When connecting to Data, it is often useful to verify that you have configured your <TechnicalTag relative="../" tag="datasource" text="Datasource" /> correctly.  To verify a new Datasource, you can load data from it into a Validator using a Batch Request.  There are examples of this workflow at the end of most of [our guides on how to connect to specific source data systems](../guides/connecting_to_your_data/index.md#database).


<CreateHeader/>

When creating Expectations for an Expectation Suite, most workflows will have you use a Validator.  You can see this in [our guide on how to create and edit Expectations with a Profiler](../guides/expectations/how_to_create_and_edit_expectations_with_a_profiler.md), and in the Jupyter Notebook opened if you follow [our guide on how to create and edit Expectations with instant feedback from a sample Batch of data](../guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data.md).

<ValidateHeader/>

Checkpoints utilize a Validator when running an Expectation Suite against a Batch Request.  This process is entirely handled for you by the Checkpoint; you will not need to create or configure the Validator in question. 

## Features

### Out of the box functionality

Validators don't require additional configuration.  Provide one with an Expectation Suite and a Batch Request, and it will work out of the box.

## API basics

### How to access

Validators are not typically saved.  Instead, they are instantiated when needed.  If you need a Validator outside a Checkpoint (for example, to create Expectations interactively in a Jupyter Notebook) you will use one that is created for that purpose.

### How to create

You can create a Validator through the `get_validator(...)` command of a Data Context.  For an example of this, you can reference the ["Instantiate your Validator"](../guides/expectations/how_to_create_and_edit_expectations_with_a_profiler.md#3-instantiate-your-validator) section of [our guide on how to create and edit Expectations with a Profiler](../guides/expectations/how_to_create_and_edit_expectations_with_a_profiler.md) 

### Configuration

Creating a Validator with the `get_validator(...)` method will require you to provide an Expectation Suite and a Batch Request.  Other than these parameters, there is no configuration needed for Validators.
