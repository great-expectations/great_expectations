---
title: "Batch Request"
---
import UniversalMap from '/docs/images/universal_map/_universal_map.mdx';
import TechnicalTag from '../term_tags/_tag.mdx';
import BatchesAndBatchRequests from './_batches_and_batch_requests.mdx';
import ConnectHeader from '/docs/images/universal_map/_um_connect_header.mdx';
import CreateHeader from '/docs/images/universal_map/_um_create_header.mdx';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';
import ValidateHeader from '/docs/images/universal_map/_um_validate_header.mdx';


<UniversalMap setup='inactive' connect='active' create='active' validate='active'/> 

## Overview

### Definition
A Batch Request specifies a <TechnicalTag tag="batch" text="Batch" /> of data.
It can be created by using the `build_batch_request` method found on a <TechnicalTag tag="data_asset" text="Data Asset" />.

### Features and promises

A Batch Request contains all the necessary details to query the appropriate underlying data.  
The relationship between a Batch Request and the data returned as a Batch is guaranteed.  
If a Batch Request identifies multiple Batches that fit the criteria of the user provided `options` 
argument to the `build_batch_request` method on a Data Asset, the Batch Request will return all of the matching Batches.

If you are using an interactive session, you can inspect the allowed keys for the `options` argument for a Data Asset
by printing the `batch_request_options` attribute.

### Relationship to other objects

A Batch Request is always used when Great Expectations builds a Batch.   
Any time you interact with something that requires a Batch of Data 
(such as a <TechnicalTag relative="../" tag="profiler" text="Profiler" />, 
<TechnicalTag relative="../" tag="checkpoint" text="Checkpoint" />, or 
<TechnicalTag relative="../" tag="validator" text="Validator" />) 
you will use a Batch Request to create the Batch that is used.

## Use cases

<ConnectHeader/>

Since a Batch Request is necessary in order to get a Batch of data to operator on, 
all of our guides on how to connect to specific source data systems discussing building a batch request.
The interface is always calling `build_batch_request` on your Data Asset with the appropriate `options` paramater.

You can find these guides in our documentation on [how to connect to data](../guides/connecting_to_your_data/index.md).

<CreateHeader/>

If you are using a Profiler or the interactive method of creating Expectations, you will need to provide a Batch of data for the Profiler to analyze or your manually defined Expectations to test against.  For both of these processes, you will therefore need a Batch Request to get the Batch.

For more information, see:

- [Our how-to guide on the interactive process for creating Expectations](../guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data.md)
- [Our how-to guide on using a Profiler to generate Expectations](../guides/expectations/how_to_create_and_edit_expectations_with_a_profiler.md)

<ValidateHeader/>

When <TechnicalTag relative="../" tag="validation" text="Validating" /> data with a Checkpoint, you will need to provide one or more Batch Requests and one or more <TechnicalTag relative="../" tag="expectation_suite" text="Expectation Suites" />.  You can do this at runtime, or by defining Batch Request and Expectation Suite pairs in advance, in the Checkpoint's configuration.

For more information on setting up Batch Request/Expectation Suite pairs in a Checkpoint's configuration, see:

- [Our guide on how to add data or suites to a Checkpoint](../guides/validation/checkpoints/how_to_add_validations_data_or_suites_to_a_checkpoint.md)

## Features

### Guaranteed relationships

The relationship between a Batch and the Batch Request that generated it is guaranteed.  A Batch Request includes all of the information necessary to identify a specific Batch or Batches.

Batches are always built using a Batch Request.  When the Batch is built metadata is attached to the Batch object and is available via the Batch `metadata` attribute. 
This metadata contains all the option values necessary to recreate the Batch Request that corresponds to the Batch. 

## API basics

### How to access

You will rarely need to access an existing Batch Request.  Instead, you will often build a Batch Request from a Data Asset. 
A Batch Request can also be saved to a configuration file when you save an object that required a Batch Request for setup, such as a Checkpoint. 
Once you receive a Batch back, it is unlikely you will need to reference to the Batch Request that generated it.  
Indeed, if the Batch Request was part of a configuration, Great Expectations will simply initialize a new copy rather than load an existing one when the Batch Request is needed. 

### How to create

You can create a Batch Request from a Data Asset by calling `build_batch_request`. 
Here is an example of configuring a Pandas Filesystem Asset and creating a Batch Request:

 ```python name="tests/integration/docusaurus/reference/glossary/batch_request batch_request"
```


The `options` one passes in to specify a batch will vary depending on how the specific Data Asset was configured.
To look at the keys for the options dictionary, you can do the following:

```python name="tests/integration/docusaurus/reference/glossary/batch_request options"
```




