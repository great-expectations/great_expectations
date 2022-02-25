---
title: "Batch Request"
---
import UniversalMap from '/docs/images/universal_map/_universal_map.mdx';
import TechnicalTag from '../term_tags/_tag.mdx';
import BatchesAndBatchRequests from './_batches_and_batch_requests.mdx';
import ConnectHeader from '/docs/images/universal_map/_um_connect_header.mdx';
import CreateHeader from '/docs/images/universal_map/_um_create_header.mdx';
import ValidateHeader from '/docs/images/universal_map/_um_validate_header.mdx';


<UniversalMap setup='inactive' connect='active' create='active' validate='active'/> 

## Overview

### Definition

A Batch Request is provided to a <TechnicalTag relative="../" tag="datasource" text="Datasource" /> in order to create a <TechnicalTag relative="../" tag="batch" text="Batch" />.

### Features and promises

A Batch Request contains all the necessary details to query the appropriate underlying data.  The relationship between a Batch Request and the data returned as a Batch is guaranteed.  If a Batch Request identifies multiple Batches that fit the criteria of the user provided `batch_identifiers`, the Batch Request will return all of the matching Batches.

### Relationship to other objects

A Batch Request is always used when Great Expectations builds a Batch.  The Batch Request includes a "query" for a Datasource's <TechnicalTag relative="../" tag="data_connector" text="Data Connector" /> to describe the data to include in the Batch.  Any time you interact with something that requires a Batch of Data (such as a <TechnicalTag relative="../" tag="profiler" text="Profiler" />, <TechnicalTag relative="../" tag="checkpoint" text="Checkpoint" />, or <TechnicalTag relative="../" tag="validator" text="Validator" />) you will use a Batch Request and Datasource to create the Batch that is used.

## Use cases

<ConnectHeader/>

Since a Batch Request is necessary in order to get a Batch from a Datasource, all of our guides on how to connect to specific source data systems include a section on using a Batch Request to test that your Datasource is properly configured.  These sections also serve as examples on how to define a Batch Request for a Datasource that is configured for a given source data system.

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
- [Our guide on how to configure a new Checkpoint using `test_yaml_config(...)`](../guides/validation/checkpoints/how_to_configure_a_new_checkpoint_using_test_yaml_config.md)

When passing `RuntimeBatchRequest`s to a Checkpoint, you will not be pairing Expectation Suites with Batch Requests.  Instead, when you provide `RuntimeBatchRequest`s to a Checkpoint, it will run all of its configured Expectation Suites against each of the `RuntimeBatchRequest`s that are passed in.

For examples of how to pass `RuntimeBatchRequest`s to a Checkpoint, see the examples used to test your Datasource configurations in [our documentation on how to connect to data](../guides/connecting_to_your_data/index.md).  `RuntimeBatchRequest`s are typically used when you need to pass in a DataFrame at runtime.

For a good example if you don't have a specific source data system in mind right now, check out [Example 2 of our guide on how to pass an in memory dataframe to a Checkpoint](../guides/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.md#example-2-pass-a-complete-runtimebatchrequest-at-runtime).

## Features

### Guaranteed relationships

The relationship between a Batch and the Batch Request that generated it is guaranteed.  A Batch Request includes all of the information necessary to identify a specific Batch or Batches.

Batches are always built using a Batch Request.  When the Batch is built, additional metadata is included, one of which is a Batch Definition.  The Batch Definition directly corresponds to the Batch Request that was used to create the Batch.

## API basics

### How to access

You will rarely need to access an existing Batch Request.  Instead, you will often find yourself defining a Batch Request in a configuration file, or passing in parameters to create a Batch Request which you will then pass to a Datasource.  Once you receive a Batch back, it is unlikely you will need to reference to the Batch Request that generated it.  Indeed, if the Batch Request was part of a configuration, Great Expectations will simply initialize a new copy rather than load an existing one when the Batch Request is needed. 

### How to create

Batch Requests are instances of either a `RuntimeBatchRequest` or a `BatchRequest`

A `BatchRequest` can be defined by passing a dictionary with the necessary parameters when a `BatchRequest` is initialized, like so:

```python title="Python code
from great_expectations.core.batch import BatchRequest

batch_request_paramaters = {
  'datasource_name': 'getting_started_datasource',
  'data_connector_name': 'default_inferred_data_connector_name',
  'data_asset_name': 'yellow_tripdata_sample_2019-01.csv',
  'limit': 1000
}

batch_request=BatchRequest(**batch_request_parameters)
```

Regardless of the source data system that the Datasource being referenced by a Batch Request is associated with, the parameters for initializing a Batch Request will remain the same.  Great Expectations will handle translating that information into a query appropriate for the source data system behind the scenes.


A `RuntimeBatchRequest` will need a Datasource that has been configured with a `RuntimeDataConnector`.  You will then use a `RuntimeBatchRequest` to specify the Batch that you will be working with.

For more information and examples regarding setting up a Datasource for use with `RuntimeBatchRequest`s, see:

- [Our guide on how to configure a `RuntimeDataConnector`](../guides/connecting_to_your_data/how_to_configure_a_runtimedataconnector.md)


## More Details

<BatchesAndBatchRequests/>

### RuntimeDataConnector and RuntimeBatchRequest

A Runtime Data Connector is a special kind of Data Connector that supports easy integration with Pipeline Runners where
the data is already available as a reference that needs only a lightweight wrapper to track validations. Runtime Data
Connectors are used alongside a special kind of Batch Request class called a `RuntimeBatchRequest`. Instead of serving
as a description of what data Great Expectations should fetch, a Runtime Batch Request serves as a wrapper for data that
is passed in at runtime (as an in-memory dataframe, file/S3 path, or SQL query), with user-provided identifiers for
uniquely identifying the data.

In a Batch Definition produced by a Runtime Data Connector, the `batch_identifiers` come directly from the Runtime Batch
Request and serve as a persistent, unique identifier for the data included in the Batch. By relying on
user-provided `batch_identifiers`, we allow the definition of the specific batch's identifiers to happen at runtime, for
example using a run_id from an Airflow DAG run. The specific runtime batch_identifiers to be expected are controlled in
the Runtime Data Connector configuration. Using that configuration creates a control plane for governance-minded
engineers who want to enforce some level of consistency between validations.

