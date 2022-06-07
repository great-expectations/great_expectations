---
id: execution_engine
title: Execution Engine
hoverText: A system capable of processing data to compute Metrics
---
import UniversalMap from '/docs/images/universal_map/_universal_map.mdx';
import TechnicalTag from '../term_tags/_tag.mdx';
import ConnectHeader from '/docs/images/universal_map/_um_connect_header.mdx';
import CreateHeader from '/docs/images/universal_map/_um_create_header.mdx';
import ValidateHeader from '/docs/images/universal_map/_um_validate_header.mdx';


<UniversalMap setup='inactive' connect='active' create='active' validate='active'/> 

## Overview

### Definition

An Execution Engine is a system capable of processing data to compute <TechnicalTag relative="../" tag="metric" text="Metrics" />.

### Features and promises

An Execution Engine provides the computing resources that will be used to actually perform <TechnicalTag relative="../" tag="validation" text="Validation" />. Great Expectations can take advantage of different Execution Engines, such as Pandas, Spark, or SqlAlchemy, and even translate the same <TechnicalTag relative="../" tag="expectation" text="Expectations" /> to validate data using different engines.

Data is always viewed through the lens of an Execution Engine in Great Expectations. When we obtain a <TechnicalTag relative="../" tag="batch" text="Batch" />Batch of data, that Batch contains metadata that wraps the native Data Object of the Execution Engine -- for example, a `DataFrame` in Pandas or Spark, or a table or query result in SQL.

### Relationship to other objects

Execution Engines are components of <TechnicalTag relative="../" tag="datasource" text="Datasources" />.  They accept <TechnicalTag relative="../" tag="batch_request" text="Batch Requests" /> and deliver Batches.  You will have to specify the Execution Engine for a Datasource in its configuration.  Beyond that, you will not need to directly interact with an Execution Engine under ordinary use cases.  The Execution Engine is instead an underlying component of the Datasource, and when you interact with the Datasource it will handle the Execution Engine for you.

## Use cases

<ConnectHeader/>

An Execution Engine is defined in the configuration of a Datasource.  After this, you will not need to directly interact with an Execution Engine.  Instead, it will be employed under the hood by the Datasoruce it is configured for.

<CreateHeader/>

If a <TechnicalTag relative="../" tag="profiler" text="Profiler" /> is used to create Expectations, or if you use the [interactive workflow for creating Expectations](../guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data.md), an Execution Engine will be involved as part of the Datasource used to provide data from a source data system for introspection.

<ValidateHeader/>

When a <TechnicalTag relative="../" tag="checkpoint" text="Checkpoint" /> Validates data, it uses a Datasource (and therefore an Execution Engine) to execute one or more Batch Requests and acquire the data that the Validation is run on.

## Features

### Standardized data and Expectations

Execution engines handle the interactions with the source data system that their Datasource is configured for.  However, they also wrap data from those source data systems with metadata that allows Great Expectations to read it regardless of its native format. Additionally, Execution Engines enable the calculations of Metrics used by Expectations so that they can operate in a format appropriate to their associated source data system.  Because of this, the same Expectations can be used to validate data from different Datasources, even if those Datasources interact with source data systems so different in nature that they require different Execution Engines to access their data. 

### Deferred Metrics

SqlAlchemyExecutionEngine and SparkDFExecutionEngine provide an additional feature that allows deferred resolution of Metrics, making it possible to bundle the request for several metrics into a single trip to the backend. Additional Execution Engines may also support this feature in the future.

The `resolve_metric_bundle()` method of these engines computes values of a bundle of Metrics; this function is used internally by `resolve_metrics()` on Execution Engines that support bundled metrics

## API basics

### How to access

You will not need to directly access an Execution Engine.  Instead, you will configure it as a part of a Datasource.  When you interact with a Datasource, it will handle the Execution Engine's operation under the hood.

### How to create

You will not need to directly instantiate an Execution Engine.  Instead, they are automatically created as a component in a Datasource.

If you are interested in using and accessing data with an Execution Engine that Great Expectations does not yet support, consider making your work a contribution to the [Great Expectations open source GitHub project](https://github.com/great-expectations/great_expectations).  This is a considerable undertaking, so you may also wish to [reach out to us on Slack](https://greatexpectations.io/slack) as we will be happy to provide guidance and support.

### Execution Engine init arguments

- `name`
- `caching`
- `batch_spec_defaults` (is this needed?)
- `batch_data_dict`
- `validator`

### Execution Engine Properties

- `loaded_batch_data` (all "loaded" batches)
- `active_batch_data_id`

### Execution Engine Methods

- `load_batch_data(batrch_id, batch_data)`
- `resolve_metrics`: computes metric values
- `get_compute_domain`: gets the compute domain for a particular type of intermediate metric.

### Configuration

Execution Engines and their configurations are specified in the configurations of Datasources.  In the configuration for your Datasource, you will have an `execution_engine` key.  This is a dictionary which will have at the least a `class_name` key that indicates the Execution Engine that will be associated with the Datasource.  If you are using a custom Execution Engine from a Plugin, you will also need to include a `module_name` key.  

The available Execution Engine classes are `PandasExecutionEngine`, `SparkDFExecutionEngine`, and `SqlAlchemyExecutionEngine`. The Spark Execution Engine is supported as a scalable alternative to Pandas.

If additional configuration is required by the Execution Engine, it will also be specified in the `execution_engine` configuration.  For example, the `SqlAlchemyExecutionEngine` will also expect the key `connection_string` as part of its configuration.

For specifics on the required keys for a given Execution Engine, please see our [how-to guides for Connecting to Data](../guides/connecting_to_your_data/index.md).