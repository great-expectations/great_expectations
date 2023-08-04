---
id: execution_engine
title: Execution Engine
hoverText: A system capable of processing data to compute Metrics
---

import TechnicalTag from '../term_tags/_tag.mdx';

An Execution Engine is a system capable of processing data to compute <TechnicalTag relative="../" tag="metric" text="Metrics" />.

An Execution Engine provides the computing resources that will be used to actually perform <TechnicalTag relative="../" tag="validation" text="Validation" />. Great Expectations can take advantage of different Execution Engines, such as Pandas, Spark, or SqlAlchemy, and even translate the same <TechnicalTag relative="../" tag="expectation" text="Expectations" /> to validate data using different engines.

Data is always viewed through the lens of an Execution Engine in Great Expectations. When we obtain a <TechnicalTag relative="../" tag="batch" text="Batch" /> of data, that Batch contains metadata that wraps the native Data Object of the Execution Engine -- for example, a `DataFrame` in Pandas or Spark, or a table or query result in SQL.

## Relationship to other objects

Execution Engines are components of <TechnicalTag relative="../" tag="datasource" text="Datasources" />.  They accept <TechnicalTag relative="../" tag="batch_request" text="Batch Requests" /> and deliver Batches.  The Execution Engine is an underlying component of the Data Source, and when you interact with the Data Source it will handle the Execution Engine for you.

## Use cases

You define the Execution Engine that you want to use to process data to compute Metrics in the Data Source configuration.  After you define the Execution Engine, you don't need to interact with it because the Data Source it is configured for uses it automatically.

If a <TechnicalTag relative="../" tag="profiler" text="Profiler" /> is used to create Expectations, or if you use the [interactive workflow for creating Expectations](../guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data.md), an Execution Engine will be involved as part of the Data Source used to provide data from a source data system for introspection.

When a <TechnicalTag relative="../" tag="checkpoint" text="Checkpoint" /> Validates data, it uses a Data Source (and therefore an Execution Engine) to execute one or more Batch Requests and acquire the data that the Validation is run on.

When creating Custom Expectations and Metrics, often Execution Engine-specific logic is required for that Expectation or Metric. See [Custom Expectations](../guides/expectations/custom_expectations_lp.md) for more information.

## Standardized data and Expectations

Execution engines handle the interactions with the source data.  They also wrap data from those source data systems with metadata that allows Great Expectations to read it regardless of its native format. Additionally, Execution Engines enable the calculations of Metrics used by Expectations so that they can operate in a format appropriate to their associated source data system.  Because of this, the same Expectations can be used to validate data from different Datasources, even if those Datasources interact with source data systems so different in nature that they require different Execution Engines to access their data. 

## Deferred Metrics

SqlAlchemyExecutionEngine and SparkDFExecutionEngine provide an additional feature that allows deferred resolution of Metrics, making it possible to bundle the request for several metrics into a single trip to the backend. Additional Execution Engines may also support this feature in the future.

The `resolve_metric_bundle()` method of these engines computes values of a bundle of Metrics; this function is used internally by `resolve_metrics()` on Execution Engines that support bundled metrics.

## Access

You will not need to directly access an Execution Engine. When you interact with a Data Source it will handle the Execution Engine's operation under the hood.

## Create

You will not need to directly instantiate an Execution Engine.  Instead, they are automatically created as a component in a Data Source.

If you are interested in using and accessing data with an Execution Engine that Great Expectations does not yet support, consider making your work a contribution to the [Great Expectations open source GitHub project](https://github.com/great-expectations/great_expectations).  This is a considerable undertaking, so you may also wish to [reach out to us on Slack](https://greatexpectations.io/slack) as we will be happy to provide guidance and support.

### Execution Engine init arguments

- `name`
- `caching`
- `batch_spec_defaults`
- `batch_data_dict`
- `validator`

### Execution Engine Properties

- `loaded_batch_data`
- `active_batch_data_id`

### Execution Engine Methods

- `load_batch_data(batrch_id, batch_data)`
- `resolve_metrics`: computes metric values
- `get_compute_domain`: gets the compute domain for a particular type of intermediate metric.

## Configure

Execution Engines are not configured directly, but determined based on the Data Source you choose.
