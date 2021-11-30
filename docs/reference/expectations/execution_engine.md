---
title: Execution Engine
---


:::info sam-notes

This page exists in the RTD docs but wasn't linked from anywhere. It looks like a draft.

:::

:::note

THE FOLLOWING WAS TAKEN FROM 20201007_execution_engine.md in github.com/superconductive/design The same documentation
also includes information on:

* Validation
* Expectations
* Expectation Bundles

:::

An `Execution Engine` provides the computing resources that will be used to actually perform validation. Great
Expectations can take advantage of many different Execution Engines, such as Pandas, Spark, or SqlAlchemy, and even
translate the same Expectations to validate data using different engines.

Data is always viewed through the lens of an Execution Engine in Great Expectations. When we obtain a Batch of data,
that Batch contains metadata that wraps the native Data Object of the Execution Engine -- for example, a DataFrame in
Pandas or Spark, or a table or query result in SQL.

## Execution Engine init arguments

- `name`
- `caching`
- `batch_spec_defaults` (is this needed?)
- `batch_data_dict`
- `validator`

## Execution Engine Properties

- `loaded_batch_data` (all "loaded" batches)
- `active_batch_data_id`

## Execution Engine Methods

- `load_batch_data(batrch_id, batch_data)`
- `resolve_metrics`: computes metric values
- `get_compute_domain`: gets the compute domain for a particular type of intermediate metric.

SqlAlchemyExecutionEngine and SparkDFExecutionEngine provide an additional feature that allows deferred resolution of
metrics, making it possible to bundle the request for several metrics into a single trip to the backend. Additional
Execution Engines may also support this feature in the future.

- `resolve_metric_bundle`: computes values of a bundle of metrics; this function is used internally by resolve_metrics
  on execution engines that support bundled metrics
  
## Validation Flow

1. Validator.graph_validate(expectation_suite)
2. for each Expectation: get_validation_dependencies
    ```python
      {
          "user_useful_name": MetricConfiguration,
          ...
      }
   ```
3. _populate_dependencies
4. for each dependent metric: get_evaluation_dependencies
    - a validation_graph object is ready. Nodes are MetricConfigurations, edges are dependencies.
5. `_parse_validation_graph`
6. for each set of ready_metrics: Execution Engine resolve_metrics
7. for each metric: bundleable?
  a. yes -> add to bundle
  b. no -> `resolve_metric`
    i. call metric_fn to get **value of metric**
8. `resolve_metric_bundle`
  a. for each metric in bundle:
    i. call metric_fn to get: **tuple(engine_function, domain_kwargs)**
    ii. add engine_function to resolve call for the domain
  b. for each domain, dispatch call to engine, and add resulting metrics to metrics dictionary
9. Expectation.validate(metrics) (now metrics are populated)
