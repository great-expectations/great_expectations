---
id: metric
title: Metric
hoverText: A computed attribute of data such as the mean of a column.
---
import UniversalMap from '/docs/images/universal_map/_universal_map.mdx';
import TechnicalTag from '../term_tags/_tag.mdx';
import CreateHeader from '/docs/images/universal_map/_um_create_header.mdx';
import ValidateHeader from '/docs/images/universal_map/_um_validate_header.mdx';


<UniversalMap setup='inactive' connect='inactive' create='active' validate='active'/> 

## Overview

### Definition

A Metric is a computed attribute of data such as the mean of a column.

### Features and promises

Metrics are values derived from one or more <TechnicalTag relative="../" tag="batch" text="Batches" /> that can be used to evaluate <TechnicalTag relative="../" tag="expectation" text="Expectations" /> or to summarize the result of <TechnicalTag relative="../" tag="validation" text="Validation" />. It can be helpful to think of a Metric as the answer to a question.  A Metric could be a statistic, such as the minimum value of the column, or a more complex object, such as a histogram. Metrics are a core part of Validating data.

### Relationship to other objects

Metrics are generated as part of running Expectations against a Batch (and can be referenced as such). For example, if you have an Expectation that the mean of a column falls within a certain range, the mean of the column must first be computed to see if its value is as expected.  The generation of Metrics involves <TechnicalTag relative="../" tag="execution_engine" text="Execution Engine" /> specific logic.  These Metrics can be included in <TechnicalTag relative="../" tag="validation_result" text="Validation Results" />, based on the `result_format` configured for them.  In memory Validation Results can in turn be accessed by Actions, including the `StoreValidationResultAction` which will store them in the <TechnicalTag relative="../" tag="validation_result_store" text="Validation Results Store" />.  Therefore, Metrics from previously run Expectation Suites can also be referenced by accessing stored Validation Results that contain them.

## Use cases

<CreateHeader/>

Metrics are generated in accordance with the requirements of an Expectation when an Expectation is evaluated.  This includes Expectations that are evaluated as part of the [interactive process for creating Expectations](../guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data.md) and the [Profiler process for creating Expectations](../guides/expectations/how_to_create_and_edit_expectations_with_a_profiler.md).  

Past Metrics can also be accessed by some Expectations through Evaluation Parameters.  However, when you are creating Expectations there may not be past Metrics to provide.  In these cases, it is possible to define a temporary value that the Evaluation Parameter can use in place of the missing past Metric.

<ValidateHeader/>

<TechnicalTag relative="../" tag="checkpoint" text="Checkpoints" /> Validate data by running the Expectations in one or more <TechnicalTag relative="../" tag="expectation_suite" text="Expectation Suite" />.  In the process, Metrics will be generated.  These Metrics can be passed to the Actions in the Checkpoint's `action_list` as part of the <TechnicalTag relative="../" tag="validation_result" text="Validation Results" /> for the Expectations (depending on the Validation Result's `result_format`), and will be stored in a <TechnicalTag relative="../" tag="metric_store" text="Metric Store" /> if the `StoreMetricsAction` is one of the <TechnicalTag relative="../" tag="validation_action" text="Actions" /> in the Checkpoint's `action_list`.


## Features

### Metrics are core to the Validation of data

When an Expectation should be evaluated, Great Expectations collects all the Metrics requested by the Expectation and provides them to the Expectation's validation logic. Most validation is done by comparing values from a column or columns to a Metric associated with the Expectation being evaluated.

### Past Metrics are available to other Expectations and Data Docs

An Expectation can also expose Metrics, such as the observed value of a useful statistic via an Expectation Validation Result, where <TechnicalTag relative="../" tag="data_docs" text="Data Docs" /> -- or other Expectations -- can use them.  This is done through an Action (to which the Expectation's Validation Result has been passed) which will save them to a Metric Store.  The Action in question is the `StoreMetricsAction`.  You can view the implementation of this Action in the `great_expectations/checkpoint/actions.py` file of a Great Expectations deployment, or the [same file in our GitHub.](https://github.com/great-expectations/great_expectations/blob/0312642755f6003c70623e9aa3ceed1020373dac/great_expectations/checkpoint/actions.py#L905)

## API basics

### How to access

Validation Results can expose Metrics that are defined by specific Expectations that have been validated, called "Expectation Defined Metrics." To access those values, you address the Metric as a dot-delimited string that identifies the value, such as `expect_column_values_to_be_unique.success`or `expect_column_values_to_be_between.result.unexpected_percent`. These Metrics may be stored in a Metrics Store.

It is important to note that these Metrics are *NOT* available for compute on raw data directly as other Metrics are.

A `metric_kwargs_id` is a string representation of the Metric Kwargs that can be used as a database key. For simple cases, it could be easily readable, such as `column=Age`, but when there are multiple keys and values or complex values, it will most likely be a md5 hash of key/value pairs. It can also be `None` in the case that there are no kwargs required to identify the Metric.

The following examples demonstrate how Metrics are defined:

```python title="Python code"
res = df.expect_column_values_to_be_in_set(
    "color",
    ["red", "green"]
)
res.get_metric(
    "expect_column_values_to_be_in_set.result.missing_count",
    column="color"
)
```

See the [How to configure a MetricsStore](../guides/setup/configuring_metadata_stores/how_to_configure_a_metricsstore.md) guide for more information.

### How to create

Metrics are produced using logic specific to the Execution Engine associated with the Datasource that provides the data for the Batch Request/s that the Metric is calculated for.  That logic that is defined in a `MetricProvider`. When a `MetricProvider` class is first encountered, Great Expectations will register the Metric and any methods that it defines as able to produce Metrics.  The registered metric will then be able to be used with `validator.get_metric()` or `validator.get_metrics()`. 

### Configuration

Configuration of Metrics is applied when they are defined as part of an Expectation.

#### Metrics naming conventions

Metrics can have any name. However, for the "core" Great Expectations Metrics, we use the following conventions:

* For **aggregate Metrics**, such as the mean value of a column, we use the domain and name of the statistic, such as `column.mean` or `column.max`.
* For **map Metrics**, which produce values for individual records or rows, we define the domain using the prefix "column_values" and use several consistent suffixes to provide related Metrics. For example, for the Metric that defines whether specific column values fall into an expected set, several related Metrics are defined:
    * `column_values.in_set.unexpected_count` provides the total number of unexpected values in the domain.
    * `column_values.in_set.unexpected_values` provides a sample of unexpected_values; "result_format" is one of its
      value_keys to determine how many values should be returned.
    * `column_values.in_set.unexpected_rows` provides full rows for which the value in the domain column was unexpected
    * `column_values.in_set.unexpected_value_counts` provides a count of how many times each unexpected value occurred
