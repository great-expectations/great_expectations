---
sidebar_label: 'MetricProviders'
title: 'MetricProviders'
id: metricproviders
description: Learn how MetricProviders are an integral component of the Expectation software development kit (SDK).
---

MetricProviders generate and register Metrics to support Expectations, and they are an important part of the Expectation software development kit (SDK). Typically, MetricProviders are used to resolve complex requirements and are not implemented widely. If you're considering using MetricProviders, you're encouraged to join the [Great Expectations Slack community](https://greatexpectations.io/slack) and discuss your requirements in the [#gx-community-support](https://greatexpectationstalk.slack.com/archives/CUTCNHN82) channel.

You'll encounter references to MetricProviders in many of the topics in the Great Expectations (GX) documentation. An in-depth knowledge of MetricProvider functionality is helpful, but not required to successfully complete the tasks documented in these topics. However, some knowledge of MetricProvider functionality is useful if you're developing Custom Expectations, or you're maintaining or extending MetricProvider classes.

MetricProviders let you declare all the Metrics that are needed to support an Expectation in concise, Don't Repeat Yourself (DRY) syntax. MetricProviders support the following use cases:

- Conceptually grouping code for Metrics calculation across multiple backends.
- Incremental development. You can develop Metrics for each Execution Engine, one at a time.
- Sharing Metrics across Expectations.
- Generating Metrics that can be inferred from specific types of Expectation automatically.

## Assumed knowledge

To get the most out of the information provided here, you should have an understanding of:

- Expectations
- Metrics
- The Metric registry
- ExecutionEngines

## When MetricProviders are required

A minimum of one supporting Metric is required by every Expectation. For example, `expect_column_mean_to_be_between` relies on a Metric that calculates the mean of a column. Often, an Expectation requires multiple Metrics. For example, the following Metrics are required in the `expect_column_values_to_be_in_set` Expectation:

- `column_values.in_set.unexpected_count`
- `column_values.in_set.unexpected_rows`
- `column_values.in_set.unexpected_values`
- `column_values.in_set.unexpected_value_counts`

To allow Expectations to work with multiple backends, methods for calculating Metrics need to be implemented for each ExecutionEngine. For example, pandas is implemented by calling the built-in pandas `.mean()` method on the column, Spark is implemented with a built-in Spark `mean` function, and SQLAlchemy is implemented with a SQLAlchemy generic function.

Metrics can help you incorporate conditional statements in Expectations that support conditional evaluations. For example, `column_values.in_set.condition`.

Metrics such as `column_values.in_set.unexpected_index_list` and `column_values.in_set.unexpected_index_query` can help you calculate the truthiness of your data.

## Class hierarchy

Although the class hierarchy for MetricProviders and Expectations is different, they use the same naming conventions. The following is the MetricProviders class hierarchy:

```text
MetricProvider
    QueryMetricProvider
    TableMetricProvider
        ColumnAggregateExpectation
    MapMetricProvider
        ColumnMapMetricProvider
            RegexColumnMapMetricProvider
            SetColumnMapMetricProvide
        ColumnPairMapMetricProvider
        MulticolumnMapExpectation
        MulticolumnMapMetricProvider
```

If you’re not sure which MetricProvider you should use with an Expectation, you can usually infer the correct MetricProvider class name from the Expectation class name. For example:

| Expectation class                 | MetricProvider class                    |
| --------------------------------- | --------------------------------------- |
| Expectation                       | MetricProvider                          |
| BatchExpectation                  | TableMetricProvider                     |
| ColumnAggregateExpectation        | ColumnAggregateMetricProvider           |
| ColumnMapExpectation              | ColumnMapMetricProvider                 |
| RegexBasedColumnMapExpectation    | RegexColumnMapMetricProvider (built in) |
| SetBasedColumnMapExpectation      | SetColumnMapMetricProvider (built in)   |
| ColumnPairMapExpectation          | ColumnPairMapMetricProvider             |
| MulticolumnMapExpectation         | MulticolumnMapMetricProvider            |
| QueryExpectation                  | QueryMetricProvider                     |


Sometimes, the MetricProvider class is created directly from the Expectation class, so you don’t need to specify a MetricProvider or methods when declaring a new Expectation. For example, the RegexBasedColumnMapExpectation automatically implements the RegexColumnMapMetricProvider, and the SetBasedColumnMapExpectation automatically implements the SetColumnMapMetricProvider.

## Define Metrics with a MetricProvider

The API for MetricProvider classes is unusual. MetricProvider classes are never intended to be instantiated, and they don’t have inputs or outputs in the normal sense of method arguments and return values. Instead, the inputs for MetricProvider classes are methods for calculating the Metric on different backend applications. Each method must be decorated with an appropriate decorator. On `new`, the MetricProvider class registers the decorated methods as part of the Metrics registry so that they can be invoked to calculate Metrics. The registered methods are the only output from MetricProviders.

:::note
Decorators invoked on `new` can make maintainability challenging. GX intends to address this shortcoming in future releases.
:::

A typical MetricProvider class has three methods that correspond to the three primary ExecutionEngines used by GX (pandas, SQL, and SparkDF). Each of the three methods are marked with an appropriate decorator.

Each MetricProvider class supports different decorators. Every MetricProvider class can use `@metric_value` and `@metric_partial`. For the MetricProvider, QueryMetricProvider, and TableMetricProvider classes, these are the only supported Metric decorators.

The following table lists the MetricProvider subclasses and their associated Metrics.

| Subclass                          | Supported Metrics                                              |
| --------------------------------- | ---------------------------------------------------------------|
| ColumnAggregateMetricProvider     | @column_aggregate_value, @column_aggregate_partial             |
| ColumnMapMetricProvider           | @column_condition_partial, @column_function_partial            |
| ColumnPairMapMetricProvider       | @column_pair_condition_partial                                 |
| MulticolumnMapMetricProvider      | @multicolumn_condition_partial, @multicolumn_function_partial  |

## Metric decorator naming conventions

The following Metric decorators differ in terms of the types of inputs and outputs they accept:

- A metric_value returns
- A metric_partials
- condition_partials versus function_partials
