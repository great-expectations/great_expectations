---
title: How to configure a MetricStore
---

Saving metrics during Validation makes it easy to construct a new data series based on observed
dataset characteristics computed by Great Expectations. That data series can serve as the source for a dashboard or
overall data quality metrics, for example.

Storing metrics is still a **beta** feature of Great Expectations, and we expect configuration and
capability to evolve rapidly.

### Adding a MetricStore

A MetricStore is a special store that can store Metrics computed during Validation. A MetricStore tracks the run_id
of the validation and the Expectation Suite name in addition to the metric name and metric kwargs.

To define a MetricStore, add a metric store config to the "stores" section of your `great_expectations.yml`.
This config requires two keys:
- The `class_name` field determines which class will be instantiated to create this store, and must be `MetricStore`.
- The `store_backend` field configures the particulars of how your metrics will be persisted. Any valid 

When defining your MetricStore, In most cases, a MetricStore will be configured as a SQL database. To add a MetricStore to your DataContext, 

```yaml
stores:
    #  ...
    metric_store:  # You can choose any name for your metric store
        class_name: MetricStore
        store_backend:
            class_name: DatabaseStoreBackend
            # These credentials can be the same as those used in a Datasource configuration
            credentials: ${my_store_credentials}
```


The next time your DataContext is loaded, it will connect to the database and initialize a table to store metrics if
one has not already been created. See the metrics_reference for more information on additional configuration
options.

### Configuring a Validation Action

Once a MetricStore is available, a `StoreMetricsAction` Validation Action can be added to your Checkpoint in order to save metrics during
validation. This Validation Action has three required fields:
- The `class_name` field determines which class will be instantiated to execute this action, and must be `StoreMetricsAction`.
- The `target_store_name` field defines which Store backend to use when persisting the metrics. This should match the key of the MetricStore you added in your `great_expectations.yml`, which in our example above is `metrics_store`.


Add the following yaml block to your Checkpoint's ``action_list``:

```yaml
action_list:
  # ...
  - name: store_metrics
    action:
      class_name: StoreMetricsAction
      target_store_name: metric_store  # Keep the space before this hash so it's not read as the name. This should match the name of the store configured above
      # Note that the syntax for selecting requested metrics will change in a future release
      requested_metrics:
        "*":  # The asterisk here matches *any* Expectation Suite name
          # use the 'kwargs' key to request metrics that are defined by kwargs,
          # for example because they are defined only for a particular column
          # - column:
          #     Age:
          #       - expect_column_min_to_be_between.result.observed_value
          - statistics.evaluated_expectations
          - statistics.successful_expectations
```

The `StoreMetricsValidationAction` processes an `ExpectationValidationResult` and stores Metrics to a configured Store.
Now, when your operator is executed, the requested metrics will be available in your database!


```python
context.run_validation_operator('action_list_operator', (batch_kwargs, expectation_suite_name))
```

:::note
To discuss with the Great Expectations community, please visit this topic in our community discussion forum: https://discuss.greatexpectations.io/t/ge-with-databricks-delta/82/3
:::
