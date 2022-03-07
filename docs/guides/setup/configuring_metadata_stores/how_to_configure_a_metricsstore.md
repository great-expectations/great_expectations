---
title: How to configure and use a MetricStore
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
- The `store_backend` field configures the particulars of how your metrics will be persisted. 
  The `class_name` field determines which class will be instantiated to create this `StoreBackend`, and other fields are passed through to the StoreBackend class on instantiation.
  In theory, any valid StoreBackend can be used, however at the time of writing, the only BackendStore under test for use with a MetricStore is the DatabaseStoreBackend with Postgres.
  To use an SQL Database like Postgres, provide two fields: `class_name`, with the value of `DatabaseStoreBackend`, and `credentials`.
  Credentials can point to credentials defined in your `config_variables.yml`, or alternatively can be defined inline.

```yaml
stores:
    #  ...
    metric_store:  # You can choose any name as the key for your metric store
        class_name: MetricStore
        store_backend:
            class_name: DatabaseStoreBackend
            credentials: ${my_store_credentials}
            # alternatively, define credentials inline:
            # credentials:
            #  username: my_username
            #  password: my_password
            #  port: 1234
            #  host: xxxx
            #  database: my_database
            #  driver: postgresql
```

The next time your DataContext is loaded, it will connect to the database and initialize a table to store metrics if
one has not already been created. See the metrics_reference for more information on additional configuration
options.

### Configuring a Validation Action

Once a MetricStore is available, a `StoreMetricsAction` Validation Action can be added to your Checkpoint in order to save metrics during
validation. This Validation Action has three required fields:
- The `class_name` field determines which class will be instantiated to execute this action, and must be `StoreMetricsAction`.
- The `target_store_name` field defines which Store backend to use when persisting the metrics. This should match the key of the MetricStore you added in your `great_expectations.yml`, which in our example above is `metrics_store`.
- The `requested_metrics` field identifies which Expectation Suites and metrics to store. Please note that this API is likely to change in a future release.
  Validation Result statistics are available using the following format:
    ```yaml
      expectation_suite_name:
        statistics.<statistic name>
   ```
  Values from inside a particular Expectation's `result` field are available using the following format:
   ```yaml
      expectation_suite_name:
        - column:
          <column name>:
            <expectation name>.result.<value name>
   ```
  In place of the Expectation Suite name, you may use `"*"` to denote that any expectation suite should match. 
     :::note Note:
  If an Expectation Suite name is used as a key, those metrics will only be added to the MetricStore when that Suite is run.
  When the wildcard `"*"` is used, those metrics will be added to the MetricStore for each Suite which runs in the Checkpoint.
  :::

Here is an example yaml config for adding a StoreMetricsAction to the `taxi_data` dataset:

```yaml
action_list:
  # ...
  - name: store_metrics
    action:
      class_name: StoreMetricsAction
      target_store_name: metric_store  # This should match the name of the store configured above
      requested_metrics:
        public.taxi_data.warning:  # match a particular expectation suite
          - column:
              passenger_count:
                - expect_column_values_to_not_be_null.result.element_count
                - expect_column_values_to_not_be_null.result.partial_unexpected_list
          - statistics.successful_expectations
        "*":  # wildcard to match any expectation suite
          - statistics.evaluated_expectations
          - statistics.success_percent
          - statistics.unsuccessful_expectations
```

### Test your MetricStore and StoreMetricsAction

To test your `StoreMetricsAction`, run your checkpoint from your code or the CLI:
```python
import great_expectations as ge
context = ge.get_context()
checkpoint_name = "your checkpoint name here"
context.run_checkpoint(checkpoint_name=checkpoint_name)
```
```bash
$ great_expectations checkpoint run <your checkpoint name>
```

### Summary
The `StoreMetricsValidationAction` processes an `ExpectationValidationResult` and stores Metrics to a configured Store.
Now, after your Checkpoint is run, the requested metrics will be available in your database!
