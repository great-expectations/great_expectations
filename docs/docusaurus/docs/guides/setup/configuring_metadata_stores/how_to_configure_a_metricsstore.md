---
title: How to configure and use a MetricStore
---
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

:::note Note:
Metric storage is an **experimental** feature.
:::

A `MetricStore` is a <TechnicalTag tag="store" text="Store" /> that stores Metrics computed during Validation. A `MetricStore` tracks the `run_id` of the Validation and the <TechnicalTag tag="expectation_suite" text="Expectation Suite" /> name in addition to the Metric name and Metric kwargs.

Saving <TechnicalTag tag="metric" text="Metrics" /> during <TechnicalTag tag="validation" text="Validation" /> lets you construct a new data series based on observed dataset characteristics computed by Great Expectations. A data series can serve as the source for a dashboard, or overall data quality metrics.

## Prerequisites

- [A Great Expectations instance](/docs/guides/setup/setup_overview)
- Completion of the [Quickstart](tutorials/quickstart/quickstart.md)
- [A configured Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_quickly_instantiate_a_data_context)

## 1. Add a MetricStore

To define a `MetricStore`, add a <TechnicalTag tag="metric_store" text="Metric Store" /> configuration to the `stores` section of your `great_expectations.yml`. The configuration must include the following keys:

- `class_name` - Enter `MetricStore`. This key determines which class is instantiated to create the `StoreBackend`. Other fields are passed through to the `StoreBackend` class on instantiation. The only backend Store under test for use with a `MetricStore` is the `DatabaseStoreBackend` with Postgres.

- `store_backend` - Defines how your metrics are persisted. 

To use an SQL Database such as Postgres, add the following fields and values: 

- `class_name` - Enter `DatabaseStoreBackend`. 

- `credentials` - Point to the credentials defined in your `config_variables.yml`, or define them inline.

The following is an example of how the `MetricStore` configuration appears in `great_expectations.yml`:

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

The next time your Data Context is loaded, it will connect to the database and initialize a table to store metrics if one has not already been created.

## 2. Configure a Validation Action

When a `MetricStore` is available, add a `StoreMetricsAction` validation <TechnicalTag tag="action" text="Action" /> to your <TechnicalTag tag="checkpoint" text="Checkpoint" /> to save Metrics during Validation. The validation Action must include the following fields:

- `class_name` - Enter `StoreMetricsAction`. Determines which class is instantiated to execute the Action.

- `target_store_name` - Enter the key for the MetricStore you added in your `great_expectations.yml`. In the previous example, the `metrics_store`field defines which Store backend to use when persisting the metrics.

- `requested_metrics` - Identify the Expectation Suites and Metrics you want to store.
  
Add the following entry to `great_expectations.yml` to generate <TechnicalTag tag="validation_result" text="Validation Result" /> statistics:

```yaml
  expectation_suite_name:
    statistics.<statistic name>
```

Add the following entry to `great_expectations.yml` to generate values from a specific <TechnicalTag tag="expectation" text="Expectation" /> `result` field:

```yaml
  expectation_suite_name:
    - column:
      <column name>:
        <expectation name>.result.<value name>
```

To indicate that any Expectation Suite can be used to generate values, use the wildcard `"*"`. 

:::note Note:
If you use an Expectation Suite name as a key, Metrics are only added to the `MetricStore` when the Expectation Suite runs. When you use the wildcard `"*"`, Metrics are added to the `MetricStore` for each Expectation Suite that runs in the Checkpoint.
:::

The following example yaml configuration adds `StoreMetricsAction` to the `taxi_data` dataset:

```
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

## 3. Test your MetricStore and StoreMetricsAction

Run the following command to run your Checkpoint and test `StoreMetricsAction`:

<!--A snippet is required for this code block.-->

```python
import great_expectations as gx
context = gx.get_context()
checkpoint_name = "your checkpoint name here"
context.run_checkpoint(checkpoint_name=checkpoint_name)
```