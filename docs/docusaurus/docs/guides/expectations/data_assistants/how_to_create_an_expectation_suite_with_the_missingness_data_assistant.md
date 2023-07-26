---
title: Create an Expectation Suite with the Missingness Data Assistant
---

import Prerequisites from '/docs/components/_prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

:::note Note:
Missingness Data Assistant is an **experimental** feature.
:::

Use the information provided here to learn how you can use the Missingness Data Assistant to profile your data and automate the creation of an Expectation Suite.

All the code used in the examples is available in GitHub at this location: [how_to_create_an_expectation_suite_with_the_missingness_data_assistant.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_missingness_data_assistant.py).

## Prerequisites

<Prerequisites>

- A [configured Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_quickly_instantiate_a_data_context).
- An understanding of how to [configure a Datasource](../../connecting_to_your_data/connect_to_data_lp.md).
- An understanding of how to [configure a Batch Request](/docs/0.15.50/guides/connecting_to_your_data/how_to_get_one_or_more_batches_of_data_from_a_configured_datasource).

</Prerequisites>

## Prepare your Batch Request

In the following examples, you'll be using a Batch Request with multiple Batches and the Datasource that the Batch Request queries uses existing New York taxi trip data.

This is the `Datasource` configuration:
 
```python name="tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_missingness_data_assistant.py datasource_config"
```

This is the `BatchRequest` configuration:

```python name="tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_missingness_data_assistant.py batch_request"
```

:::caution
The Missingness Data Assistant runs multiple queries against your `Datasource`. Data Assistant performance can vary significantly depending on the number of Batches, the number of records per Batch, and network latency. If Data Assistant runtimes are too long, use a smaller `BatchRequest`. You can also run the Missingness Data Assistant on a single Batch when you expect the number of null records to be similar across Batches.
:::

## Prepare a new Expectation Suite

Run the following code to prepare a new Expectation Suite with the Data Context `add_expectation_suite(...)` method:

```python name="tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_missingness_data_assistant.py expectation_suite"
```

## Run the Missingness Data Assistant

To run a Data Assistant, you can call the `run(...)` method for the assistant. However, there are numerous parameters available for the `run(...)` method of the Missingness Data Assistant. For instance, the `exclude_column_names` parameter allows you to define the columns that should not be Profiled.

1. Run the following code to define the columns to exclude:

  ```python name="tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_missingness_data_assistant.py exclude_column_names"
  ```

2. Run the following code to run the Missingness Data Assistant:

  ```python name="tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_missingness_data_assistant.py data_assistant_result"
  ```

  In this example, `context` is your Data Context instance.

  :::note
  If you consider your `BatchRequest` data valid, and want to produce Expectations with ranges that are identical to the data in the `BatchRequest`, you don't need to alter the example code. You're using the default `estimation` parameter (`"exact"`). To identify potential outliers in your `BatchRequest` data, pass `estimation="flag_outliers"` to the `run(...)` method.
  :::

  :::note
  The Missingness Data Assistant `run(...)` method can accept other parameters in addition to `exclude_column_names` such as `include_column_names`, `include_column_name_suffixes`, and `cardinality_limit_mode`. To view the available parameters, see [this information](https://github.com/great-expectations/great_expectations/blob/develop/great_expectations/rule_based_profiler/data_assistant/column_value_missing_data_assistant.py#L44).
  :::

## Save your Expectation Suite

1. After executing the Missingness Data Assistant's `run(...)` method and generating Expectations for your data, run the following code to load and save them into your Expectation Suite:

  ```python name="tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_missingness_data_assistant.py get_expectation_suite"
  ```

2. Run the following code to save the Expectation Suite:

  ```python name="tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_missingness_data_assistant.py save_expectation_suite"
  ```

## Test your Expectation Suite

  Run the following code to use a Checkpoint to operate with the Expectation Suite and Batch Request that you defined:

  ```python name="tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_missingness_data_assistant.py checkpoint"
  ```

  You can check the `"success"` key of the Checkpoint's results to verify that your Expectation Suite worked.

## Plot and inspect Metrics and Expectations

1. Run the following code to view Batch-level visualizations of the Metrics computed by the Missingness Data Assistant:

  ```python name="tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_missingness_data_assistant.py plot_metrics"
  ```

  ![Plot Metrics](../../../images/data_assistant_plot_metrics.png)

  :::note
  Hover over a data point to view more information about the Batch and its calculated Metric value.
  :::

2. Run the following code to view all Metrics computed by the Missingness Data Assistant:

  ```python name="tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_missingness_data_assistant.py metrics_by_domain"
  ```

3. Run the following code to plot the Expectations and the associated Metrics calculated by the Missingness Data Assistant:

  ```python name="tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_missingness_data_assistant.py plot_expectations_and_metrics"
  ```

  ![Plot Expectations and Metrics](../../../images/data_assistant_plot_expectations_and_metrics.png)

  :::note
  The Expectation and the Metric are not visualized by the `plot_expectations_and_metrics()` method when an Expectation is not produced by the Missingness Data Assistant for a given Metric.
  :::

4. Run the following command to view the Expectations produced and grouped by Domain:

  ```python name="tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_missingness_data_assistant.py show_expectations_by_domain_type"
  ```

5. Run the following command to view the Expectations produced and grouped by Expectation type:

  ```python name="tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_missingness_data_assistant.py show_expectations_by_expectation_type"
  ```

## Edit your Expectation Suite (Optional)

The Missingness Data Assistant creates as many Expectations as it can for the permitted columns. Although this can help with data analysis, it might be unnecessary.  It is also possible that you may possess some domain knowledge that is not reflected in the data that was sampled for the Profiling process. In these types of scenarios, you can edit your Expectation Suite to better align with your business requirements.

Run the following code to edit an existing Expectation Suite:

```markdown title="Terminal command"
great_expectations suite edit <expectation_suite_name>
```

A Jupyter Notebook opens. You can review, edit, and save changes to the Expectation Suite.