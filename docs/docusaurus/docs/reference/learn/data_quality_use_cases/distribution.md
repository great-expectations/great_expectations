---
sidebar_label: 'Distribution'
title: 'Validate data distribution with GX'
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Data distribution analysis is a critical aspect of data quality management, focusing on understanding the spread, shape, and characteristics of data within a dataset. Data distribution is a pivotal indicator of the quality and reliability of your datasets. Key considerations include:

* **Statistical validity**: Many statistical analyses and machine learning models assume a certain data distribution. Deviations from expected distributions can invalidate these models.
* **Anomaly detection**: Unexpected changes in data distribution can signal anomalies, outliers, or fraud, which can skew analytical results or impair model performance.
* **Data consistency**: Consistent data distribution over time ensures that comparisons and trends are meaningful and accurate.
* **Quality control**: Monitoring data distribution helps in maintaining data integrity by detecting data corruption, system errors, or changes in data collection processes.

By leveraging Great Expectations (GX) to analyze and validate data distributions, organizations can ensure their datasets maintain expected characteristics. This guide will walk you through using GX to effectively manage and validate data distribution, helping you to maintain high-quality datasets and enhance the trustworthiness of your data-driven insights.

## Prerequisite knowledge

This article assumes basic familiarity with GX components and workflows. If you're new to GX, start with the [GX Cloud](https://docs.greatexpectations.io/docs/cloud/overview/gx_cloud_overview) and [GX Core](https://docs.greatexpectations.io/docs/guides/overview) overviews to familiarize yourself with key concepts and setup procedures.

## Data preview

The examples in this guide use a sample dataset of customer transactions that is provided from a public Postgres database table. The sample data is also available in [CSV format](https://raw.githubusercontent.com/great-expectations/great_expectations/develop/tests/test_sets/learn_data_quality_use_cases/distribution_purchases.csv).

| transaction_id | customer_id | purchase_amount | purchase_date     | product_category | product_rating | return_date       |
|----------------|-------------|-----------------|-------------------|------------------|----------------|-------------------|
| 1001           | 501         | 250.00          | 2024-01-15        | Electronics      | 4.5            | 2024-01-30        |
| 1002           | 502         | 40.00           | 2024-01-15        | Books            | 4.2            | null              |
| 1003           | 503         | 1200.00         | 2024-01-16        | Electronics      | 4.8            | null              |
| 1004           | 504         | 80.00           | 2024-01-16        | Clothing         | 3.9            | 2024-02-01        |
| 1005           | 505         | 3500.00         | 2024-01-17        | Electronics      | 4.6            | 2024-02-10        |

In this dataset, `purchase_amount` represents the amount spent by customers in various `product_category`. Analyzing the distribution of `purchase_amount` and other numerical columns can reveal insights into customer behavior and detect anomalies.

## Key distribution Expectations

GX offers a collection of Expectations used to validate data distribution. These Expectations be added to an Expectation Suite via the GX Cloud UI or using the GX Core Python library.

![Add a distribution Expectation in GX Cloud](./distribution_resources/distribution_add_expectation.gif)

:::note[Distribution Expectation availability]
`ExpectColumnKLDivergenceToBeLessThan` and `ExpectColumnQuantileValuesToBeBetween` can be added to a GX Cloud Expectation Suite, but currently must be added using GX Core instead of the GX Cloud UI.
:::


### Expect column KL divergence to be less than

Compares the distribution of a specified column to a reference distribution using the Kullback-Leibler (KL) divergence metric. KL divergence measures the difference between two probability distributions.

**Use Case**: This Expectation checks if the KL divergence is below a specified threshold, allowing you to assess the similarity between the actual and expected data distributions.

```python title="Python" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/distribution_resources/distribution_expectations.py ExpectColumnKLDivergenceToBeLessThan"
```

<small>View `ExpectColumnKLDivergenceToBeLessThan` in the [Expectation Gallery](https://greatexpectations.io/expectations/expect_column_kl_divergence_to_be_less_than).</small>


### Expect column value z-scores to be less than

Checks that the Z-scores (number of standard deviations from mean) of all values are below a threshold.

**Use Case**: Powerful for identifying individual outliers and anomalous data points that could represent data entry issues or unusual transactions.

```python title="Python" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/distribution_resources/distribution_expectations.py ExpectColumnValueZScoresToBeLessThan"
```

<small>View `ExpectColumnValueZScoresToBeLessThan` in the [Expectation Gallery](https://greatexpectations.io/expectations/expect_column_value_z_scores_to_be_less_than).</small>


### Expect column values to be between

Ensures that all values in a column fall between a specified minimum and maximum value.

**Use Case**: Essential for bounding numerical values within valid ranges, such as ensuring product ratings or purchase amounts are within reasonable limits.

```python title="Python" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/distribution_resources/distribution_expectations.py ExpectColumnValuesToBeBetween"
```

<small>View `ExpectColumnValuesToBeBetween` in the [Expectation Gallery](https://greatexpectations.io/expectations/expect_column_values_to_be_between).</small>


### Column-level summary statistic Expectations

GX provides a group of Expectations to validate that the summary statistics of a column fall within expected ranges. The table below lists the available Expectations in this category.

| Column summary statistic | Expectation name | View in the Expectation Gallery |
| :-- | :-- | :-- |
| Minimum | Expect column min to be between | [`ExpectColumnMinToBeBetween`](https://greatexpectations.io/expectations/expect_column_min_to_be_between) |
| Maximum | Expect column max to be between | [`ExpectColumnMaxToBeBetween`](https://greatexpectations.io/expectations/expect_column_max_to_be_between) |
| Mean | Expect column mean to be between | [`ExpectColumnMeanToBeBetween`](https://greatexpectations.io/expectations/expect_column_mean_to_be_between) |
| Median | Expect column median to be between | [`ExpectColumnMedianToBeBetween`](https://greatexpectations.io/expectations/expect_column_median_to_be_between) |
| Sum | Expect column sum to be between | [`ExpectColumnSumToBeBetween`](https://greatexpectations.io/expectations/expect_column_sum_to_be_between) |
| Standard deviation | Expect column stdev to be between | [`ExpectColumnStdevToBeBetween`](https://greatexpectations.io/expectations/expect_column_stdev_to_be_between) |
| Quantile values | Expect column quantile values to be between | [`ExpectColumnQuantileValuesToBeBetween`](https://greatexpectations.io/expectations/expect_column_quantile_values_to_be_between) |


The `ExpectColumnMinToBeBetween`, `ExpectColumnMaxToBeBetween`, `ExpectColumnMeanToBeBetween`, `ExpectColumnMedianToBeBetween`, `ExpectColumnSumToBeBetween`, and `ExpectColumnStdevToBeBetween` Expectations adhere to the same usage pattern and arguments. To define the expected range for the column and summary statistic, supply the `column` name along with `min_value` and `max_value` to define the lower and upper bounds of the expected statistic value range, respectively.

For example, if using `ExpectColumnMeanToBeBetween`:

```python title="Python" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/distribution_resources/distribution_expectations.py ExpectColumnMeanToBeBetween"
```

To use the `ExpectColumnQuantileValuesToBeBetween` Expectation, specify the `quantiles` and `value_ranges` as arguments.
```python title="Python" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/distribution_resources/distribution_expectations.py ExpectColumnQuantileValuesToBeBetween"
```


:::tip[GX tips for distribution Expectations]
- Use the `mostly` parameter to allow for acceptable deviations in your data, providing flexibility in your validations.
- `ExpectColumnValuesToBeBetween` can often be confused with `ExpectColumnMinToBeBetween` and `ExpectColumnMaxToBeBetween`. Use `ExpectColumnValuesToBeBetween` to define a single minimum or maxiumum value that is used to validate each value in the column.
Use `ExpectColumnMinToBeBetween` and `ExpectColumnMaxToBeBetween` to define a range that is used to validate the overall column minimum or maximum.
- Don't rely on a single distribution Expectation. Combine Expectations that check different aspects like the center (`ExpectColumnMeanToBeBetween`), spread (`ExpectColumnQuantileValuesToBeBetween`), and shape (`ExpectColumnKLDivergenceToBeLessThan`) of the distribution. Using multiple Expectations in concert gives a more comprehensive validation.
:::


## Example: Validate distribution of a column

**Goal**: Using a collection of distribution Expectations and either GX Cloud or GX Core, validate the distribution of the `purchase_amount` column.

<Tabs
   defaultValue="gx_cloud"
   values={[
      {value: 'gx_cloud', label: 'GX Cloud'},
      {value: 'gx_core', label: 'GX Core'}
   ]}
>

<TabItem value="gx_cloud" label="GX Cloud">

Use the GX Cloud UI to walk through the following steps.

1. Create a Postgres Data Asset for the `distribution_purchases` table, using the connection string:
   ```
   postgresql+psycopg2://try_gx:try_gx@postgres.workshops.greatexpectations.io/gx_learn_data_quality
   ```

2. Add an **Expect column mean to be between** Expectation to the freshly created Data Asset's default Expectation Suite.
    * Column: `purchase_amount`
    * Min Value: 1250
    * Max Value: 1500

3. Add an **Expect column median to be between** Expectation to the Expectation Suite.
    * Column: `purchase_amount`
    * Min Value: 1000
    * Max Value: 1250

4. Add an **Expect column value z-scores to be less than** Expectation to the Expectation Suite.
    * Column: `purchase_amount`
    * Threshold: 2
    * Mostly: 90%

6. Click the **Validate** button to validate the purchase data with the distribution Expectations.
7. Click **Validate**.
8. Review Validation Results.

**Result**: All Expectations pass.

![Validation Results for distribution Expectations in GX Cloud UI](./distribution_resources/distribution_example_validation_results.png)

</TabItem>

<TabItem value="gx_core" label="GX Core">
Run the following GX Core workflow.

```python title="" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/distribution_resources/distribution_workflow.py full workflow"
```

**Result**:
```python title="Python output"
Expectation Suite passed: True

expect_column_kl_divergence_to_be_less_than: True
expect_column_mean_to_be_between: True
expect_column_median_to_be_between: True
```

</TabItem>
</Tabs>

**GX solution**: GX provides Expectations in both GX Cloud or GX Core that enable validation of a variety of distribution aspects, including shape, center, and spread.

## Scenarios

### Detecting distribution anomalies

**Context**: Sudden spikes or drops in values can indicate data entry errors, upstream pipeline problems, or other unexpected system issues. Monitoring data distribution helps in early detection of such anomalies.

**GX solution**: Use `ExpectColumnValuesToBeBetween` and `ExpectColumnMeanToBeBetween` to ensure values are within expected ranges and the mean remains consistent. Use `ExpectColumnValueZScoresToBeLessThan` to validate that data retain their expected relationship to the distribution mean.

### Monitoring data drift in model inputs

**Context**: Machine learning models assume that the input data distribution remains consistent over time. Data drift can degrade model performance, and is a key factor in determining a model retraining cadence.

**GX solution**: Use `ExpectColumnKLDivergenceToBeLessThan` to compare current data distribution with a reference distribution and detect drift.

### Ensuring consistency in time-series data

**Context**: For time-series data, such as daily sales, consistency in distribution over time is crucial for accurate forecasting and analysis.

**GX solution**: Use `ExpectColumnQuantileValuesToBeBetween` to check that quantiles of the data remain within expected ranges.


## Avoid common distribution analysis pitfalls

- **Assuming static distributions**: Data distributions often evolve over time due to seasonality, trends, or changes in data collection. It is crucial to regularly update reference distributions and Expectations to reflect the current state of the data.

- **Overlooking data quality issues**: Data entry errors, missing values, or outliers can significantly distort the distribution. Comprehensive data quality checks, including handling missing data and outliers, is an essential complement to distribution analysis and validation.

- **Not accounting for multimodal distributions**: Some datasets may have multiple peaks, requiring appropriate methods and Expectations that can handle multimodal distributions. Ignoring multimodality can lead to inaccurate interpretations of the data.

- **Neglecting time-based changes**: Distributions may change over time due to seasonality or long-term trends. Implementing time-based analysis alongside point-in-time checks is crucial for understanding and adapting to evolving data distributions.

- **Insufficient sample size**: Small sample sizes may not accurately represent the true distribution of the data. It's important to ensure that the sample size is large enough to capture the underlying distribution and avoid drawing incorrect conclusions based on limited data.

## The path forward

Effectively managing and validating data distribution is essential for ensuring data integrity and reliability. To build upon the strategies discussed in this guide, consider the following actions:

1. **Integrate distribution checks into pipelines**: Embed distribution-focused Expectations into your data pipelines to automate monitoring and quickly detect anomalies.

2. **Leverage historical data**: Use historical data to establish baseline distributions, and update them regularly to account for trends and seasonality.

3. **Combine with other data quality checks**: Incorporate distribution analysis with other data quality aspects such as volume, missingness, and data types for a comprehensive validation strategy.

4. **Visualize distribution changes**: Implement tools to visualize data distributions over time, aiding in the detection of subtle shifts.

5. **Collaborate across teams**: Work with data scientists, analysts, and domain experts to interpret distribution changes and adjust Expectations accordingly.

By consistently applying these practices and expanding your validation processes, you can strengthen the integrity of your data and improve the accuracy of your analyses and models. Continue exploring our [data quality series](/reference/learn/data_quality_use_cases/dq_use_cases_lp.md) to learn more about integrating various aspects of data quality into your workflows.