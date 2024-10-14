---
sidebar_label: 'Distribution'
title: 'Analyze data distribution with GX'
---

Data **distribution** analysis is a critical aspect of data quality management, focusing on understanding the spread, shape, and characteristics of data within a dataset. Proper distribution analysis helps identify anomalies, outliers, and patterns that may impact data reliability and the accuracy of analytical insights. By leveraging Great Expectations (GX) to analyze and validate data distributions, organizations can ensure their datasets maintain expected characteristics, leading to more robust and trustworthy data-driven decision-making processes.

This guide will walk you through leveraging GX to effectively manage and validate data distribution, helping you maintain high-quality datasets and enhance the trustworthiness of your data-driven insights.

## Understanding the impact of data distribution on quality

Data distribution plays a pivotal role in the quality and reliability of your datasets. Key considerations include:

1. **Anomaly Detection**: Unexpected changes in data distribution can signal anomalies, outliers, or fraud, which can skew analytical results or impair model performance.
2. **Statistical Validity**: Many statistical analyses and machine learning models assume a certain data distribution. Deviations from expected distributions can invalidate these models.
3. **Data Consistency**: Consistent data distribution over time ensures that comparisons and trends are meaningful and accurate.
4. **Quality Control**: Monitoring data distribution helps in maintaining data integrity by detecting data corruption, system errors, or changes in data collection processes.

By proactively analyzing and validating data distribution, you can detect and address issues early, maintaining the integrity and reliability of your data ecosystem.

## Prerequisite knowledge

This article assumes basic familiarity with GX components and workflows. If you're new to GX, start with the [GX Overview](https://docs.greatexpectations.io/docs/guides/overview) to familiarize yourself with key concepts and setup procedures.

## Data preview

The examples in this guide use a sample dataset of customer transactions, available as a [CSV file on GitHub](https://raw.githubusercontent.com/great-expectations/great_expectations/develop/tests/test_sets/learn_data_quality_use_cases/distribution_purchases.csv).

| transaction_id | customer_id | purchase_amount | purchase_date     | product_category | product_rating | return_date       |
|----------------|-------------|-----------------|-------------------|------------------|----------------|-------------------|
| 1001           | 501         | 250.00          | 2024-01-15        | Electronics      | 4.5            | 2024-01-30        |
| 1002           | 502         | 40.00           | 2024-01-15        | Books            | 4.2            | null              |
| 1003           | 503         | 1200.00         | 2024-01-16        | Electronics      | 4.8            | null              |
| 1004           | 504         | 80.00           | 2024-01-16        | Clothing         | 3.9            | 2024-02-01        |
| 1005           | 505         | 3500.00         | 2024-01-17        | Electronics      | 4.6            | 2024-02-10        |

In this dataset, `purchase_amount` represents the amount spent by customers in various `product_category`. Analyzing the distribution of `purchase_amount` and other numerical columns can reveal insights into customer behavior and detect anomalies.

## Key distribution Expectations

GX offers several Expectations specifically designed for managing data distribution. These can be added to an Expectation Suite via the GX Cloud UI or using the GX Core Python library.

:::TODO:::

![Add a distribution Expectation in GX Cloud](#TODO)


### Column-level Expectations

#### Expect Column Quantile Values To Be Between

Ensures that specified quantiles of a column fall within provided ranges.

**Use Case**: Robustly monitors key statistics of the overall distribution, which is valuable for tracking metrics like median purchase amount or 90th percentile product ratings.

```python
gxe.ExpectColumnQuantileValuesToBeBetween(
    column="purchase_amount",
    quantile_ranges={
        "quantiles": [0.5, 0.9],
        "value_ranges": [
            [50, 200],
            [500, 2000]
        ]
    }
)
```

<small>View `ExpectColumnQuantileValuesToBeBetween` in the [Expectation Gallery](https://greatexpectations.io/expectations/expect_column_quantile_values_to_be_between).</small>

#### Expect Column Stdev To Be Between

Validates that the standard deviation of a column is within a specified range.

**Use Case**: Watches for unusual changes in variance that could signal issues in data collection or processing pipelines.

```python
gxe.ExpectColumnStdevToBeBetween(
    column="purchase_amount",
    min_value=500,
    max_value=2000
)
```

<small>View `ExpectColumnStdevToBeBetween` in the [Expectation Gallery](https://greatexpectations.io/expectations/expect_column_stdev_to_be_between).</small>

<br/>
<br/>

:::tip[GX tip for column-level Expectations]
- Use the `mostly` parameter to allow for acceptable deviations in your data, providing flexibility in your validations.
- Regularly update your reference distributions (e.g., in `ExpectColumnKlDivergenceToBeLessThan`) to reflect the most recent data patterns.
- Don't rely on a single distribution Expectation. Combine Expectations that check different aspects like the center (`ExpectColumnMeanToBeBetween`), spread (`ExpectColumnQuantileValuesToBeBetween`), and shape (`ExpectColumnKlDivergenceToBeLessThan`) of the distribution. Using multiple Expectations in concert gives a more comprehensive validation.
:::

### Row-level Expectations

#### Expect Column Values To Be Between

Ensures that all values in a column fall between a specified minimum and maximum value.

**Use Case**: Essential for bounding numerical values within valid ranges, such as ensuring product ratings or purchase amounts are within reasonable limits.

```python
gxe.ExpectColumnValuesToBeBetween(
    column="product_rating",
    min_value=1,
    max_value=5,
)
```

<small>View `ExpectColumnValuesToBeBetween` in the [Expectation Gallery](https://greatexpectations.io/expectations/expect_column_values_to_be_between).</small>


#### Expect Column Value Z Scores To Be Less Than

Checks that the Z-scores (number of standard deviations from mean) of all values are below a threshold.

**Use Case**: Powerful for identifying individual outliers and anomalous data points that could represent data entry issues or unusual transactions.

```python
gxe.ExpectColumnValueZScoresToBeLessThan(
    column="purchase_amount",
    threshold=3,
)
```

<small>View `ExpectColumnValueZScoresToBeLessThan` in the [Expectation Gallery](https://greatexpectations.io/expectations/expect_column_value_z_scores_to_be_less_than).</small>

:::tip[GX tip for column-level Expectations]
- Base the range in `ExpectColumnValuesToBeBetween` on domain knowledge to ensure validity.
- Use `ExpectColumnValueZScoresToBeLessThan` to flag outliers, but set the threshold carefully to avoid false alarms.
- Analyze unexpected rows flagged by Row-level Expectations to uncover deeper insights about data issues.
:::

## Additional distribution Expectations

In addition to the key distribution Expectations discussed above, GX offers a range of other Expectations that can be useful for validating and monitoring data distributions. Some examples include:

- `ExpectColumnMedianToBeBetween`: Checks if the median value of a column is within a specified range. The median is the middle value when the data is sorted in ascending or descending order. This Expectation is useful for ensuring that the central tendency of the data falls within acceptable limits. (View in the [Expectation Gallery](https://greatexpectations.io/expectations/expect_column_median_to_be_between))

- `ExpectColumnSumToBeBetween`: Verifies that the sum of all values in a column is between a specified minimum and maximum value. This Expectation helps you check if the total of your data is within expected boundaries, which can be helpful for catching data anomalies or ensuring consistency. (View in the [Expectation Gallery](https://greatexpectations.io/expectations/expect_column_sum_to_be_between))

- `ExpectColumnKLDivergenceToBeLessThan`: Compares the distribution of a specified column to a reference distribution using the Kullback-Leibler (KL) divergence metric. KL divergence measures the difference between two probability distributions. This Expectation checks if the KL divergence is below a specified threshold, allowing you to assess the similarity between the actual and expected data distributions. (View in the [Expectation Gallery](https://greatexpectations.io/expectations/expect_column_kl_divergence_to_be_less_than))

These additional Expectations provide more advanced techniques for comparing and validating data distributions, allowing you to create even more comprehensive and tailored validation suites.

To learn more about these and other available Expectations, be sure to explore the [Expectation Gallery](https://greatexpectations.io/expectations). The gallery provides a complete list of Expectations, along with code examples and use cases to help you effectively apply them to your specific data quality challenges.

## Examples

### Validating customer age distribution

Here's an example GX Core workflow that validates the age distribution in the sample customer data:

```python
import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd

# Create Data Context.
context = gx.get_context()

# Connect to sample data, create Data Source and Data Asset.
CONNECTION_STRING = "postgresql+psycopg2://try_gx:try_gx@postgres.workshops.greatexpectations.io/gx_learn_data_quality"

data_source = context.data_sources.add_postgres(
    "postgres database", connection_string=CONNECTION_STRING
)
data_asset = data_source.add_table_asset(
    name="financial transfers table", table_name="distribution_purchases"
)

# Create an Expectation Suite
suite = context.add_expectation_suite(expectation_suite_name="purchase_amount_distribution_suite")

# Add distribution Expectations
purchase_amount_distribution_expectation = gxe.ExpectColumnKlDivergenceToBeLessThan(
    column="purchase_amount",
    partition_object={"bins": [0, 500, 1000, 1500, 2000, 2500, 3000, 3500, 4000], "weights": [0.3, 0.2, 0.15, 0.1, 0.1, 0.05, 0.05, 0.05]},
    threshold=0.1
)

purchase_amount_mean_expectation = gxe.ExpectColumnMeanToBeBetween(column="purchase_amount", min_value=500, max_value=2000)
purchase_amount_median_expectation = gxe.ExpectColumnMedianToBeBetween(column="purchase_amount", min_value=250, max_value=1500)

suite.add_expectation(purchase_amount_distribution_expectation)
suite.add_expectation(purchase_amount_mean_expectation)
suite.add_expectation(purchase_amount_median_expectation)

# Validate the data asset and capture the result
validator = context.get_validator(
    batch_request=data_asset.build_batch_request(),
    expectation_suite=suite
)
results = validator.validate()

# Print the validation results
print(results)
```

This example demonstrates how to validate the age distribution in the customer dataset using multiple distribution Expectations.

## Scenarios

### Detecting anomalies in transaction amounts

**Context**: Sudden spikes or drops in transaction amounts can indicate fraud, data entry errors, or system issues. Monitoring the distribution helps in early detection of such anomalies.

**GX solution**: Use `ExpectColumnValuesToBeBetween` and `ExpectColumnMeanToBeBetween` to ensure transaction amounts are within expected ranges and the mean remains consistent.

```python title="" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/distribution_resources/distribution_workflow.py transaction_anomalies"
# Validate that purchase_amount is within expected range
validator.expect_column_values_to_be_between(
    column="purchase_amount",
    min_value=1,
    max_value=10000,
)

# Validate that the mean of purchase_amount is within expected range
validator.expect_column_mean_to_be_between(
    column="purchase_amount",
    min_value=200,
    max_value=500
)
```

### Monitoring data drift in model inputs

**Context**: Machine learning models assume that the input data distribution remains consistent over time. Data drift can degrade model performance.

**GX solution**: Use `ExpectColumnKlDivergenceToBeLessThan` to compare current data distribution with a reference distribution and detect drift.

```python title="" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/distribution_resources/distribution_workflow.py model_data_drift"
# Set up reference distribution (e.g., from training data)
reference_distribution = {
    "bins": [0, 100, 500, 1000, 5000, 10000],
    "weights": [0.05, 0.25, 0.35, 0.25, 0.1]
}

# Validate that KL divergence is below threshold
validator.expect_column_kl_divergence_to_be_less_than(
    column="purchase_amount",
    partition_object=reference_distribution,
    threshold=0.1
)
```

### Ensuring consistency in time-series data

**Context**: For time-series data, such as daily sales, consistency in distribution over time is crucial for accurate forecasting and analysis.

**GX solution**: Use `ExpectColumnQuantileValuesToBeBetween` to check that quantiles of the data remain within expected ranges.

```python title="" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/distribution_resources/distribution_workflow.py time_series_consistency"
# Validate that quantiles are within expected ranges
validator.expect_column_quantile_values_to_be_between(
    column="purchase_amount",
    quantile_ranges={
        "quantiles": [0.25, 0.5, 0.75],
        "value_ranges": [
            [100, 150],
            [200, 250],
            [300, 400]
        ]
    }
)
```

## Avoid common distribution analysis pitfalls

- **Assuming Static Distributions**: Data distributions often evolve over time due to seasonality, trends, or changes in data collection. It's crucial to regularly update reference distributions and Expectations to reflect the current state of the data.

- **Overlooking Data Quality Issues**: Data entry errors, missing values, or outliers can significantly distort the distribution. Ensuring comprehensive data quality checks, including handling missing data and outliers, is essential for accurate distribution analysis.

- **Not Accounting for Multimodal Distributions**: Some datasets may have multiple peaks, requiring appropriate methods and Expectations that can handle multimodal distributions. Ignoring multimodality can lead to inaccurate interpretations of the data.

- **Neglecting Time-based Changes**: Distributions may change over time due to seasonality or long-term trends. Implementing time-based analysis alongside point-in-time checks is crucial for understanding and adapting to evolving data distributions.

- **Insufficient Sample Size**: Small sample sizes may not accurately represent the true distribution of the data. It's important to ensure that the sample size is large enough to capture the underlying distribution and avoid drawing incorrect conclusions based on limited data.

## Next steps: Enhancing distribution management

Effectively managing data distribution is essential for ensuring data integrity and reliability. To build upon the strategies discussed in this guide, consider the following actions:

1. **Integrate Distribution Checks into Pipelines**: Embed distribution-focused Expectations into your data pipelines to automate monitoring and quickly detect anomalies.

2. **Leverage Historical Data**: Use historical data to establish baseline distributions, and update them regularly to account for trends and seasonality.

3. **Combine with Other Data Quality Checks**: Incorporate distribution analysis with other data quality aspects such as volume, missingness, and data types for a comprehensive validation strategy.

4. **Visualize Distribution Changes**: Implement tools to visualize data distributions over time, aiding in the detection of subtle shifts.

5. **Collaborate Across Teams**: Work with data scientists, analysts, and domain experts to interpret distribution changes and adjust Expectations accordingly.

By consistently applying these practices and expanding your validation processes, you can strengthen the integrity of your data and improve the accuracy of your analyses and models. Continue exploring our data quality series to learn more about integrating various aspects of data quality into your workflows.


