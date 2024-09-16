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

### Expect Column Values To Be Between

Ensures that all values in a column fall between a specified minimum and maximum value.

**Use Case**: Essential for bounding numerical values within valid ranges, such as ensuring product ratings or purchase amounts are within reasonable limits.

```python
validator.expect_column_values_to_be_between(
    column="product_rating",
    min_value=1,
    max_value=5,
)
```

<small>View `ExpectColumnValuesToBeBetween` in the [Expectation Gallery](https://greatexpectations.io/expectations/expect_column_values_to_be_between).</small>

### Expect Column Pair Values A To Be Greater Than B

Validates that values in one column are greater than corresponding values in another column.

**Use Case**: Crucial for maintaining valid relationships between data entities, like verifying that return dates are later than purchase dates.

```python
validator.expect_column_pair_values_A_to_be_greater_than_B(
    column_A="return_date",
    column_B="purchase_date",
)
```

<small>View `ExpectColumnPairValuesAToBeGreaterThanB` in the [Expectation Gallery](https://greatexpectations.io/expectations/expect_column_pair_values_A_to_be_greater_than_B).</small>

### Expect Column Value Z Scores To Be Less Than

Checks that the Z-scores (number of standard deviations from mean) of all values are below a threshold.

**Use Case**: Powerful for identifying individual outliers and anomalous data points that could represent data entry issues or unusual transactions.

```python
validator.expect_column_value_z_scores_to_be_less_than(
    column="purchase_amount",
    threshold=3,
)
```

<small>View `ExpectColumnValueZScoresToBeLessThan` in the [Expectation Gallery](https://greatexpectations.io/expectations/expect_column_value_z_scores_to_be_less_than).</small>

### Expect Column Quantile Values To Be Between

Ensures that specified quantiles of a column fall within provided ranges.

**Use Case**: Robustly monitors key statistics of the overall distribution, which is valuable for tracking metrics like median purchase amount or 90th percentile product ratings.

```python
validator.expect_column_quantile_values_to_be_between(
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

### Expect Column Stdev To Be Between

Validates that the standard deviation of a column is within a specified range.

**Use Case**: Watches for unusual changes in variance that could signal issues in data collection or processing pipelines.

```python
validator.expect_column_stdev_to_be_between(
    column="purchase_amount",
    min_value=500,
    max_value=2000
)
```

<small>View `ExpectColumnStdevToBeBetween` in the [Expectation Gallery](https://greatexpectations.io/expectations/expect_column_stdev_to_be_between).</small>

<br/>
<br/>

:::tip[GX tips for distribution Expectations]
- Use the `mostly` parameter to allow for acceptable deviations in your data, providing flexibility in your validations.
- Regularly update your reference distributions (e.g., in `ExpectColumnKlDivergenceToBeLessThan`) to reflect the most recent data patterns.
- Combine multiple distribution Expectations to create a comprehensive validation suite that covers central tendency, dispersion, and shape of your data distribution.
:::

## Additional distribution Expectations

In addition to the key distribution Expectations discussed above, GX offers a range of other Expectations that can be useful for validating and monitoring data distributions. Some examples include:

- `ExpectColumnMedianToBeBetween`: Checks if the median value of a column is within a specified range. The median is the middle value when the data is sorted in ascending or descending order. This Expectation is useful for ensuring that the central tendency of the data falls within acceptable limits.

- `ExpectColumnSumToBeBetween`: Verifies that the sum of all values in a column is between a specified minimum and maximum value. This Expectation helps you check if the total of your data is within expected boundaries, which can be helpful for catching data anomalies or ensuring consistency.

- `ExpectColumnKLDivergenceToBeLessThan`: Compares the distribution of a specified column to a reference distribution using the Kullback-Leibler (KL) divergence metric. KL divergence measures the difference between two probability distributions. This Expectation checks if the KL divergence is below a specified threshold, allowing you to assess the similarity between the actual and expected data distributions.

These additional Expectations provide more advanced techniques for comparing and validating data distributions, allowing you to create even more comprehensive and tailored validation suites.

To learn more about these and other available Expectations, be sure to explore the [Expectation Gallery](https://greatexpectations.io/expectations). The gallery provides a complete list of Expectations, along with code examples and use cases to help you effectively apply them to your specific data quality challenges.

## Examples

### Validating customer age distribution

Here's an example GX Core workflow that validates the age distribution in the sample customer data:

```python
import great_expectations as gx
import pandas as pd

# Load the sample data
df = pd.read_csv("distribution_customer_data.csv")

# Create a Data Context and add a Pandas Data Source
context = gx.get_context()
datasource = context.sources.add_pandas(name="my_pandas_datasource")

# Create a DataFrame Asset
asset = datasource.add_dataframe_asset(name="customer_data", dataframe=df)

# Create an Expectation Suite
suite = context.add_expectation_suite(expectation_suite_name="customer_age_distribution_suite")

# Add distribution Expectations
suite.expect_column_kl_divergence_to_be_less_than(
    column="age",
    partition_object={"bins": [0, 20, 40, 60, 80, 100], "weights": [0.05, 0.25, 0.4, 0.25, 0.05]},
    threshold=0.1
)
suite.expect_column_mean_to_be_between(column="age", min_value=30, max_value=50)
suite.expect_column_median_to_be_between(column="age", min_value=35, max_value=45)

# Create a Validator and run the suite
validator = context.get_validator(
    batch_request=asset.build_batch_request(),
    expectation_suite=suite
)
results = validator.validate()

# Print the results
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


- **Ignoring Small Changes**: Minor shifts in distribution can accumulate over time, leading to significant deviations. Regularly monitor and analyze even small changes.

- **Assuming Static Distributions**: Data distributions can evolve due to seasonality, trends, or changes in data collection. Update your reference distributions and Expectations to reflect current data.

- **Overlooking Data Quality Issues**: Data entry errors, missing values, or outliers can distort the distribution. Ensure comprehensive data quality checks, including handling of missing data and outliers.

- **Not Accounting for Multimodal Distributions**: Some data may have multiple peaks. Use appropriate methods and Expectations that can handle multimodal distributions.

- **Failure to Collaborate with Domain Experts**: Involving subject matter experts can help in setting realistic thresholds and interpreting distribution changes accurately.

- **Assuming Normal Distribution**: Not all data follows a normal distribution. Use appropriate Expectations and techniques for non-normal distributions when necessary.
- **Ignoring Multimodality**: Some datasets may have multiple peaks. Be cautious when using simple summary statistics and consider using more advanced distribution analysis techniques.
- **Overlooking Outliers**: Extreme values can significantly impact distribution metrics. Use robust statistics and consider separate Expectations for handling outliers.
- **Neglecting Time-based Changes**: Distributions may change over time due to seasonality or long-term trends. Implement time-based analysis alongside point-in-time checks.
- **Misinterpreting Correlation**: Distribution similarities don't always imply correlation. Use additional statistical tests to validate relationships between variables.

## Next steps: Enhancing distribution management

Effectively managing data distribution is essential for ensuring data integrity and reliability. To build upon the strategies discussed in this guide, consider the following actions:

1. **Integrate Distribution Checks into Pipelines**: Embed distribution-focused Expectations into your data pipelines to automate monitoring and quickly detect anomalies.

2. **Leverage Historical Data**: Use historical data to establish baseline distributions, and update them regularly to account for trends and seasonality.

3. **Combine with Other Data Quality Checks**: Incorporate distribution analysis with other data quality aspects such as volume, missingness, and data types for a comprehensive validation strategy.

4. **Visualize Distribution Changes**: Implement tools to visualize data distributions over time, aiding in the detection of subtle shifts.

5. **Collaborate Across Teams**: Work with data scientists, analysts, and domain experts to interpret distribution changes and adjust Expectations accordingly.

By consistently applying these practices and expanding your validation processes, you can strengthen the integrity of your data and improve the accuracy of your analyses and models. Continue exploring our data quality series to learn more about integrating various aspects of data quality into your workflows.


