---
sidebar_label: 'Missingness'
title: 'Manage missing data with GX'
---

Missing data, also known as **missingness**, poses a significant challenge in data quality management. Missing data occurs when expected information is absent from a dataset, often appearing as `NULL` values in databases or manifesting differently across various systems. Effectively managing this issue is crucial for maintaining data integrity and reliability since unaddressed missing data can lead to disrupted calculations, skewed analyses, and compromised data analytics tasks.

Great Expectations (GX) offers a robust solution for addressing missing data through a comprehensive suite of Expectations that allow users to define and enforce data quality rules. By integrating GX into your data pipelines, you can establish robust validation processes that catch issues early, ensuring your dataset remains clean, consistent, and ready for accurate reporting, predictive modeling, and other advanced analytics applications.

In this guide, you will learn how to leverage GX to effectively handle missing data. This includes writing Expectations, setting up automated checks, and making informed decisions about managing various types of missingness. By following these steps, you can ensure your datasets maintain high quality, thus enabling more accurate and reliable data-driven insights.

## Prerequisite knowledge

Before diving into this guide, ensure you have a basic understanding of GX components and workflows. If you're new to GX, start with the [GX Overview](https://docs.greatexpectations.io/docs/guides/overview) to familiarize yourself with key concepts and setup procedures.

## Data preview

The examples in this guide use a sample transaction dataset, available as a [CSV file on GitHub](https://raw.githubusercontent.com/great-expectations/great_expectations/develop/tests/test_sets/learn_data_quality_use_cases/missingness.csv). This dataset contains fields prone to missing data, ideal for demonstrating GX's capabilities.

| type     | sender_account_number  | recipient_fullname | transfer_amount | transfer_date       | errors |
|----------|------------------------|--------------------|-----------------|---------------------|--------|
| domestic | 244084670977           | Jaxson Duke        | 9143.40         | 2024-05-01 01:12    | NULL   |
| NULL     | 123456789012           | Jane Smith         | 5000.00         | NULL                | NULL   |

In this dataset, you'll notice missing data in the `type` and `transfer_date` columns. The `errors` column, populated by a separate monitoring system, indicates issues found during data processing. A `NULL` value in the `errors` column means no errors were detected for that record.

## Key missingness Expectations

GX provides a suite of missingness-focused Expectations to manage missing data in your datasets. These Expectations can be added to an Expectation Suite via the GX Cloud UI or using the GX Core Python library.


:::[TODO]
> Add animated GIF demonstrating how to create Expectations using the GX Cloud UI.
:::

### Expect Column Values To Be Null

Ensures that values within a column are `NULL`.

**Use Case**: Handle columns that should typically be populated but might be left null due to specific conditions.

```python title="" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/missingness_resources/missingness_expectations.py ExpectColumnValuesToBeNull"
```

<sup>View `ExpectColumnValuesToBeNull` in the [Expectation Gallery](https://greatexpectations.io/expectations/expect_column_values_to_be_null).</sup>

### Expect Column Values To Not Be Null

Ensures that values within a specific column are not `NULL`.

**Use Case**: Ensure critical columns are consistently populated, such as `transfer_amount`.

```python title="Python" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/missingness_resources/missing_expectations.py ExpectColumnValuesToNotBeNull"
```

<sup>View `ExpectColumnValuesToNotBeNull` in the [Expectation Gallery](https://greatexpectations.io/expectations/expect_column_values_to_not_be_null).</sup>

<br/>
<br/>

:::tip[GX tips for missingness Expectations]
- Use the `mostly` argument in `ExpectColumnValuesToBeNull` and `ExpectColumnValuesToNotBeNull` to set different tolerance levels for null and non-null values. This allows you to tailor your data quality checks to different columns. For example, you might allow more null values in a 'customer_feedback' column than in an 'errors' column. Adjust these levels based on your data patterns and business needs to create more flexible and appropriate checks.
- Use these Expectations to track data as it progresses through your pipeline. Apply `ExpectColumnValuesToBeNull` to fields expected to be empty in early stages, and `ExpectColumnValuesToNotBeNull` to those same fields in later stages.
:::

## Managing types of missing data

### Intermittent missing values

Imagine you operate a transaction processing system. Due to a momentary server outage, `transfer_date` values may intermittently be missing. Ensuring that these values are populated at least 99.9% of the time can help maintain the reliability of your analyses.

#### GX solution
Ensure values are populated at least 99.9% of the time.

```python title="" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/missingness_resources/missingness_expectations.py intermittent_missing_values"
```

### Missing critical data

Consider an essential financial reporting system where the `transfer_amount` field is crucial for accurate analysis. This field might remain unpopulated due to an upstream system failure. Ensuring this field is always populated prevents significant discrepancies in reports.

#### GX solution
Ensure critical fields are always present.

```python title="" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/missingness_resources/missingness_expectations.py missing_critical_data"
```

### New cases appear

New events or cases might introduce `NULL` values in fields that previously had no missing data. For instance, the `last_visited` field, which should typically be populated, might now have `NULL` values for some users.

#### GX solution
Define an expectation to ensure the `type` field is populated for most entries.

```python title="" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/missingness_resources/missingness_expectations.py new_cases_appear"
```

### System anomalies

This use case differs significantly from the others as it reflects an expectation about the system being monitored rather than just data quality. Here, GX serves a dual role: ensuring data quality and performing a more classical alerting/observability function. For example, an increase in error rates might lead to the `errors` field being unexpectedly populated.

#### GX solution
Ensure fields that should typically be `NULL` remain unpopulated under normal circumstances.

```python title="" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/missingness_resources/missingness_expectations.py system_anomalies"
```

This Expectation serves as an early warning system for potential issues in your monitored environment. By alerting you when the `errors` field is unexpectedly populated, it allows for prompt investigation and resolution of underlying system problems, going beyond traditional data quality checks.

## Avoid common missingness pitfalls

- **Overlooking Edge Cases**: Rare scenarios of missing data can go unnoticed, leading to incomplete data quality checks. Regularly broaden your [ExpectColumnValuesToNotBeNull](#expect-column-values-to-not-be-null) coverage to cover such cases.
- **Inconsistent Definitions**: Differing definitions of 'missing' data across teams can lead to inconsistent handling. Standardize definitions and document them clearly across teams.
- **False Positives/Negatives**: Rigid thresholds can cause false positives or negatives. Use historical data for setting thresholds and consider the `mostly` attribute available for both [ExpectColumnValuesToNotBeNull](#expect-column-values-to-not-be-null) and [ExpectColumnValuesToBeNull](#expect-column-values-to-be-null).
- **Manual Handling**: Manually addressing missing data can introduce errors and is unsustainable for large datasets. Automate data quality checks and remediation.
- **Ignoring Root Causes**: Addressing symptoms without understanding the root causes can lead to recurring problems. Conduct root cause analyses and implement relevant Expectations early in your data stream.

## Next Steps: Implementing Missingness Strategies

Managing missing data is a critical component of ensuring data quality and reliability. By implementing the strategies explored in this guide, you can significantly enhance your data's integrity and trustworthiness. Here's how you can build upon these practices:

1. Incorporate the discussed Expectations into your existing data pipelines and projects.
2. Explore the [Expectation Gallery](https://greatexpectations.io/expectations/) to discover more detailed Expectations that can be tailored to your specific needs.
3. Expand your validation approach to cover [other aspects of data quality](/reference/learn/data_quality_use_cases/dq_use_cases_lp.md), such as data integrity, volume, and distribution.
4. Regularly review and iterate on your validation processes to maintain high data quality standards.

Remember that managing missing data is just one facet of a comprehensive data quality strategy. To build robust and reliable data pipelines, consider integrating various Expectations that address [multiple aspects of data quality](/reference/learn/data_quality_use_cases/dq_use_cases_lp.md). This holistic approach will help you proactively identify and address potential issues, reducing downstream errors and fostering a culture of data confidence within your organization.

By consistently applying these methodologies and expanding your validation practices, you can create a strong foundation for trustworthy data analysis and decision-making processes. Continue exploring our data quality series to learn more about other crucial aspects of maintaining high-quality data in your pipelines.
