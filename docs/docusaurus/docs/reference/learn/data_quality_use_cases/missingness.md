---
sidebar_label: 'Missingness'
title: 'Manage missing data with GX'
---

Missing data, or **missingness**, is a critical issue in data quality management. It refers to the absence of expected information in a dataset, often appearing as NULL values in databases or manifesting differently across systems.

Effectively managing missing data is crucial for maintaining data integrity and reliability. This process involves identifying gaps, understanding their causes, and determining appropriate remedial actions. Unaddressed missing data can disrupt calculations, skew analyses, and compromise various data analytics tasks.

Great Expectations (GX) provides a suite of powerful Expectations that allow you to define and enforce data quality rules, including those for missing data. These tools enable you to establish robust validation processes within your data pipelines, catching issues before they propagate through your data ecosystem.

This guide will walk you through leveraging GX to handle missing data effectively. You'll learn to write Expectations, set up automated checks, and make informed decisions about managing various types of missingness. By following these steps, you'll ensure your datasets remain clean, consistent, and ready for accurate reporting, predictive modeling, and other advanced analytics applications.

## Prerequisite knowledge

Before diving into this guide, ensure you have a basic understanding of GX components and workflows. If you're new to GX, consider starting with the [GX Overview](/core/introduction/gx_overview.md) to familiarize yourself with key concepts and setup procedures.

## Data preview

Below is a sample of the dataset referenced throughout this guide:

| type     | sender_account_number  | recipient_fullname | transfer_amount | transfer_date       | errors |
|----------|------------------------|--------------------|-----------------|---------------------|--------|
| domestic | 244084670977           | Jaxson Duke        | 9143.40         | 2024-05-01 01:12    | NULL   |
| NULL     | 123456789012           | Jane Smith         | 5000.00         | NULL                | NULL   |

This dataset may have missing data in the `type` and `transfer_date` columns. The `errors` column is assumed to be populated by a separate monitoring system, not by GX. It shows any issues found during data processing. A `NULL` value in the `errors` column means no errors were detected for that record.

You can [access this dataset](https://raw.githubusercontent.com/great-expectations/great_expectations/develop/tests/test_sets/learn_data_quality_use_cases/missingness.csv) from the `great_expectations` GitHub repo in order to reproduce the code recipes provided in this article.


## Key missingness Expectations

GX provides a suite of missingness-focused Expectations to manage missing data in your datasets. These Expectations can be added to an Expectation Suite via the GX Cloud UI or using the GX Core Python library.


:::[TODO]
> Add animated GIF demonstrating how to create Expectations using the GX Cloud UI.
:::

### Expect Column Values To Be Null

Ensures that values within a column are `NULL`.

**Use Case**: Handle columns that should typically be populated but might be left null due to specific conditions.

```python title="" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/missingness_resources/missingness_expectations.py ExpectColumnValuesToBeNull"
gxe.ExpectColumnValuesToBeNull('errors')
```

<sup>View `ExpectColumnValuesToBeNull` in the [Expectation Gallery](https://greatexpectations.io/expectations/expect_column_values_to_be_null).</sup>

### Expect Column Values To Not Be Null

Ensures that values within a specific column are not `NULL`.

**Use Case**: Ensure critical columns are consistently populated, such as `transfer_amount`.

```python title="Python" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/missingness_resources/missing_expectations.py ExpectColumnValuesToNotBeNull"
gxe.ExpectColumnValuesToNotBeNull('transfer_amount')
```

<sup>View `ExpectColumnValuesToNotBeNull` in the [Expectation Gallery](https://greatexpectations.io/expectations/expect_column_values_to_not_be_null).</sup>

<br/>
<br/>

:::tip[GX tips for missingness Expectations]
- Set different tolerance levels for `ExpectColumnValuesToBeNull` and `ExpectColumnValuesToNotBeNull`. This allows for nuanced data quality checks that align with your specific data patterns and business needs. For example, you might allow up to 20% nulls in a 'customer_feedback' column, but want to be alerted if more than 1% of values are unexpectedly non-null in an 'errors' column.
- Use these Expectations to track data as it progresses through your pipeline. Apply `ExpectColumnValuesToBeNull` to fields expected to be empty in early stages, and `ExpectColumnValuesToNotBeNull` to those same fields in later stages.
:::

## Managing types of missing data

### Intermittent missing values

Imagine you operate a transaction processing system. Due to a momentary server outage, `transfer_date` values may intermittently be missing. Ensuring that these values are populated at least 99.9% of the time can help maintain the reliability of your analyses.

#### GX solution
Ensure values are populated at least 99.9% of the time.

```python title="" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/missingness_resources/missingness_expectations.py intermittent_missing_values"
gxe.ExpectColumnValuesToNotBeNull('transfer_date', mostly=0.999)
```

### Missing critical data

Consider an essential financial reporting system where the `transfer_amount` field is crucial for accurate analysis. This field might remain unpopulated due to an upstream system failure. Ensuring this field is always populated prevents significant discrepancies in reports.

#### GX solution
Ensure critical fields are always present.

```python title="" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/missingness_resources/missingness_expectations.py missing_critical_data"
gxe.ExpectColumnValuesToNotBeNull('transfer_amount')
```

### New cases appear

New events or cases might introduce `NULL` values in fields that previously had no missing data. For instance, the `last_visited` field, which should typically be populated, might now have `NULL` values for some users.

#### GX solution
Define an expectation to ensure the `type` field is populated for most entries.

```python title="" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/missingness_resources/missingness_expectations.py new_cases_appear"
gxe.ExpectColumnValuesToNotBeNull(
    'type',
    mostly=0.95  # Allows for up to 5% of values to be NULL
)
```

### System anomalies

This use case differs significantly from the others as it reflects an expectation about the system being monitored rather than just data quality. Here, GX serves a dual role: ensuring data quality and performing a more classical alerting/observability function. For example, an increase in error rates might lead to the `errors` field being unexpectedly populated.

#### GX solution
Ensure fields that should typically be `NULL` remain unpopulated under normal circumstances.

```python title="" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/missingness_resources/missingness_expectations.py system_anomalies"
gxe.ExpectColumnValuesToBeNull('errors')
```

This Expectation serves as an early warning system for potential issues in your monitored environment. By alerting you when the `errors` field is unexpectedly populated, it allows for prompt investigation and resolution of underlying system problems, going beyond traditional data quality checks.

## Avoid common missingness pitfalls

- **Overlooking Edge Cases**: Rare scenarios of missing data can go unnoticed, leading to incomplete data quality checks. Regularly broaden your [ExpectColumnValuesToNotBeNull](#expect-column-values-to-not-be-null) coverage to cover such cases.
- **Inconsistent Definitions**: Differing definitions of 'missing' data across teams can lead to inconsistent handling. Standardize definitions and document them clearly across teams.
- **False Positives/Negatives**: Rigid thresholds can cause false positives or negatives. Use historical data for setting thresholds and consider the `mostly` attribute available for both [ExpectColumnValuesToNotBeNull](#expect-column-values-to-not-be-null) and [ExpectColumnValuesToBeNull](#expect-column-values-to-be-null).
- **Manual Handling**: Manually addressing missing data can introduce errors and is unsustainable for large datasets. Automate data quality checks and remediation.
- **Ignoring Root Causes**: Addressing symptoms without understanding the root causes can lead to recurring problems. Conduct root cause analyses and implement relevant Expectations early in your data stream.

## Next Steps: Implementing Missingness Strategies

Through this guide, you explored effective strategies for managing missing data with Great Expectations. Here's how you can take these practices forward:

1. Integrate the discussed Expectations into your current projects.
2. Use the [Expectation Gallery](https://greatexpectations.io/expectations/) to explore more detailed Expectations and tailor them to your needs.
3. Join the [GX Community](https://discourse.greatexpectations.io/c/share/18) to exchange experiences and solutions with fellow developers.
4. Proceed with our comprehensive [GX data quality series](#) for deeper insights and advanced techniques.

:::[TODO]
> Update link above
:::

We'd love to hear how you are using Great Expectations to handle missing data in your projects. Share your stories, questions, or tips in the [GX Community](https://discourse.greatexpectations.io/c/share/18), and let's continue building a robust and supportive data quality ecosystem together.

By embedding these methodologies in your workflow, you can significantly bolster your data's integrity, fostering enhanced reliability and trustworthiness.
