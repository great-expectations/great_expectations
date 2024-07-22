---
sidebar_label: 'Missingness'
title: 'Manage missing data with GX'
---


**Missing data**, or **missingness**, refers to the absence of expected information within a dataset. These absences can be represented by `NULL` values in SQL databases or appear differently in various systems.

Maintaining the integrity of your data involves effectively managing missing data points. Missing data can interfere with calculations, skew reports, and produce inaccurate outputs in machine learning models. Thus, identifying and resolving missing data is paramount to ensure data reliability and utility.

Managing missing data involves:

1. Identifying where data is missing
2. Understanding the causes of the missing data
3. Deciding on appropriate remedial actions

Great Expectations (GX) offers robust tools for managing missing data. By leveraging GX Expectations, you can define rules to:

- Detect instances where data should not be missing
- Ensure the proportion of missing data remains within acceptable limits
- Analyze patterns in data missingness

In this guide, we will walk you through utilizing Great Expectations to proficiently handle missing data, ensuring your datasets remain clean, consistent, and ready for analysis or machine learning applications. You will learn step-by-step methods for writing Expectations, setting up automated checks, and making informed decisions about managing various types of missing data.

## Prerequisite knowledge

This guide assumes a basic familiarity with GX components and workflows. Refer to the [GX Overview](/core/introduction/gx_overview.md) for foundational concepts.

## Data preview

Below is a sample of the dataset referenced throughout this guide:

| type     | sender_account_number  | recipient_fullname | transfer_amount | transfer_date       | errors |
|----------|------------------------|--------------------|-----------------|---------------------|--------|
| domestic | 244084670977           | Jaxson Duke        | 9143.40         | 2024-05-01 01:12    | NULL   |
| NULL     | 123456789012           | Jane Smith         | 5000.00         | NULL                | NULL   |

This dataset includes potential for missing data in `type` and `transfer_date` columns.

You can [access this dataset](https://raw.githubusercontent.com/great-expectations/great_expectations/develop/tests/test_sets/learn_data_quality_use_cases/missingness.csv) from the `great_expectations` GitHub repo in order to reproduce the code recipes provided in this article.


## Key missingness Expectations

GX provides a suite of missingness-focused Expectations to manage missing data in your datasets. These Expectations can be added to an Expectation Suite via the GX Cloud UI or using the GX Core Python library.


:::[TODO]
> add animated gif
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

## Managing types of missing data

### Intermittent missing values

Certain values may intermittently be missing, often due to system or network issues.

#### Example problem scenarios
- A temporary outage in the transaction processing system resulting in missing `transfer_date` data.

#### GX solution
Ensure values are populated at least 99.9% of the time.

```python title="" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/missingness_resources/missingness_expectations.py intermittent_missing_values"
gxe.ExpectColumnValuesToNotBeNull('transfer_date', mostly=0.999)
```

### Missing critical data

Critical data fields might be missing due to upstream issues or changes in data collection processes.

#### Example Problem Scenarios
- An important field, like `transfer_amount`, was not populated due to a temporary upstream system outage.

#### GX Solution
Ensure critical fields are always present.

```python title="" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/missingness_resources/missingness_expectations.py missing_critical_data"
gxe.ExpectColumnValuesToNotBeNull('transfer_amount')
```

### New cases appear

New events or cases might introduce `NULL` values in fields that previously had no missing data.

#### Example problem scenarios
- The `last_visited` field, which should typically be populated, now has `NULL` values for some users.

#### GX solution
Define an expectation to ensure the `type` field is populated for most entries.

```python title="" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/missingness_resources/missingness_expectations.py new_cases_appear"
gxe.ExpectColumnValuesToNotBeNull(
    'type',
    mostly=0.95  # Allows for up to 5% of values to be NULL
)
```

### System anomalies

This use case differs significantly from the others as it reflects an expectation about the system being monitored rather than just data quality. Here, GX serves a dual role: ensuring data quality and performing a more classical alerting/observability function.

#### Example problem scenarios
- An increase in error rates leads to the `errors` field being unexpectedly populated.

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

## Next steps: implementing missingness strategies

In this guide, we have explored methods for managing missing data using Great Expectations. From detecting unexpected `NULL` values to handling seasonal anomalies, GX provides comprehensive tools to ensure data quality. Apply the Expectations discussed here to build cleaner, more reliable datasets.

### Further learning

We encourage you to integrate these practices into your data quality workflows:

- Explore more detailed expectations in the [Expectation Gallery](https://greatexpectations.io/expectations/)
- Join the [GX Community](https://discourse.greatexpectations.io/c/share/18) for discussions and shared experiences.
- Continue to the [GX series on data quality](#) for other insights.

By consistently applying these practices, you will enhance the integrity and reliability of your data, fostering greater confidence in your datasets.
