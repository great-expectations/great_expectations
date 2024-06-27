---
sidebar_label: 'Schema'
title: 'Data quality: Ensuring schema consistency with Great Expectations'
---

**Data schema** refers to the structural blueprint of a dataset, encompassing elements such as column
names, data types, and the overall organization of information. When working with data, ensuring
that it adheres to its predefined schema is a critical aspect of data quality management. This
process, known as schema validation, is among the top priority use cases for data quality platforms.

Validating your data's schema is crucial for maintaining data reliability and usability in
downstream tasks. This process involves checking that the structure of your dataset conforms to
established rules, such as verifying column names, data types, and the presence of required fields.
Schema changes, whether planned or unexpected, can significantly impact data integrity and the
performance of data-dependent systems.

Great Expectations (GX) provides a powerful suite of schema-focused **Expectations** that allow you
to define and enforce the structural integrity of your datasets. These tools enable you to establish
robust schema validation within your data pipelines, helping to catch and address schema-related
issues before they propagate through your data ecosystem. This guide will walk you through
leveraging these Expectations to implement effective schema validation in your data workflows.

## Data Preview

Below is a sample of the dataset that is referenced by examples and explanations within this article.

| type     | sender_account_number  | recipient_fullname | transfer_amount | transfer_date       |
|----------|------------------------|--------------------|-----------------|---------------------|
| domestic | 244084670977           | Jaxson Duke        | 9143.40         | 2024-05-01 01:12    |
| domestic | 954005011218           | Nelson O’Connell   | 3285.21         | 2024-05-01 05:08    |

This dataset includes columns like `sender_account_number`, `recipient_fullname`, `transfer_amount`, and `transfer_date`.

## Key Schema Expectations

Explore core Expectations for schema validation, delving into their practical applications and nuances:

### `expect_column_to_exist`

Ensures the presence of a specified column in your dataset. This Expectation is foundational for
schema validation, verifying that critical columns are included, thus preventing data processing
errors due to missing fields.

```python
dataset.expect_column_to_exist("sender_account_number")
```

:::info[Use Case]
Ideal during data ingestion or integration of multiple data sources to ensure that
essential fields are present before proceeding with downstream processing.
:::

:::tip[GX Tip]
Implement this Expectation early in your data pipeline to catch missing columns as soon
as possible, minimizing downstream errors and rework.
:::


### `expect_column_values_to_be_in_type_list`

Ensures that the values in a specified column are within a specified type list. This Expectation is
useful for columns with varied permissible types, such as mixed-type fields often found in legacy
databases.

```python
dataset.expect_column_values_to_be_in_type_list("account_type", ["int", "str"])
```

:::info[Use Case]
Suitable for datasets transitioning from older systems where type consistency might
not be strictly enforced, aiding smooth data migration and validation.
:::

:::tip[GX Tip]
Combine this Expectation with detailed logging to track which types are most frequently
encountered, aiding in eventual standardization efforts.
:::

### `expect_column_values_to_be_of_type`

Validates that the values within a column are of a specific data type. This is more stringent
compared to the previous Expectation, suitable for scenarios needing strict type adherence.

```python
dataset.expect_column_values_to_be_of_type("transfer_amount", "float")
```

:::info[Use Case]
Handling data transferred using formats that do not embed schema (e.g., CSV), where
apparent type changes can occur when new values appear.
:::

:::tip[GX Tip]
Opt for `expect_column_values_to_be_of_type` when dealing with columns where
any type deviation could lead to significant processing errors or inaccuracies.
:::

### `expect_table_column_count_to_equal`

Ensures the dataset has an exact number of columns. This precise Expectation is for datasets with a
fixed schema structure, providing a strong safeguard against unexpected changes.

```python
dataset.expect_table_column_count_to_equal(7)
```

:::info[Use Case]
Perfect for regulatory reporting scenarios where the schema is strictly defined, and
any deviation can lead to compliance violations.
:::

:::tip[GX Tip]
Periodically review and update this Expectation alongside any schema changes,
especially when new regulatory requirements emerge.
:::

### `expect_table_columns_to_match_ordered_list`

Validates the exact order of columns. This is crucial when processing pipelines depend on a specific
column order, ensuring consistency and reliability.

```python
dataset.expect_table_columns_to_match_ordered_list([
    "sender_account_number", "recipient_account_number",
    "transfer_amount", "transfer_date"
])
```

:::info[Use Case]
Particularly relevant when handling scenarios such as changes in the order in which
columns are computed during serialization.
:::

:::tip[GX Tip]
Use `expect_table_columns_to_match_ordered_list` over
`expect_table_columns_to_match_set` when order matters, such as in scripts directly referencing column positions.
:::

### `expect_table_columns_to_match_set`

Checks that the dataset contains specific columns, without regard to order. This Expectation offers
flexibility where column presence is more critical than their sequence.

```python
dataset.expect_table_columns_to_match_set([
    "sender_account_number", "recipient_account_number",
    "transfer_amount", "transfer_date"
])
```

:::info[Use Case]
Useful for datasets that might undergo reordering during preprocessing; key for data
warehousing operations where column integrity is crucial, but order might vary.
:::

:::tip[GX Tip]
Opt for `expect_table_columns_to_match_set` when integrating datasets from
various sources where column order might differ, but consistency in available data is required.
:::

### `expect_table_column_count_to_be_between`

Validates that the number of columns falls within a specific range, offering flexibility for
datasets that can expand or contract within a known boundary.

```python
dataset.expect_table_column_count_to_be_between(min_value=5, max_value=7)
```

:::info[Use Case]
Beneficial for evolving datasets where additional columns could be added over time,
but the general structure remains bounded within a predictable range.
:::

:::tip[GX Tip]
Regularly review the allowed range as your dataset evolves, ensuring it aligns
with business requirements and anticipates potential future expansion.
:::

## Examples & Scenarios

-- TODO: these are mostly pseudo code ----

### Comparative Analysis: Ensuring Schema Consistency in Financial Transfers

**Context**: In financial transfers, adhering to a fixed schema is paramount for regulatory compliance and operational accuracy. Ensuring that all necessary columns are present and correctly typed can prevent significant operational disruptions.

**Goal**: Validate two datasets to ensure the presence of specific columns and correct column count.

```python
import pandas as pd
import great_expectations as gx

# Sample datasets
data_1 = [
    {'type': 'domestic', 'sender_account_number': '244084670977', 'recipient_fullname': 'Jaxson Duke', 'transfer_amount': 9143.40, 'transfer_date': '2024-05-01 01:12'},
    {'type': 'domestic', 'sender_account_number': '954005011218', 'recipient_fullname': 'Nelson O’Connell', 'transfer_amount': 3285.21, 'transfer_date': '2024-05-01 05:08'}
]
data_2 = [
    {'type': 'domestic', 'sender_account_number': '842374923847', 'recipient_fullname': 'Alex Smith', 'transfer_amount': 5783.18, 'transfer_date': '2024-04-12 15:35'},
    # Missing 'recipient_fullname'
    {'type': 'domestic', 'sender_account_number': '673894027340', 'transfer_amount': 8493.14, 'transfer_date': '2024-04-21 09:50'}
]

df1 = pd.DataFrame(data_1)
df2 = pd.DataFrame(data_2)

context = gx.get_context()

expectation_suite_name = "schema_comparison_suite"
context.create_expectation_suite(expectation_suite_name)

validator_1 = context.get_validator(df1, expectation_suite_name=expectation_suite_name)
validator_2 = context.get_validator(df2, expectation_suite_name=expectation_suite_name)

# Define Expectations
validator_1.expect_column_to_exist("recipient_fullname")
validator_2.expect_column_to_exist("recipient_fullname")

validator_1.expect_table_column_count_to_equal(5)
validator_2.expect_table_column_count_to_equal(5)

# Run validation
result_1 = validator_1.validate()
result_2 = validator_2.validate()

print("Validation Result 1:", result_1)
print("Validation Result 2:", result_2)
```

**Insight**: Dataset 2 fails to validate due to the absence of `recipient_fullname` in one of the rows and the correct column count, highlighting how missing critical columns can disrupt financial processing or lead to compliance issues.

### Different Expectation Suites: Strict vs. Relaxed Type Checking

**Context**: In some contexts, both the names and order of columns can be critically important. Using different suites to enforce these aspects can help maintain consistency.

**Goal**: Validate datasets to ensure columns appear in the correct order and all required columns are present.

```python
# First Expectation Suite: strict column order
expectation_suite_name_1 = "schema_ordered_columns"
suite_1 = context.create_expectation_suite(expectation_suite_name_1)
validator_1 = context.get_validator(df1, expectation_suite_name=expectation_suite_name_1)
validator_1.expect_table_columns_to_match_ordered_list([
    "type", "sender_account_number", "recipient_fullname", "transfer_amount", "transfer_date"
])

# Second Expectation Suite: relaxed column order
expectation_suite_name_2 = "schema_unordered_columns"
suite_2 = context.create_expectation_suite(expectation_suite_name_2)
validator_2 = context.get_validator(df2, expectation_suite_name=expectation_suite_name_2)
validator_2.expect_table_columns_to_match_set([
    "type", "sender_account_number", "recipient_fullname", "transfer_amount", "transfer_date"
])

# Run validation
result_ordered = validator_1.validate()
result_unordered = validator_2.validate()

print("Ordered Columns Validation Result:", result_ordered)
print("Unordered Columns Validation Result:", result_unordered)
```

**Insight**: The strict suite ensures that columns appear in the specified order, crucial in contexts where order matters for processing logic, while the relaxed suite allows flexibility but ensures all required columns are present.

## Community Best Practices

### Common Pitfalls and How to Avoid Them

- **Inconsistent Data Types**: Inconsistencies in data types can arise when data is ingested from diverse sources or when schema definitions are updated without comprehensive checks. These inconsistencies often lead to processing errors, making analyses unreliable. Regular monitoring of data ingestion points and strict enforcement of type consistency through your data validation framework can mitigate these issues.

- **Schema Evolution**: Evolving business requirements often necessitate changes in data schemas, which if not managed correctly, can lead to significant disruptions. Schema changes can break data pipelines and lead to data compatibility issues. Implementing a structured process for schema versioning and maintaining backward compatibility can help ensure that these changes are less disruptive.

- **Relying Solely on Schema Validation**: One common pitfall is the over-reliance on schema validation as the sole mechanism for data quality assurance. While schema validation ensures structural integrity, it does not account for semantic correctness. To achieve comprehensive data quality, combine schema validation with semantic checks at the field level, such as validating value ranges, patterns, and relationships between fields.

- **Logging and Monitoring**: Even the best validation setup can fail without proper logging and monitoring. Undetected schema validation failures can propagate through the data pipeline unnoticed, leading to broader issues. Detailed logging and real-time monitoring are essential to create an audit trail and enable quick detection and resolution of schema validation problems, maintaining the integrity of your data pipelines.

**Additional Resources on the Web**:

- [Data Quality with Great Expectations](https://astrafy.io/the-hub/blog/technical/data-quality-with-great-expectations)
- [AWS Well-Architected: Validate the data quality of source systems before transferring data for analytics](https://docs.aws.amazon.com/wellarchitected/latest/analytics-lens/best-practice-1.1---validate-the-data-quality-of-source-systems-before-transferring-data-for-analytics..html)

## The Path Forward

Robust schema validation is fundamental to trustworthy data pipelines. Great Expectations empowers you to proactively define and enforce the structural integrity of your data, ensuring its reliability for critical analyses and decision-making processes. By consistently incorporating schema validation practices, you enhance data quality, reduce downstream errors, and foster a strong culture of data confidence within your organization.

However, schema validation is just one aspect of a comprehensive data quality strategy. Achieving high-quality data requires a multifaceted approach involving:

1. **Data Integrity**: Ensures that data remains accurate and consistent over its lifecycle by validating key constraints.
2. **Missing Data**: Identifies and validates gaps in datasets to [maintain completeness](../missingness) and usability.
3. **Patterns and Formats**: Ensures that data values adhere to expected formats or patterns for consistency.
4. **Cardinality and Membership**: Ensures that columns maintain correct unique counts and that values belong to a specified set.
5. **Data Volume**: Monitors and validates the number of records to ensure they fall within expected bounds.
6. **Numerical Data**: Validates numerical data properties and distributions to detect anomalies.

To effectively handle these dimensions, consider integrating various Expectations to cover these broader data quality aspects. Regular validation, monitoring, and iterations are key to maintaining high standards.
