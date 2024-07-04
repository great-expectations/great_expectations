---
sidebar_label: 'Schema'
title: 'Ensuring schema consistency with Great Expectations'
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

## Data preview

This example uses the `yellow_tripdata` dataset that comes preinstalled within the Great Expectations
`greatexpectations/demo:2024-04-11` docker container.

This dataset includes columns like `VendorID`, `total_amount`, `payment_type`, and `tpep_pickup_datetime`.

## Key schema Expectations

Explore core Expectations for schema validation, delving into their practical applications and nuances:

### Expect Column to Exist

Ensures the presence of a specified column in your dataset. This Expectation is foundational for
schema validation, verifying that critical columns are included, thus preventing data processing
errors due to missing fields. ([Link to Expectation
Gallery](https://greatexpectations.io/expectations/expect_column_to_exist))

```python
expectation = gxe.ExpectColumnToExist(column="VendorID")
```


:::info[Use Case]
Ideal during data ingestion or integration of multiple data sources to ensure that
essential fields are present before proceeding with downstream processing.
:::

:::tip[GX Tip]
Implement this Expectation early in your data pipeline to catch missing columns as soon
as possible, minimizing downstream errors and rework.
:::


### Expect Column Values to be in Type List

Ensures that the values in a specified column are within a specified type list. This Expectation is
useful for columns with varied permissible types, such as mixed-type fields often found in legacy
databases. ([Link to Expectation
Gallery](https://greatexpectations.io/expectations/expect_column_values_to_be_in_type_list))

```python
expectation = gxe.ExpectColumnValuesToBeInTypeList(column="payment_type", type_list=["DOUBLE_PRECISION", "STRING"])
```

:::info[Use Case]
Suitable for datasets transitioning from older systems where type consistency might
not be strictly enforced, aiding smooth data migration and validation.
:::

:::tip[GX Tip]
Combine this Expectation with detailed logging to track which types are most frequently
encountered, aiding in eventual standardization efforts.
:::

### Expect Column Values to be of Type

Validates that the values within a column are of a specific data type. This is more stringent
compared to the previous Expectation, suitable for scenarios needing strict type adherence. ([Link
to Expectation Gallery](https://greatexpectations.io/expectations/expect_column_values_to_be_of_type))

```python
expectation = gxe.ExpectColumnValuesToBeOfType(column="total_amount", type_="DOUBLE_PRECISION")
```

:::info[Use Case]
Handling data transferred using formats that do not embed schema (e.g., CSV), where
apparent type changes can occur when new values appear.
:::

:::tip[GX Tip]
Opt for `ExpectColumnValuesToBeOfType` when dealing with columns where
any type deviation could lead to significant processing errors or inaccuracies.
:::

### Expect Table Column Count to Equal

Ensures the dataset has an exact number of columns. This precise Expectation is for datasets with a
fixed schema structure, providing a strong safeguard against unexpected changes. ([Link
to Expectation Gallery](https://greatexpectations.io/expectations/expect_table_column_count_to_equal))

```python
expectation = gxe.ExpectTableColumnCountToEqual(value=19)
```

:::info[Use Case]
Perfect for regulatory reporting scenarios where the schema is strictly defined, and
any deviation can lead to compliance violations.
:::

:::tip[GX Tip]
Periodically review and update this Expectation alongside any schema changes,
especially when new regulatory requirements emerge.
:::

### Expect Table Columns to Match Ordered List

Validates the exact order of columns. This is crucial when processing pipelines depend on a specific
column order, ensuring consistency and reliability. ([Link
to Expectation Gallery](https://greatexpectations.io/expectations/expect_table_columns_to_match_ordered_list))

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

### Expect Table Columns to Match Set

Checks that the dataset contains specific columns, without regard to order. This Expectation offers
flexibility where column presence is more critical than their sequence. ([Link
to Expectation Gallery](https://greatexpectations.io/expectations/expect_table_columns_to_match_set))

```python
expectation = gxe.ExpectTableColumnsToMatchSet(column_set=["VendorID", "payment_type", "total_amount"], exact_match=False)
```

:::info[Use Case]
Useful for datasets that might undergo reordering during preprocessing; key for data
warehousing operations where column integrity is crucial, but order might vary.
:::

:::tip[GX Tip]
Opt for `ExpectTableColumnsToMatchSet` when integrating datasets from
various sources where column order might differ, but consistency in available data is required.
:::

### Expect Table Column Count to be Between

Validates that the number of columns falls within a specific range, offering flexibility for
datasets that can expand or contract within a known boundary. ([Link
to Expectation Gallery](https://greatexpectations.io/expectations/expect_table_column_count_to_be_between))

```python
expectation = gxe.ExpectTableColumnCountToBeBetween(min_value=1, max_value=20)
```

:::info[Use Case]
Beneficial for evolving datasets where additional columns could be added over time,
but the general structure remains bounded within a predictable range.
:::

:::tip[GX Tip]
Regularly review the allowed range as your dataset evolves, ensuring it aligns
with business requirements and anticipates potential future expansion.
:::

## Examples and scenarios

### Comparative Analysis: Ensuring Schema Consistency in Travel information

### Comparative analysis: Ensuring schema consistency in financial transfers

**Goal**: Validate example datasets to ensure the presence of specific columns and correct column count.

```python
import great_expectations as gx
import great_expectations.exceptions as exceptions
import great_expectations.expectations as gxe

from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.datasource.fluent.interfaces import Datasource

from constants import (
    DB_CONNECTION_STRING,
    TABLE_NAME,
    DATASOURCE_NAME,
    ASSET_NAME,
    SUITE_NAME,
    BATCH_DEFINITION_NAME_WHOLE_TABLE
)

context = gx.get_context(mode="file")

try:
    datasource = context.sources.add_postgres(DATASOURCE_NAME, connection_string=DB_CONNECTION_STRING)
    data_asset = datasource.add_table_asset(name=ASSET_NAME, table_name=TABLE_NAME)
    batch_definition = data_asset.add_batch_definition_whole_table(BATCH_DEFINITION_NAME_WHOLE_TABLE)

except exceptions.DataContextError:
    datasource = context.get_datasource(DATASOURCE_NAME)
    assert isinstance(datasource, Datasource)
    data_asset = datasource.get_asset(asset_name=ASSET_NAME)
    batch_definition = next(bd for bd in data_asset.batch_definitions if bd.name == BATCH_DEFINITION_NAME_WHOLE_TABLE)

expectation = gxe.ExpectColumnValuesToNotBeInSet(column="total_amount", value_set=[0])
batch = batch_definition.get_batch()
result = batch.validate(expectation)
print(result)

expectation = gxe.ExpectTableColumnCountToEqual(value=17)
result = batch.validate(expectation)
print(result)
```

**Insight**: Dataset fails to validate due to `total_amount` being equal to zero in one of the rows and the column count
not matching the previous version after two new columns where added during a database migration. This highlights how missing
critical columns can disrupt processing or lead to compliance issues.

### Different Expectation suites: Strict vs. relaxed type checking

**Context**: In some contexts, both the names and order of columns can be critically important. Using different suites to enforce these aspects can help maintain consistency.

**Goal**: Validate datasets to ensure columns appear in the correct order and all required columns are present.

```python
# First Expectation Suite: strict column order
try:
    suite = context.suites.add(ExpectationSuite(name="STRICT COLUMN ORDER"))

    expectation = suite.add_expectation(
        gxe.ExpectTableColumnsToMatchSet(column_set=[
            "index", "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "passenger_count", "trip_distance", "RatecodeID", "store_and_fwd_flag",
            "PULocationID", "DOLocationID", "payment_type", "fare_amount", "extra",
            "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
            "total_amount", "congestion_surcharge"
            ]))

except exceptions.DataContextError:
    suite = context.suites.get("STRICT COLUMN ORDER")

# Second Expectation Suite: relaxed column order
try:
    suite = context.suites.add(ExpectationSuite(name="RELAXED COLUMN ORDER"))

    expectation = suite.add_expectation(
        gxe.ExpectTableColumnsToMatchSet(column_set=["index", "VendorID", "total_amount"], exact_match=False))

except exceptions.DataContextError:
    suite = context.suites.get("RELAXED COLUMN ORDER")

# Run validation
results = batch.validate(suite)
print(results)
```

**Insight**: The strict suite ensures that columns appear in the specified order, crucial in contexts where order matters for processing logic, while the relaxed suite allows flexibility but ensures all required columns are present.

## Community best practices

### Common pitfalls and how to avoid them

- **Inconsistent Data Types**: Inconsistencies in data types can arise when data is ingested from diverse sources or when schema definitions are updated without comprehensive checks. These inconsistencies often lead to processing errors, making analyses unreliable. Regular monitoring of data ingestion points and strict enforcement of type consistency through your data validation framework can mitigate these issues.

- **Schema Evolution**: Evolving business requirements often necessitate changes in data schemas, which if not managed correctly, can lead to significant disruptions. Schema changes can break data pipelines and lead to data compatibility issues. Implementing a structured process for schema versioning and maintaining backward compatibility can help ensure that these changes are less disruptive.

- **Relying Solely on Schema Validation**: One common pitfall is the over-reliance on schema validation as the sole mechanism for data quality assurance. While schema validation ensures structural integrity, it does not account for semantic correctness. To achieve comprehensive data quality, combine schema validation with semantic checks at the field level, such as validating value ranges, patterns, and relationships between fields.

- **Logging and Monitoring**: Even the best validation setup can fail without proper logging and monitoring. Undetected schema validation failures can propagate through the data pipeline unnoticed, leading to broader issues. Detailed logging and real-time monitoring are essential to create an audit trail and enable quick detection and resolution of schema validation problems, maintaining the integrity of your data pipelines.

### Additional resources on the web

- [Data Quality with Great Expectations](https://astrafy.io/the-hub/blog/technical/data-quality-with-great-expectations)
- [AWS Well-Architected: Validate the data quality of source systems before transferring data for analytics](https://docs.aws.amazon.com/wellarchitected/latest/analytics-lens/best-practice-1.1---validate-the-data-quality-of-source-systems-before-transferring-data-for-analytics..html)

## The path forward

Robust schema validation is fundamental to trustworthy data pipelines. Great Expectations empowers you to proactively define and enforce the structural integrity of your data, ensuring its reliability for critical analyses and decision-making processes. By consistently incorporating schema validation practices, you enhance data quality, reduce downstream errors, and foster a strong culture of data confidence within your organization.

However, schema validation is just one aspect of a comprehensive data quality strategy. Achieving high-quality data requires a multifaceted approach involving:

1. **Data Integrity**: Ensures that data remains accurate and consistent over its lifecycle by validating key constraints.
2. **Missing Data**: Identifies and validates gaps in datasets to [maintain completeness](/reference/learn/data_quality_use_cases/missingness.md) and usability.
3. **Patterns and Formats**: Ensures that data values adhere to expected formats or patterns for consistency.
4. **Cardinality and Membership**: Ensures that columns maintain correct unique counts and that values belong to a specified set.
5. **Data Volume**: Monitors and validates the number of records to ensure they fall within expected bounds.
6. **Numerical Data**: Validates numerical data properties and distributions to detect anomalies.

To effectively handle these dimensions, consider integrating various Expectations to cover these broader data quality aspects. Regular validation, monitoring, and iterations are key to maintaining high standards.
