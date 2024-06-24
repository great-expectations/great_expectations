---
sidebar_label: 'Schema'
title: 'Data Quality: Ensuring Schema Consistency with Great Expectations'
---

Data schema refers to the structural blueprint of a dataset, encompassing elements such as column
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

### Data Preview

Here's a glimpse of the sample dataset we'll use to demonstrate schema validation:

| type     | sender_account_number  | recipient_fullname | transfer_amount | transfer_date       |
|----------|------------------------|--------------------|-----------------|---------------------|
| domestic | 244084670977           | Jaxson Duke        | 9143.40         | 2024-05-01 01:12    |
| domestic | 954005011218           | Nelson O’Connell   | 3285.21         | 2024-05-01 05:08    |

This dataset includes columns like `sender_account_number`, `recipient_fullname`, `transfer_amount`, and `transfer_date`.

### Key Schema Expectations

Let's explore core Expectations for schema validation, delving into their practical applications and
nuances:

**1. `expect_column_to_exist`**:
Ensures the presence of a specified column in your dataset. This Expectation is foundational for
schema validation, verifying that critical columns are included, thus preventing data processing
errors due to missing fields.

```python
dataset.expect_column_to_exist("sender_account_number")
```

**Use Case**: Ideal during data ingestion or integration of multiple data sources to ensure that
essential fields are present before proceeding with downstream processing.

**GX Tip**: Implement this Expectation early in your data pipeline to catch missing columns as soon
as possible, minimizing downstream errors and rework.

**2. `expect_column_values_to_be_in_type_list`**:
Ensures that the values in a specified column are within a specified type list. This Expectation is
useful for columns with varied permissible types, such as mixed-type fields often found in legacy
databases.

```python
dataset.expect_column_values_to_be_in_type_list("account_type", ["int", "str"])
```

**Use Case**: Suitable for datasets transitioning from older systems where type consistency might
not be strictly enforced, aiding smooth data migration and validation.

**GX Tip**: Combine this Expectation with detailed logging to track which types are most frequently
encountered, aiding in eventual standardization efforts.

**3. `expect_column_values_to_be_of_type`**:
Validates that the values within a column are of a specific data type. This is more stringent
compared to the previous Expectation, suitable for scenarios needing strict type adherence.

```python
dataset.expect_column_values_to_be_of_type("transfer_amount", "float")
```

**Use Case**: Handling data transferred using formats that do not embed schema (e.g., CSV), where
apparent type changes can occur when new values appear.

**GX Tip**: Opt for `expect_column_values_to_be_of_type` when dealing with columns where
any type deviation could lead to significant processing errors or inaccuracies.

**4. `expect_table_column_count_to_equal`**:
Ensures the dataset has an exact number of columns. This precise Expectation is for datasets with a
fixed schema structure, providing a strong safeguard against unexpected changes.

```python
dataset.expect_table_column_count_to_equal(7)
```

**Use Case**: Perfect for regulatory reporting scenarios where the schema is strictly defined, and
any deviation can lead to compliance violations.

**GX Tip**: Periodically review and update this Expectation alongside any schema changes,
especially when new regulatory requirements emerge.

**5. `expect_table_columns_to_match_ordered_list`**:
Validates the exact order of columns. This is crucial when processing pipelines depend on a specific
column order, ensuring consistency and reliability.

```python
dataset.expect_table_columns_to_match_ordered_list([
    "sender_account_number", "recipient_account_number",
    "transfer_amount", "transfer_date"
])
```

**Use Case**: Particularly relevant when handling scenarios such as changes in the order in which
columns are computed during serialization.

**GX Tip**: Use `expect_table_columns_to_match_ordered_list` over
`expect_table_columns_to_match_set` when order matters, such as in scripts directly referencing column positions.

**6. `expect_table_columns_to_match_set`**:
Checks that the dataset contains specific columns, without regard to order. This Expectation offers
flexibility where column presence is more critical than their sequence.

```python
dataset.expect_table_columns_to_match_set([
    "sender_account_number", "recipient_account_number",
    "transfer_amount", "transfer_date"
])
```

**Use Case**: Useful for datasets that might undergo reordering during preprocessing; key for data
warehousing operations where column integrity is crucial, but order might vary.

**GX Tip**: Opt for `expect_table_columns_to_match_set` when integrating datasets from
various sources where column order might differ, but consistency in available data is required.

**7. `expect_table_column_count_to_be_between`**:
Validates that the number of columns falls within a specific range, offering flexibility for
datasets that can expand or contract within a known boundary.

```python
dataset.expect_table_column_count_to_be_between(min_value=5, max_value=7)
```

**Use Case**: Beneficial for evolving datasets where additional columns could be added over time,
but the general structure remains bounded within a predictable range.

**GX Tip**: Regularly review the allowed range as your dataset evolves, ensuring it aligns
with business requirements and anticipates potential future expansion.

### Integrating Schema Validation

Below is a tutorial that demonstrates schema validation of a Pandas DataFrame using GX.

Tutorial Process Overview:

1. **Prepare Sample Data**: Create a sample dataset for testing and validation.
2. **Connect to the Data**: Establish a connection to the sample data using Great Expectations.
3. **Define Expectations**: Set the rules for your data schema.
4. **Save Expectations**: Store these rules for future validation checks.
5. **Create Checkpoints**: Bundle your rules and specify where in the pipeline they need to be validated.
6. **Run Validation**: Execute the validation checks at the specified checkpoints.
7. **Review Results**: Inspect the outcomes and identify any issues.

**1. Prepare Sample Data**:

Create a sample dataset using Pandas for testing and validation purposes.

```python
import pandas as pd

data = [
    {'type': 'domestic',
     'sender_account_number': 244084670977,
     'recipient_fullname': 'Jaxson Duke',
     'recipient_address': '50 E 53rd St',
     'recipient_phone': '+1-566-675-5951x933',
     'recipient_bankname': 'M&T Bank',
     'recipient_account_number': 962975843020,
     'transfer_amount': 9143.40,
     'transfer_date': '2024-05-01 01:12'
    },
    {'type': 'domestic',
     'sender_account_number': 954005011218,
     'recipient_fullname': 'Nelson O’Connell',
     'recipient_address': '84 Cabrini Blvd',
     'recipient_phone': '001-817-645-5875x3579',
     'recipient_bankname': 'Wells Fargo Bank',
     'recipient_account_number': 434903524241,
     'transfer_amount': 3285.21,
     'transfer_date': '2024-05-01 05:08'
    },
# TODO: might need more rows
]


df = pd.DataFrame(data)
```

**2. Connect to the Data**:

Create a [Data Context](#) object using GX and connect to the sample data.

```python
import great_expectations as gx

context = gx.get_context()
data_source = context.sources.add_pandas(name="pandas dataframe")

# TODO
```

**3. Define Expectations**:
Expectations are assertions about your dataset. Utilize `.expect_*` methods to specify schema rules.
```python
# Examples of defining expectations
dataset.expect_column_to_exist("sender_account_number")
dataset.expect_column_values_to_be_of_type("transfer_amount", "float")

# TODO: add some Expectations that will fail
```

**4. Save Expectations**:
Once defined, save these as an **Expectation Suite** for future reference and validation.
```python
dataset.save_expectation_suite()
```

**5. Create Checkpoints**:
**Checkpoints** bundle your Expectations to validate your data at specific points within your pipeline. They encapsulate the process of executing validations.
```python
checkpoint = context.add_or_update_checkpoint(name="checkpoint", validator=dataset)
```

**6. Run Validation**:
Triggering a Checkpoint will run through the Expectations and validate your dataset against them.
```python
checkpoint_result = checkpoint.run()
```

**7. Review Results**:
After running the Checkpoint, you can inspect the validation outcomes and address any discrepancies.
```python
context.view_validation_result(checkpoint_result)
```


### Conclusion

Robust schema validation is fundamental to trustworthy data pipelines. Great Expectations empowers you to proactively define and enforce the structural integrity of your data, ensuring its reliability for critical analyses and decision-making processes. By consistently incorporating schema validation practices, you enhance data quality, reduce downstream errors, and foster a strong culture of data confidence within your organization.

Regularly review and update your Expectations as your data evolves, ensuring continued alignment with business requirements and data quality standards. Embrace a proactive, iterative approach to maintain high-quality data pipelines.
