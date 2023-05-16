---
title: How to create and edit Expectations with the User Configurable Profiler
---

import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

Create a new <TechnicalTag tag="expectation_suite" text="Expectation Suite" /> by profiling your data with the User Configurable <TechnicalTag tag="profiler" text="Profiler" />.

## Prerequisites

- [A Great Expectations instance](/docs/guides/setup/setup_overview)
- Completion of the [Quickstart](tutorials/quickstart/quickstart.md)
- [A configured Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_quickly_instantiate_a_data_context)
- [A configured Datasource](/docs/guides/connecting_to_your_data/connect_to_data_overview)


:::note

The User Configurable Profiler simplifies the creation of a new Expectation Suite by creating multiple <TechnicalTag tag="expectation" text="Expectations" /> for your data.

The Profiler generates more expectations than you typically need. For example, if your table has 10,000 rows, the Profiler produces an Expectation with the following configuration:

```json
{
      "expectation_type": "expect_table_row_count_to_be_between",
      "kwargs": {
        "min_value": 10000,
        "max_value": 10000
      },
      "meta": {}
    }
```

The intention is that you'll edit and update the Expectation Suite to better suit your specific use case. The Expectation Suite is not intended to be used without changes.

:::

## 1. Load or create your Data Context

In this guide you'll use an on-disk data context with a pandas <TechnicalTag tag="datasource" text="Datasource" /> and a CSV data asset. If you don't have one, run the following command to create one:

```python name="tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler create_asset"
```

If a <TechnicalTag tag="datasource" text="Datasource" /> and data asset already exist, run the following command to load an on-disk <TechnicalTag tag="data_context" text="Data Context" />:

```python name="tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler get_asset"
```

## 2. Set your expectation_suite_name and create your Batch Request

Run the following command to identify the <TechnicalTag tag="batch" text="Batch" /> of data you want to <TechnicalTag tag="profiling" text="Profile" /> to create your Expectation Suite:

```python name="tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler name_suite"
```

## 3. Instantiate your Validator

Run the following command to use a <TechnicalTag tag="validator" text="Validator" /> to access and interact with your data:

```python name="tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler create_validator"
```

Run `validator.head()` to confirm that it contains the expected data.

## 4. Instantiate a UserConfigurableProfiler

Run the following command to instantiate a UserConfigurableProfiler and pass in the Validator with your data:

```python name="tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler create_profiler"
```

## 5. Use the profiler to build a suite

Run the following command to print a list of all the Expectations created by column, and return the Expectation Suite object:

```python name="tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler build_suite"
```

## 6. Running validation, saving your suite, and building Data Docs (Optional)

Run the following command to <TechnicalTag tag="validation" text="Validate" /> your data with the new Expectation Suite, save your Expectation Suite, and build the <TechnicalTag tag="data_docs" text="Data Docs" />:

```python name="tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler e2e"
```

## Optional Parameters

The following are the optional parameters for UserConfigurableProfiler:

- **excluded_expectations**: List\[str\] - Specifies the Expectation types to exclude from the Expectation Suite.
- **ignored_columns**: List\[str\] - Columns to ignore when building Expectations. For example, metadata columns are not the same ind different tables.
- **not_null_only**: Bool - By default, each column is evaluated for nullity. If the column values contain fewer than 50% null values, then the Profiler adds `expect_column_values_to_not_be_null`; if greater than 50% it adds `expect_column_values_to_be_null`. If `not_null_only` is set to True, the Profiler adds a `not_null` Expectation irrespective of the percent nullity. For this reason, an `expect_column_values_to_be_null` is not added.
- **primary_or_compound_key**: List\[str\] - Specifies one or more columns in list form as a primary or compound key, and adds `expect_column_values_to_be_unique` or `expect_compound_column_values_to_be_unique`.
- **table_expectations_only**: Bool - When set to True, all columns are ignored and a table-level Expectation is created. Table-level Expectations include `expect_table_row_count_to_equal` and `expect_table_columns_to_match_ordered_list`.
- **value_set_threshold**: str: Specify a value from the following ordered list - "none", "one", "two", "very_few", "few", "many", "very_many", "unique". When the Profiler runs, each column is profiled for cardinality. This threshold determines the greatest cardinality to add  `expect_column_values_to_be_in_set`. For example, if `value_set_threshold` is set to "unique", it adds a value_set Expectation for every included column. If set to "few", it adds a value_set expectation for columns whose cardinality is one of "one", "two", "very_few" or "few". The default value here is "many". To compare whether two tables are identical, set the value to "unique".
- **semantic_types_dict**: Dict\[str, List\[str\]\]. See the following section.

To make use of these parameters, you can specify them when you instantiate your Profiler. For example:

```python name="tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler optional_params"
```

**After instantiating a Profiler with specific parameters, you must re-instantiate the Profiler to apply the changes.**

## Semantic Types Dictionary Configuration

If the Profiler detects that a column is numeric, it creates numeric Expectations. If you don't want numeric Expectations, you can use `semantic_types_dict` to define the Expectations type. The semantic types that you can specify in the UserConfigurableProfiler are "numeric", "value_set", and "datetime". You can pass a dictionary where the keys are the semantic types, and the values are lists of columns of those semantic types.

When you specify `semantic_types_dict`, the Profiler creates table-level Expectations, Expectations for all columns using nullity and column proportions of unique values, and then semantic-type-specific Expectations for the columns specified in the semantic_types dictionary. The following is an example of the code you can run:

```python name="tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler semantic"
```

The following Expectations are added when you use `semantics_type_dict`:

**Table Expectations:**
- [`expect_table_row_count_to_be_between`](https://greatexpectations.io/expectations/expect_table_row_count_to_be_between)
- [`expect_table_columns_to_match_ordered_list`](https://greatexpectations.io/expectations/expect_table_columns_to_match_ordered_list)


**Expectations added for all included columns**
- [`expect_column_value_to_not_be_null`](https://greatexpectations.io/expectations/expect_column_values_to_not_be_null) (if a column consists of more than 50% null values, this will instead add [`expect_column_values_to_be_null`](https://greatexpectations.io/expectations/expect_column_values_to_be_null))
- [`expect_column_proportion_of_unique_values_to_be_between`](https://greatexpectations.io/expectations/expect_column_proportion_of_unique_values_to_be_between)
- [`expect_column_values_to_be_in_type_list`](https://greatexpectations.io/expectations/expect_column_values_to_be_in_type_list)


**Value set Expectations**
- [`expect_column_values_to_be_in_set`](https://greatexpectations.io/expectations/expect_column_values_to_be_in_set)


**Datetime Expectations**
- [`expect_column_values_to_be_between`](https://greatexpectations.io/expectations/expect_column_values_to_be_between)


**Numeric Expectations**
- [`expect_column_min_to_be_between`](https://greatexpectations.io/expectations/expect_column_min_to_be_between)
- [`expect_column_max_to_be_between`](https://greatexpectations.io/expectations/expect_column_max_to_be_between)
- [`expect_column_mean_to_be_between`](https://greatexpectations.io/expectations/expect_column_mean_to_be_between)
- [`expect_column_median_to_be_between`](https://greatexpectations.io/expectations/expect_column_median_to_be_between)
- [`expect_column_quantile_values_to_be_between`](https://greatexpectations.io/expectations/expect_column_quantile_values_to_be_between)


**Other Expectations**
- [`expect_column_values_to_be_unique`](https://greatexpectations.io/expectations/expect_column_values_to_be_unique) (if a single key is specified for `primary_or_compound_key`)
- [`expect_compound_columns_to_be_unique`](https://greatexpectations.io/expectations/expect_compound_columns_to_be_unique) (if a compound key is specified for `primary_or_compound_key`)
