---
title: How to create and edit Expectations with the User Configurable Profiler
---

import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

This guide will help you create a new <TechnicalTag tag="expectation_suite" text="Expectation Suite" /> by profiling your data with the User Configurable <TechnicalTag tag="profiler" text="Profiler" />.

:::note

The User Configurable Profiler makes it easier to produce a new Expectation Suite by building out a bunch of <TechnicalTag tag="expectation" text="Expectations" /> for your data.

These Expectations are deliberately over-fitted on your data e.g. if your table has 10,000 rows, the Profiler will produce an Expectation with the following config:

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

Thus, the intention is for this Expectation Suite to be edited and updated to better suit your specific use case - it is not specifically intended to be used as is.

:::

## Steps

### 1. Load or create your Data Context

In this guide we will use an on-disk data context with a pandas <TechnicalTag tag="datasource" text="Datasource" />
and a csv data asset. If you don't already have one you can create one:

```python name="tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler create_asset"
```

If a <TechnicalTag tag="datasource" text="Datasource" /> and data asset already exist, you can load 
an on-disk <TechnicalTag tag="data_context" text="Data Context" /> via:
```python name="tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler get_asset"
```

### 2. Set your expectation_suite_name and create your Batch Request

The <TechnicalTag tag="batch_request" text="Batch Request" /> specifies which 
<TechnicalTag tag="batch" text="Batch" /> of data you would like to 
<TechnicalTag tag="profiling" text="Profile" /> in order to create your Expectation Suite.
We will pass it into a <TechnicalTag tag="validator" text="Validator" /> in the next step.

```python name="tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler name_suite"
```

### 3. Instantiate your Validator

We use a Validator to access and interact with your data. We will be passing the Validator to our Profiler in the next step.

```python name="tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler create_validator"
```

After you get your Validator, you can call `validator.head()` to confirm that it contains the data that you expect.

### 4. Instantiate a UserConfigurableProfiler

Next, we instantiate a UserConfigurableProfiler, passing in the Validator with our data:

```python name="tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler create_profiler"
```

### 5. Use the profiler to build a suite

Once we have our Profiler set up with our Batch, we call `profiler.build_suite()`. 
This will print a list of all the Expectations created by column, and return the Expectation Suite object.

```python name="tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler build_suite"
```

### 6. (Optional) Running validation, saving your suite, and building Data Docs

If you'd like, you can <TechnicalTag tag="validation" text="Validate" /> your data with the new Expectation Suite, 
save your Expectation Suite, and build <TechnicalTag tag="data_docs" text="Data Docs" /> to take a closer look 
at the output:

```python name="tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler e2e"
```

And you're all set!

## Optional Parameters

The UserConfigurableProfiler can take a few different parameters to further hone the results. These parameters are:

- **excluded_expectations**: List\[str\] - Specifies Expectation types which you want to exclude from the Expectation Suite
- **ignored_columns**: List\[str\] - Columns for which you do not want to build Expectations (i.e. if you have metadata columns which might not be the same between tables
- **not_null_only**: Bool - By default, each column is evaluated for nullity. If the column values contain fewer than 50% null values, then the Profiler will add `expect_column_values_to_not_be_null`; if greater than 50% it will add `expect_column_values_to_be_null`. If `not_null_only` is set to True, the Profiler will add a `not_null` Expectation irrespective of the percent nullity (and therefore will not add an `expect_column_values_to_be_null`)
- **primary_or_compound_key**: List\[str\] - This allows you to specify one or more columns in list form as a primary or compound key, and will add `expect_column_values_to_be_unique` or `expect_compound_column_values_to_be_unique`
- **table_expectations_only**: Bool - If True, this will only create table-level Expectations (i.e. ignoring all columns). Table-level Expectations include `expect_table_row_count_to_equal` and `expect_table_columns_to_match_ordered_list`
- **value_set_threshold**: str: Specify a value from the following ordered list - "none", "one", "two", "very_few", "few", "many", "very_many", "unique". When the Profiler runs, each column is profiled for cardinality. This threshold determines the greatest cardinality for which to add `expect_column_values_to_be_in_set`. For example, if `value_set_threshold` is set to "unique", it will add a value_set Expectation for every included column. If set to "few", it will add a value_set expectation for columns whose cardinality is one of "one", "two", "very_few" or "few". The default value here is "many". For the purposes of comparing whether two tables are identical, it might make the most sense to set this to "unique".
- **semantic_types_dict**: Dict\[str, List\[str\]\]. Described in more detail below.

If you would like to make use of these parameters, you can specify them while instantiating your Profiler.

```python name="tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler optional_params"
```

**Once you have instantiated a Profiler with parameters specified, you must re-instantiate the Profiler if you wish to change any of the parameters.**

### Semantic Types Dictionary Configuration

The Profiler is fairly rudimentary - if it detects that a column is numeric, it will create numeric Expectations (e.g. ``expect_column_mean_to_be_between``). But if you are storing foreign keys or primary keys as integers, then you may not want numeric Expectations on these columns. This is where the semantic_types dictionary comes in.

The available semantic types that can be specified in the UserConfigurableProfiler are "numeric", "value_set", and "datetime". The Expectations created for each of these types is below. You can pass in a dictionary where the keys are the semantic types, and the values are lists of columns of those semantic types.

When you pass in a `semantic_types_dict`, the Profiler will still create table-level expectations, and will create certain expectations for all columns (around nullity and column proportions of unique values). It will then only create semantic-type-specific Expectations for those columns specified in the semantic_types dict.

```python name="tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler semantic"
```

These are the Expectations added when using a `semantics_type_dict`:

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
