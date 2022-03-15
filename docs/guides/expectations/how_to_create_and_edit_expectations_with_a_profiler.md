---
title: How to create and edit Expectations with the User Configurable Profiler
---

import Prerequisites from '../../guides/connecting_to_your_data/components/prerequisites.jsx'

This guide will help you create a new Expectation Suite by profiling your data with the User Configurable Profiler.

<Prerequisites>

- Configured a [Data Context](../../tutorials/getting_started/initialize_a_data_context.md).
- Configured a [Datasource](../../tutorials/getting_started/connect_to_data.md)

</Prerequisites>

:::note

The User Configurable Profiler makes it easier to produce a new Expectation Suite by building out a bunch of Expectations for your data.

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

:::note

You can access this same functionality from the Great Expectations CLI by running

```console
great_expectations suite new --profile
```

If you go that route, you can follow along in the resulting Jupyter Notebook instead of using this guide.

:::


## Steps

### 1. Load or create your Data Context

Load an on-disk Data Context via:

```python
from great_expectations.data_context.data_context import DataContext

context = DataContext(
    context_root_dir='path/to/my/context/root/directory/great_expectations'
)
```

Alternatively, [you can instantiate a Data Context without a .yml file](../setup/configuring_data_contexts/how_to_instantiate_a_data_context_without_a_yml_file.md)

### 2. Set your expectation_suite_name and create your Batch Request 

The Batch Request specifies which Batch of data you would like to profile in order to create your suite. We will pass it into a Validator in the next step.

```python
expectation_suite_name = "insert_the_name_of_your_suite_here"

batch_request = {
    "datasource_name": "my_datasource",
    "data_connector_name": "default_inferred_data_connector_name",
    "data_asset_name": "yellow_tripdata_sample_2020-05.csv",
}
```

### 3. Instantiate your Validator

We use a Validator to access and interact with your data. We will be passing the Validator to our Profiler in the next step.

```python
from great_expectations.core.batch import BatchRequest

validator = context.get_validator(
    batch_request=BatchRequest(**batch_request),
    expectation_suite_name=expectation_suite_name
)
```

After you get your Validator, you can call `validator.head()` to confirm that it contains the data that you expect.

### 4. Instantiate a UserConfigurableProfiler

Next, we instantiate a UserConfigurableProfiler, passing in the Validator with our data

```python
from great_expectations.profile.user_configurable_profiler import UserConfigurableProfiler
profiler = UserConfigurableProfiler(profile_dataset=validator)
```

### 5. Use the profiler to build a suite

Once we have our Profiler set up with our Batch, we call `profiler.build_suite()`. This will print a list of all the Expectations created by column, and return the Expectation Suite object.

```python
suite = profiler.build_suite()
```

### 6. (Optional) Running validation, saving your suite, and building Data Docs

If you'd like, you can Validate your data with the new Expectation Suite, save your Expectation Suite, and build Data Docs to take a closer look at the output

```python
from great_expectations.checkpoint.checkpoint import SimpleCheckpoint

# Review and save our Expectation Suite 
print(validator.get_expectation_suite(discard_failed_expectations=False))
validator.save_expectation_suite(discard_failed_expectations=False)

# Set up and run a Simple Checkpoint for ad hoc validation of our data 
checkpoint_config = {
    "class_name": "SimpleCheckpoint",
    "validations": [
        {
            "batch_request": batch_request,
            "expectation_suite_name": expectation_suite_name,
        }
    ],
}
checkpoint = SimpleCheckpoint(
    f"_tmp_checkpoint_{expectation_suite_name}", context, **checkpoint_config
)
checkpoint_result = checkpoint.run()

# Build Data Docs
context.build_data_docs()

# Get the only validation_result_identifier from our SimpleCheckpoint run, and open Data Docs to that page
validation_result_identifier = checkpoint_result.list_validation_result_identifiers()[0] 
context.open_data_docs(resource_identifier=validation_result_identifier)
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

```python
excluded_expectations = ["expect_column_quantile_values_to_be_between"]
ignored_columns = ['comment', 'acctbal', 'mktsegment', 'name', 'nationkey', 'phone']
not_null_only = True
table_expectations_only = False
value_set_threshold = "unique"

validator = context.get_validator(
    batch_request=BatchRequest(**batch_request),
    expectation_suite_name=expectation_suite_name
)

profiler = UserConfigurableProfiler(
    profile_dataset=validator,
    excluded_expectations=excluded_expectations,
    ignored_columns=ignored_columns,
    not_null_only=not_null_only,
    table_expectations_only=table_expectations_only,
    value_set_threshold=value_set_threshold)

suite = profiler.build_suite()
```

**Once you have instantiated a Profiler with parameters specified, you must re-instantiate the Profiler if you wish to change any of the parameters.**

### Semantic Types Dictionary Configuration

The Profiler is fairly rudimentary - if it detects that a column is numeric, it will create numeric Expectations (e.g. ``expect_column_mean_to_be_between``). But if you are storing foreign keys or primary keys as integers, then you may not want numeric Expectations on these columns. This is where the semantic_types dictionary comes in.

The available semantic types that can be specified in the UserConfigurableProfiler are "numeric", "value_set", and "datetime". The Expectations created for each of these types is below. You can pass in a dictionary where the keys are the semantic types, and the values are lists of columns of those semantic types.

When you pass in a `semantic_types_dict`, the Profiler will still create table-level expectations, and will create certain expectations for all columns (around nullity and column proportions of unique values). It will then only create semantic-type-specific Expectations for those columns specified in the semantic_types dict.

```python
semantic_types_dict = {
    "numeric": ["acctbal"],
    "value_set": ["nationkey","mktsegment", 'custkey', 'name', 'address', 'phone', "acctbal"]
}

validator = context.get_validator(
    batch_request=BatchRequest(**batch_request),
    expectation_suite_name=expectation_suite_name
)

profiler = UserConfigurableProfiler(
    profile_dataset=validator,
    semantic_types_dict=semantic_types_dict
)
suite = profiler.build_suite()
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
