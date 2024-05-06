---
sidebar_label: 'Validate column values against a column in another table'
title: 'Validate column values against a column in another table'
description: Validate column values against a column in another table.
---

When validating data schema, it is common use case to vet incoming values against a collection values in a reference table. For example:
* Incoming orders must be checked for a valid currency.
* An address must be checked for a valid country.
* A categorical value must be checked against a list of values that define that category.

Rather than specify valid values within a hardcoded list that must be updated by developers every time it changes, it's often much more maintainable to store values in a table that can be easily updated without requiring a software change. GX provides an Expectation, `expect_column_values_to_be_present_in_other_table` that enables you to do just that.

## Sample data

In this example, we'll demonstrate how to validate that a column in a source table only contains values that are present in the column of another table. The two tables we'll use in this example are:

* `customer`: a customer table containing the customer id, name, and country

| `country_code` | `country_name` |
| :-- | :-- |
| US | United States |
| GB | United Kingdom |
| JP | Japan |
| IS | Iceland |
| MV | Maldives |
| BZ | Belize |

* `country`: a reference table containing country code and country name

| `customer_id` | `customer_name` | `customer_country` |
| :-- | :-- | :-- |
| 1 | Alice | US |
| 2 | Bob | GB |
| 3 | Charlie | JP |
| 4 | Dave | IS |
| 5 | Eddie | MV |
| 6 | Fred | BZ |
| 7 | Greg | BZ |
| 8 | Hilary | AR |

## Run the Expectation

First, install `great_expectations_experimental` to gain access to contrib Expectations.

```
pip install great-expectations-experimental
```

To use the `expect_column_values_to_be_present_in_other_table` Expectation, import it directly from `great_expectations_experimental` in addition the standard `great_expectations` import.
```
import great_expectations as gx

from great_expectations_experimental.expectations.expect_column_values_to_be_present_in_other_table import ExpectColumnValuesToBePresentInOtherTable
```

Next, create the GX context, Data Source, Data Asset, Batch Request, and Validator. This example uses a Snowflake Data Source. To connect to a different Data Source, see [Connect to data](/core/manage_and_access_data/connect_to_data/connect_to_data.md).
```
context = gx.get_context()

data_source = context.sources.add_snowflake(
    name="<data-source-name>",
    account="<snowflake-account-identifier>",
    user="<snowflake-account-username>",
    password="<snowflake-account-password>",
    database="<snowflake-account-database>",
    schema="<snowflake-account-schema>",
    warehouse="<snowflake-account-warehouse>",
    role="<snowflake-account-role>",
)

data_asset = data_source.add_table_asset(
    name="customers",
    table_name="customer"
)

context.add_or_update_expectation_suite(
    expectation_suite_name="customer_expectations"
)

batch_request = data_asset.build_batch_request()

validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name="customer_expectations",
)
```

To validate that `customer.customer_country` contains only values present in `country.country_code`, run `expect_column_values_to_be_present_in_other_table` using the Validator object.
```
validator.expect_column_values_to_be_present_in_other_table(
    foreign_key_column="customer_country",
    foreign_table="country",
    foreign_table_key_column="country_code",
)
```

## Result

Running this Expectation against our sample data results in a failed Expectation; the country code of customer 8 (Hilary) is not contained in the `country.country_code` column.

```
{
  "success": false,
  "result": {
    "observed_value": "1 missing value.",
    "unexpected_list": [
      "AR"
    ],
    "unexpected_index_column_names": [
      "country"
    ],
    "unexpected_index_list": [
      {
        "country": "AR"
      }
    ],
    "partial_unexpected_counts": [
      {
        "value": "AR",
        "count": 1
      }
    ]
  },
  "meta": {},
  "exception_info": {
    "raised_exception": false,
    "exception_traceback": null,
    "exception_message": null
  }
}
```

If Hilary is removed from the customer table, then `expect_column_values_to_be_present_in_other_table` passes with the following output.
```
{
  "success": true,
  "result": {
    "observed_value": "0 missing values.",
    "unexpected_list": [],
    "unexpected_index_column_names": [
      "country"
    ],
    "unexpected_index_list": [],
    "partial_unexpected_counts": []
  },
  "meta": {},
  "exception_info": {
    "raised_exception": false,
    "exception_traceback": null,
    "exception_message": null
  }
}
```