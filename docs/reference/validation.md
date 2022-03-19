---
title: Validation
---


Once you've constructed and stored Expectations, you can use them to validate new data. Validation generates a report
that details any specific deviations from expected values.

We recommend using a [Data Context](../reference/data_context.md) to manage Expectation Suites and coordinate validation across runs.

## Validation Results

The report contains information about:

* the overall success (the `success` field),
* summary statistics of the expectations (the `statistics` field), and
* the detailed results of each expectation (the `results` field).

Take the following example setup:

```python 
import json 
import great_expectations as ge

my_expectation_suite = json.load(open("my_titanic_expectations.json"))
my_df = ge.read_csv(
  "./tests/examples/titanic.csv", 
  expectation_suite=my_expectation_suite
)
my_df.validate()
```

The resulting report returned looks like this:

```python 
{
  "results" : [
    {
      "expectation_type": "expect_column_to_exist",
      "success": True,
      "kwargs": {
        "column": "Unnamed: 0"
      }
    },
    ...
    {
      "unexpected_list": 30.397989417989415,
      "expectation_type": "expect_column_mean_to_be_between",
      "success": True,
      "kwargs": {
        "column": "Age",
        "max_value": 40,
        "min_value": 20
      }
    },
    {
      "unexpected_list": [],
      "expectation_type": "expect_column_values_to_be_between",
      "success": True,
      "kwargs": {
        "column": "Age",
        "max_value": 80,
        "min_value": 0
      }
    },
    {
      "unexpected_list": [
        "Downton (?Douton), Mr William James",
        "Jacobsohn Mr Samuel",
        "Seman Master Betros"
      ],
      "expectation_type": "expect_column_values_to_match_regex",
      "success": True,
      "kwargs": {
        "regex": "[A-Z][a-z]+(?: \\([A-Z][a-z]+\\))?, ",
        "column": "Name",
        "mostly": 0.95
      }
    },
    {
      "unexpected_list": [
        "*"
      ],
      "expectation_type": "expect_column_values_to_be_in_set",
      "success": False,
      "kwargs": {
        "column": "PClass",
        "value_set": [
          "1st",
          "2nd",
          "3rd"
        ]
      }
    }
  ],
  "success", False,
  "statistics": {
      "evaluated_expectations": 10,
      "successful_expectations": 9,
      "unsuccessful_expectations": 1,
      "success_percent": 90.0,
  }
}
```

## Reviewing Validation Results

The easiest way to review Validation Results is to view them from your local Data Docs site, where you can also
conveniently view Expectation Suites and with additional configuration, Profiling Results
(see [Data Docs site configuration](../guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_a_filesystem.md)). Out of the box, Great Expectations Data Docs is configured to compile a local
data documentation site when you start a new project by running `great_expectations init`. By default, this local site
is saved to the `uncommitted/data_docs/local_site/` directory of your project and will contain pages for Expectation
Suites and Validation Results.

If you would like to review the raw Validation Results in JSON format, the default Validation Results directory
is `uncommitted/validations/`. Note that by default, Data Docs will only compile Validation Results located in this
directory.

## Checkpoints (formerly known as Validation Operators)

The example above demonstrates how to validate one batch of data against one Expectation Suite. The `validate` method
returns a dictionary of Validation Results. This is sufficient when exploring your data and getting to know Great
Expectations. When deploying Great Expectations in a real data pipeline, you will typically discover additional needs:

* validating a group of batches that are logically related
* validating a batch against several Expectation Suites
* doing something with the Validation Results (e.g., saving them for a later review, sending notifications in case of
  failures, etc.).

Checkpoints are mini-applications that can be configured to implement these scenarios.

Read [Checkpoints and Actions](./checkpoints_and_actions.md) to learn more.

## Reference Architectures

Useful Reference Architectures for validation include:

* Include validation at the end of a complex data transformation, to verify that no cases were lost, duplicated, or
  improperly merged.
* Include validation at the *beginning* of a script applying a machine learning model to a new batch of data, to verify
  that its distributed similarly to the training and testing set.
* Automatically trigger table-level validation when new data is dropped to an FTP site or S3 bucket, and send the
  validation report to the uploader and bucket owner by email.
* Schedule database validation jobs using cron, then capture errors and warnings (if any) and post them to Slack.
* Validate as part of an Airflow task: if Expectations are violated, raise an error and stop DAG propagation until the
  problem is resolved. Alternatively, you can implement Expectations that raise warnings without halting the DAG.

For certain Reference Architectures, it may be useful to parameterize Expectations, and supply Evaluation Parameters at
validation time. See [Evaluation Parameters](./evaluation_parameters.md) for more information.
