.. _validation:

================================================================================
Validation
================================================================================

Once you've constructed Expectations, you can use them to validate new data.

.. code-block:: bash

    >> import json
    >> import great_expectations as ge
    >> my_expectations_config = json.load(file("my_titanic_expectations.json"))
    >> my_df = ge.read_csv(
        "./tests/examples/titanic.csv",
        expectations_config=my_expectations_config
    )
    >> my_df.validate()

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
            "values_set": [
              "1st", 
              "2nd", 
              "3rd"
            ]
          }
        }
      ]
    }

Calling great_expectations's validation method generates a JSON-formatted report describing the outcome of all expectations.

Command-line validation
------------------------------------------------------------------------------

This is especially powerful when combined with great_expectations's command line tool, which lets you validate in a one-line bash script.

.. code-block:: bash

    $ great_expectations validate tests/examples/titanic.csv \
        tests/examples/titanic_expectations.json
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
            "values_set": [
              "1st", 
              "2nd", 
              "3rd"
            ]
          }
        }
      ]
    }

Deployment patterns
------------------------------------------------------------------------------

Useful deployment patterns include:

* Include validation at the end of a complex data transformation, to verify that no cases were lost, duplicated, or improperly merged.
* Include validation at the *beginning* of a script applying a machine learning model to a new batch of data, to verify that its distributed similarly to the training and testing set.
* Automatically trigger table-level validation when new data is dropped to an FTP site or S3 bucket, and send the validation report to the uploader and bucket owner by email.
* Schedule database validation jobs using cron, then capture errors and warnings (if any) and post them to Slack.
* Validate as part of an Airflow task: if Expectations are violated, raise an error and stop DAG propagation until the problem is resolved. Alternatively, you can implement expectations that raise warnings without halting the DAG.


