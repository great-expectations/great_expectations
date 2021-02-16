"""
A basic generated Great Expectations tap that validates a single batch of data.

Data that is validated is controlled by BatchKwargs, which can be adjusted in
this script.

Data are validated by use of the `ActionListValidationOperator` which is
configured by default. The default configuration of this Validation Operator
saves validation results to your results store and then updates Data Docs.

This makes viewing validation results easy for you and your team.

Usage:
- Run this file: `python {}`.
- This can be run manually or via a scheduler such as cron.
"""
import sys

import great_expectations as ge

# tap configuration
context = ge.DataContext(
    "/private/var/folders/_t/psczkmjd69vf9jz0bblzlzww0000gn/T/pytest-of-taylor/pytest-1812/empty_data_context0/great_expectations"
)
suite = context.get_expectation_suite("sweet_suite")
batch_kwargs = {
    "path": "/private/var/folders/_t/psczkmjd69vf9jz0bblzlzww0000gn/T/pytest-of-taylor/pytest-1812/filesystem_csv0/f1.csv",
    "datasource": "1_datasource",
}

# tap validation process
batch = context.get_batch(batch_kwargs, suite)
results = context.run_validation_operator("action_list_operator", [batch])

if not results["success"]:
    print("Validation Failed!")
    sys.exit(1)

print("Validation Succeeded!")
sys.exit(0)
