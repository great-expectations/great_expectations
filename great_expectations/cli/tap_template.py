"""
A basic generated Great Expectations tap that validates a single batch of data.

Data that is validated is controlled by BatchKwargs, which can be adjusted in
this script.

Data are validated by use of the `ActionListValidationOperator` which is
configured by default. The default configuration of this Validation Operator
saves validation results to your results store and then updates Data Docs.

This makes viewing validation results easy for you and your team.

Usage:
- Run this file: `python time_series_confirmed.py`.
- This can be run manually or via a scheduler such as cron.
"""
import sys
import great_expectations as ge

# tap configuration
context = ge.DataContext("{1}")
suite = context.get_expectation_suite("{2}")
batch_kwargs = {3}

# tap validation process
batch = context.get_batch(batch_kwargs, suite)
results = context.run_validation_operator("action_list_operator", [batch])

if not results["success"]:
    print("Validation Failed!")
    sys.exit(1)

print("Validation Succeeded!")
sys.exit(0)
