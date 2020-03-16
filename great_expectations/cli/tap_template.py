"""
A basic generated Great Expectations Tap that validates a single batch of data.

This file can be run with `python {0}`.
This can be run manually or via a scheduler such as crontab.

Note this does not save any validation results.
"""
import sys

import great_expectations as ge

# Tap configuration
context = ge.DataContext("{1}")
suite = context.get_expectation_suite("{2}")
batch_kwargs = {3}

# Tap process
batch = context.get_batch(batch_kwargs, suite)
# TODO validation operator with good error handling
results = context.run_validation_operator("action_list_operator", [batch])

if not results["success"]:
    print("Validation Failed!")
    sys.exit(1)

print("Validation Succeeded!")
sys.exit(0)
