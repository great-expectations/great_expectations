"""
This is a basic generated Great Expectations script that runs a checkpoint.

A checkpoint is a list of one or more batches paired with one or more
Expectation Suites and a configurable Validation Operator.

Checkpoints can be run directly without this script using the
`great_expectations checkpoint run` command. This script is provided for those
who wish to run checkpoints via python.

Data that is validated is controlled by BatchKwargs, which can be adjusted in
the checkpoint file: great_expectations/checkpoints/{0}.yml.

Data are validated by use of the `ActionListValidationOperator` which is
configured by default. The default configuration of this Validation Operator
saves validation results to your results store and then updates Data Docs.

This makes viewing validation results easy for you and your team.

Usage:
- Run this file: `python great_expectations/uncommitted/run_{0}.py`.
- This can be run manually or via a scheduler such as cron.
- If your pipeline runner supports python snippets you can paste this into your
pipeline.
"""
import sys

from great_expectations import DataContext

# checkpoint configuration
context = DataContext("{1}")
checkpoint = context.get_checkpoint("{0}")

# load batches of data
batches_to_validate = []
for batch in checkpoint["batches"]:
    batch_kwargs = batch["batch_kwargs"]
    for suite_name in batch["expectation_suite_names"]:
        suite = context.get_expectation_suite(suite_name)
        batch = context.get_batch(batch_kwargs, suite)
        batches_to_validate.append(batch)

# run the validation operator
results = context.run_validation_operator(
    checkpoint["validation_operator_name"],
    assets_to_validate=batches_to_validate,
    # TODO prepare for new RunID - checkpoint name and timestamp
    # run_id=RunID(checkpoint)
)

# take action based on results
if not results["success"]:
    print("Validation failed!")
    sys.exit(1)

print("Validation succeeded!")
sys.exit(0)
