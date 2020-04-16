"""
A basic generated Great Expectations tap that validates a single batch of data.

Data that is validated is controlled by BatchKwargs, which can be adjusted in
this script.

Data are validated by use of the `ActionListValidationOperator` which is
configured by default. The default configuration of this Validation Operator
saves validation results to your results store and then updates Data Docs.

This makes viewing validation results easy for you and your team.

Usage:
- Run this file: `python {0}`.
- This can be run manually or via a scheduler such as cron.
- If your pipeline runner supports python snippets you can paste this into your
pipeline.
"""
import sys
from click.testing import CliRunner
from great_expectations import DataContext

# tap configuration
context = DataContext("{1}")
# validation config json file 
validation_config_json = {3}

# tap validation process
root_dir = context.root_directory
runner = CliRunner(mix_stderr=False)
result = runner.invoke(
        cli,
        ["validation-operator", "run", "-d", root_dir, "--validation_config_file", validation_config_json],
        catch_exceptions=False
)

if not result["success"]:
    print("Validation Failed!")
    sys.exit(1)

print("Validation Succeeded!")
sys.exit(0)
