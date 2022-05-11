
'\nThis is a basic generated Great Expectations script that runs a checkpoint.\n\nInternally, a Checkpoint is a list of one or more batches paired with one or more\nExpectation Suites and a configurable Validation Operator.\n\nCheckpoints can be run directly without this script using the\n`great_expectations checkpoint run` command. This script is provided for those\nwho wish to run checkpoints via python.\n\nData that is validated is controlled by BatchKwargs, which can be adjusted in\nthe checkpoint file: great_expectations/checkpoints/{0}.yml.\n\nData are validated by use of the `ActionListValidationOperator` which is\nconfigured by default. The default configuration of this Validation Operator\nsaves validation results to your results store and then updates Data Docs.\n\nThis makes viewing validation results easy for you and your team.\n\nUsage:\n- Run this file: `python great_expectations/uncommitted/run_{0}.py`.\n- This can be run manually or via a scheduler such as cron.\n- If your pipeline runner supports python snippets you can paste this into your\npipeline.\n'
import sys
from great_expectations import DataContext
context = DataContext('{1}')
checkpoint = context.get_checkpoint('{0}')
results = checkpoint.run()
if (not results['success']):
    print('Validation failed!')
    sys.exit(1)
print('Validation succeeded!')
sys.exit(0)
