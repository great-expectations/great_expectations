"\nThis is a basic generated Great Expectations script that runs a Checkpoint.\n\nCheckpoints are the primary method for validating batches of data in production and triggering any followup actions.\n\nA Checkpoint facilitates running a validation as well as configurable Actions such as updating Data Docs, sending a\nnotification to team members about validation results, or storing a result in a shared cloud storage.\n\nSee also <cyan>https://docs.greatexpectations.io/en/latest/guides/how_to_guides/validation/how_to_create_a_new_checkpoint_using_test_yaml_config.html</cyan> for more information about the Checkpoints and how to configure them in your Great Expectations environment.\n\nCheckpoints can be run directly without this script using the `great_expectations checkpoint run` command.  This script\nis provided for those who wish to run Checkpoints in python.\n\nUsage:\n- Run this file: `python great_expectations/uncommitted/run_{0:s}.py`.\n- This can be run manually or via a scheduler such, as cron.\n- If your pipeline runner supports python snippets, then you can paste this into your pipeline.\n"
import sys

from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.data_context import DataContext

data_context: DataContext = DataContext(context_root_dir="{1:s}")
result: CheckpointResult = data_context.run_checkpoint(
    checkpoint_name="{0:s}", batch_request=None, run_name=None
)
if not result["success"]:
    print("Validation failed!")
    sys.exit(1)
print("Validation succeeded!")
sys.exit(0)
