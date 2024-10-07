"""
This is an example script for how to create a Checkpoint with Actions.

To test, run:
pytest --docs-tests -k "docs_example_create_a_checkpoint" tests/integration/test_script_runner.py
"""


def set_up_context_for_example(context):
    # Create a Batch Definition
    batch_definition = (
        context.data_sources.add_pandas_filesystem(
            name="my_data_source", base_directory="./data/folder_with_data"
        )
        .add_csv_asset(name="my_data_asset")
        .add_batch_definition_path(
            name="my_batch_definition", path="yellow_tripdata_sample_2019-01.csv"
        )
    )

    # Create an Expectation Suite
    expectation_suite = context.suites.add(
        gx.ExpectationSuite(name="my_expectation_suite")
    )
    # Add some Expectations
    expectation_suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="pickup_datetime")
    )
    expectation_suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="passenger_count")
    )

    # Create a Validation Definition
    context.validation_definitions.add(
        gx.ValidationDefinition(
            data=batch_definition,
            suite=expectation_suite,
            name="my_validation_definition",
        )
    )


# EXAMPLE SCRIPT STARTS HERE:

# Dummy values to appease config substitution
import os

os.environ["validation_notification_slack_webhook"] = (
    "https://hooks.slack.com/services/..."
)
os.environ["validation_notification_slack_channel"] = "my_slack_channel"

# <snippet name="docs/docusaurus/docs/core/trigger_actions_based_on_results/_examples/create_a_checkpoint_with_actions.py - full code example">
import great_expectations as gx

context = gx.get_context()
# Hide this
set_up_context_for_example(context)

# Create a list of one or more Validation Definitions for the Checkpoint to run
# <snippet name="docs/docusaurus/docs/core/trigger_actions_based_on_results/_examples/create_a_checkpoint_with_actions.py - create a Validation Definitions list">
validation_definitions = [
    context.validation_definitions.get("my_validation_definition")
]
# </snippet>

# Create a list of Actions for the Checkpoint to perform
# <snippet name="docs/docusaurus/docs/core/trigger_actions_based_on_results/_examples/create_a_checkpoint_with_actions.py - define an Action list">
action_list = [
    # This Action sends a Slack Notification if an Expectation fails.
    gx.checkpoint.SlackNotificationAction(
        name="send_slack_notification_on_failed_expectations",
        slack_token="${validation_notification_slack_webhook}",
        slack_channel="${validation_notification_slack_channel}",
        notify_on="failure",
        show_failed_expectations=True,
    ),
    # This Action updates the Data Docs static website with the Validation
    #   Results after the Checkpoint is run.
    gx.checkpoint.UpdateDataDocsAction(
        name="update_all_data_docs",
    ),
]
# </snippet>

# Create the Checkpoint
# <snippet name="docs/docusaurus/docs/core/trigger_actions_based_on_results/_examples/create_a_checkpoint_with_actions.py - create a Checkpoint">
checkpoint_name = "my_checkpoint"
checkpoint = gx.Checkpoint(
    name=checkpoint_name,
    validation_definitions=validation_definitions,
    actions=action_list,
    result_format={"result_format": "COMPLETE"},
)
# </snippet>

# Save the Checkpoint to the Data Context
# <snippet name="docs/docusaurus/docs/core/trigger_actions_based_on_results/_examples/create_a_checkpoint_with_actions.py - save the Checkpoint to the Data Context">
context.checkpoints.add(checkpoint)
# </snippet>

# Retrieve the Checkpoint later
# <snippet name="docs/docusaurus/docs/core/trigger_actions_based_on_results/_examples/create_a_checkpoint_with_actions.py - retrieve a Checkpoint from the Data Context">
checkpoint_name = "my_checkpoint"
checkpoint = context.checkpoints.get(checkpoint_name)
# </snippet>
# </snippet>
