from great_expectations import get_context

# TODO: will become from great_expectations import Checkpoint
# TODO will become from great_expectations.actions import SlackNotificationAction
from great_expectations.checkpoint import Checkpoint, SlackNotificationAction

# TODO will become freom great_expectations.exceptions import ResourceNotFoundError
from great_expectations.exceptions import DataContextError

context = get_context(project_root_dir="./")

try:
    checkpoint = context.checkpoints.get("project_integration_checkpoint")
# TODO: Will become ResourceNotFoundError
except DataContextError:
    checkpoint = context.checkpoints.add(
        Checkpoint(
            name="project_integration_checkpoint",
            validation_definitions=[context.validation_definitions.get("my_project")],
            actions=[
                SlackNotificationAction(
                    # TODO: config variable substitution not working
                    slack_token="${SLACK_NOTIFICATION_TOKEN}",
                    slack_channel="#alerts-timber-test",
                ),
            ],
        )
    )

result = checkpoint.run(batch_parameters={"year": "2020", "month": "04"})
print(result)
