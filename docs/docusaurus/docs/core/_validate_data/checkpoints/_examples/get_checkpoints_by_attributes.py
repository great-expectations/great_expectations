"""
This example script demonstrates how to filter the list of Checkpoints in a
 Data Context on attributes.

The <snippet> tags are used to insert the corresponding code into
  GX documentation, and you can disregard them.

"""

# <snippet name="/core/validate_data/checkpoints/_examples/get_checkpoints_by_attributes.py full example code">
import great_expectations as gx
from great_expectations.checkpoints import SlackNotificationAction

context = gx.get_context()

# <snippet name="/core/validate_data/checkpoints/_examples/get_checkpoints_by_attributes.py filter checkpoints list on actions">
# highlight-start
checkpoints_with_slack_alerts = [
    checkpoint.name
    for checkpoint in context.checkpoints
    if any(isinstance(action, SlackNotificationAction) for action in checkpoint.actions)
]

# highlight-end
print(checkpoints_with_slack_alerts)
# </snippet>
# </snippet>
