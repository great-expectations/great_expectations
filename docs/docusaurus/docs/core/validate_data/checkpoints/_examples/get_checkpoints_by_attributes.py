"""
This example script demonstrates how to filter the list of Checkpoints in a
 Data Context on attributes.

The <snippet> tags are used to insert the corresponding code into the
 Great Expectations documentation.  They can be disregarded by anyone
 reviewing this script.
"""

# <snippet name="/core/validate_data/checkpoints/_examples/get_checkpoints_by_attributes.py full example code">
import great_expectations as gx

context = gx.get_context()

# <snippet name="/core/validate_data/checkpoints/_examples/get_checkpoints_by_attributes.py filter checkpoints list on data asset">
data_source_name = "my_datasource"
asset_name = "my_data_asset"
validation_definitions_for_my_asset = [validation_definition for validation_definition in context.validation_definitions
                                       if validation_definition.data_source.name == data_source_name
                                       and validation_definition.asset.name == asset_name]

# highlight-start
checkpoints_for_my_asset = [checkpoint.name for checkpoint in context.checkpoints
                            if set(checkpoint.validations).intersection(validation_definitions_for_my_asset)]
# highlight-end
print(checkpoints_for_my_asset)
# </snippet>

# <snippet name="/core/validate_data/checkpoints/_examples/get_checkpoints_by_attributes.py filter checkpoints list on actions">
# highlight-start
checkpoints_with_slack_alerts = [checkpoint.name for checkpoint in context.checkpoints
                                 if any(isinstance(action, gx.SlackNotification) for action in checkpoint.actions)]

# highlight-end
print(checkpoints_with_slack_alerts)
# </snippet>
# </snippet>
