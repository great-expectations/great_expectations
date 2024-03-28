"""
This example script demonstrates how to list the Checkpoints available
 in a Data Context and use attributes to filter the list.

The <snippet> tags are used to insert the corresponding code into the
  GX documentation, and you can disregard them.
 
"""

# <snippet name="/core/validate_data/checkpoints/_examples/list_available_checkpoints.py full example code">
import great_expectations as gx

context = gx.get_context()

# highlight-start
# <snippet name="/core/validate_data/checkpoints/_examples/list_available_checkpoints.py print checkpoint names">
checkpoint_names = [checkpoint.name for checkpoint in context.checkpoints]
print(checkpoint_names)
# </snippet>
# highlight-end

# <snippet name="/core/validate_data/checkpoints/_examples/list_available_checkpoints.py filter checkpoints list">
data_source_name = "my_datasource"
asset_name = "my_data_asset"
validation_definitions_for_my_asset = [validation_definition for validation_definition in context.validation_definitions
                                       if validation_definition.data_source.name == data_source_name
                                       and validation_definition.asset.name == asset_name]

# highlight-start
checkpoints_for_my_asset = [checkpoint.name for checkpoint in context.checkpoints
                            if set(checkpoint.validations).intersection(validation_definitions_for_my_asset)]
print(checkpoints_for_my_asset)
# highlight-end
# </snippet>
# </snippet>
