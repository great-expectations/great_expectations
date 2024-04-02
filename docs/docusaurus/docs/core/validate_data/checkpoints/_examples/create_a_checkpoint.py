"""
This example script demonstrates how to create a Checkpoint and add it
  to a Data Context.

The <snippet> tags are used to insert the corresponding code into
 GX documentation, and you can disregard them.

"""

# <snippet name="/core/validate_data/checkpoints/_examples/create_a_checkpoint.py full script">
# highlight-start
# <snippet name="/core/validate_data/checkpoints/_examples/create_a_checkpoint.py import statements">
import great_expectations as gx
from great_expectations.core import Checkpoint

# </snippet>
# highlight-end

context = gx.get_context()

validation_definition_name = "my_existing_validation_definition"
validation_definition = context.validation_definitions.get(
    name=validation_definition_name
)

validation_definitions = [validation_definition]
# <snippet name="/core/validate_data/checkpoints/_examples/create_a_checkpoint.py determine actions">
actions = [gx.UpdateDataDocs(...), gx.SlackNotification(...)]
# </snippet>
# <snippet name="/core/validate_data/checkpoints/_examples/create_a_checkpoint.py create checkpoint">
checkpoint_name = "my_checkpoint"

# highlight-start
checkpoint = Checkpoint(
    name=checkpoint_name, validations=validation_definitions, actions=actions
)
# highlight-end
# </snippet>

# highlight-start
# <snippet name="/core/validate_data/checkpoints/_examples/create_a_checkpoint.py add checkpoint">
context.checkpoints.add(checkpoint)
# </snippet>
# highlight-end
# </snippet>
