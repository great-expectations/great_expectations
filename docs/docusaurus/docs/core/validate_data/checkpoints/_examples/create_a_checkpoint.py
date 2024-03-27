"""
This example script demonstrates how to create a Checkpoint and add it
  to a Data Context.

The <snippet> tags are used to insert the corresponding code into the
 Great Expectations documentation.  They can be disregarded by anyone
 reviewing this script.
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
validation_definition = context.validation_definitions.get(name=validation_definition_name)

# highlight-start
# <snippet name="/core/validate_data/checkpoints/_examples/create_a_checkpoint.py create checkpoint">
checkpoint = Checkpoint(
  validations=[validation_definition],
  actions=[]
)
# </snippet>
# highlight-end

# <snippet name="/core/validate_data/checkpoints/_examples/create_a_checkpoint.py add checkpoint">
# highlight-start
context.checkpoints.add(checkpoint)
# highlight-end
# </snippet>
# </snippet>