"""
This example script demonstrates how to retrieve an existing Checkpoint
 from a Data Context by name.

The <snippet> tags are used to insert the corresponding code into the
 Great Expectations documentation.  They can be disregarded by anyone
 reviewing this script.
"""

# <snippet name="/core/validate_data/checkpoints/_examples/get_checkpoint_by_name.py full example script">
import great_expectations as gx

context = gx.get_context()

# <snippet name="/core/validate_data/checkpoints/_examples/get_checkpoint_by_name.py get checkpoint by name">
checkpoint_name = "my_checkpoint"
# highlight-start
checkpoint = context.checkpoints.get(name=checkpoint_name)
# highlight-end
# </snippet>
# </snippet>
