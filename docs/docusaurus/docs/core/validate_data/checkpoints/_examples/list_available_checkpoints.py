"""
This example script demonstrates how to list the Checkpoints available
 in a Data Context and use attributes to filter the list.

The <snippet> tags are used to insert the corresponding code into
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
# </snippet>
