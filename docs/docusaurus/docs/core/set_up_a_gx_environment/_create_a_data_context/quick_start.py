"""
This example script demonstrates how to request a Data Context without
 specifying the mode parameter.

The <snippet> tags are used to insert the corresponding code into the
 Great Expectations documentation.  They can be disregarded by anyone
 reviewing this script.
"""

# <snippet name="core/set_up_a_gx_environment/_create_a_data_context/quick_start.py full example code">
# Import great_expectations and request a Data Context.
# <snippet name="core/set_up_a_gx_environment/_create_a_data_context/quick_start.py import great_expectations and get a context">
import great_expectations as gx

context = gx.get_context()
# </snippet>

# Optional. Check the type of Data Context that was returned.
# <snippet name="core/set_up_a_gx_environment/_create_a_data_context/quick_start.py check_context_type">
print(type(context).__name__)
# </snippet>
# </snippet>
