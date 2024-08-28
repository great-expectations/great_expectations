"""
This example script demonstrates how to request an Cloud Data Context
 using the `mode` parameter.

The <snippet> tags are used to insert the corresponding code into the
 Great Expectations documentation.  They can be disregarded by anyone
 reviewing this script.
"""

# <snippet name="core/set_up_a_gx_environment/_create_a_data_context/cloud_data_context.py full example code">
# Import great_expectations and request a Data Context.
# <snippet name="core/set_up_a_gx_environment/_create_a_data_context/cloud_data_context.py import great_expectations and get a context">
import great_expectations as gx

context = gx.get_context(mode="cloud")
# </snippet>

# Optional. Review the configuration of the returned Cloud Data Context.
# <snippet name="core/set_up_a_gx_environment/_create_a_data_context/cloud_data_context.py review returned Data Context">
print(context)
# </snippet>
# </snippet>

assert type(context).__name__ == "CloudDataContext"
