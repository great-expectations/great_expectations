"""
This is an example script for how to connect to a GX Cloud account using Python.

To test, run:
# TODO: This needs to be put under test when the GX Cloud docs snippet testing environment has been created.
"""


def set_up_context_for_example(context):
    pass


# EXAMPLE SCRIPT STARTS HERE:
# <snippet name="docs/docusaurus/docs/cloud/connect/connect_python.py - full code example">
# <snippet name="docs/docusaurus/docs/cloud/connect/connect_python.py - get cloud context">
import great_expectations as gx

context = gx.get_context(mode="cloud")
# </snippet>
# <snippet name="docs/docusaurus/docs/cloud/connect/connect_python.py - verify context type">
print(type(context).__name__)
# </snippet>

# Hide this
assert type(context).__name__ == "CloudDataContext"
# Hide this
set_up_context_for_example(context)

# <snippet name="docs/docusaurus/docs/cloud/connect/connect_python.py - list data sources">
print(context.list_datasources())
# </snippet>

# <snippet name="docs/docusaurus/docs/cloud/connect/connect_python.py - retrieve a data asset">
data_source_name = "replace this with the name of your Data Source"
asset_name = "replace this with the name of your Data Source's Data Asset"
batch_definition_name = (
    "replace this with the name of your Data Asset's Batch Definition"
)
batch = (
    gx.context.data_sources.get(data_source_name)
    .get_asset(asset_name)
    .get_batch_definition(batch_definition_name)
    .get_batch()
)
# </snippet>

# </snippet>
