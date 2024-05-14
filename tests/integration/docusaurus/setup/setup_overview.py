# <snippet name="tests/integration/docusaurus/setup/setup_overview.py setup">
import great_expectations_v1 as gx

context = gx.get_context()
# </snippet>

assert context is not None
