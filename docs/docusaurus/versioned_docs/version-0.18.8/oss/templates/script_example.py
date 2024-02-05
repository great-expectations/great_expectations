# <snippet name="version-0.18.8 docs/docusaurus/docs/oss/templates/script_example.py full">
# <snippet name="version-0.18.8 docs/docusaurus/docs/oss/templates/script_example.py imports">
# <snippet name="version-0.18.8 docs/docusaurus/docs/oss/templates/script_example.py first import">

# </snippet>

import great_expectations as gx
from great_expectations.core.yaml_handler import YAMLHandler

# </snippet>

yaml = YAMLHandler()
context = gx.get_context()

# <snippet name="version-0.18.8 docs/docusaurus/docs/oss/templates/script_example.py assert">
assert context
# </snippet>
# </snippet>
