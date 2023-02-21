# <snippet name="tests/integration/docusaurus/template/script_example.py full">
# <snippet name="tests/integration/docusaurus/template/script_example.py imports">
# <snippet name="tests/integration/docusaurus/template/script_example.py first import">
import os

# </snippet>

from ruamel import yaml

# </snippet>

import great_expectations as gx

context = gx.get_context()

# <snippet name="tests/integration/docusaurus/template/script_example.py assert">
assert context
# </snippet>
# </snippet>
