# <snippet name="tests/integration/docusaurus/template/script_example.py full">
# <snippet name="tests/integration/docusaurus/template/script_example.py imports">
# <snippet name="tests/integration/docusaurus/template/script_example.py first import">

# </snippet>
from __future__ import annotations

import great_expectations as gx
from great_expectations.core.yaml_handler import YAMLHandler

# </snippet>

yaml = YAMLHandler()
context = gx.get_context()

# <snippet name="tests/integration/docusaurus/template/script_example.py assert">
assert context
# </snippet>
# </snippet>
