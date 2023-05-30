"""
To run this code as a local test, use the following console command:
```
pytest -v --docs-tests -m integration -k "how_to_initialize_a_filesystem_data_context_in_python" tests/integration/test_script_runner.py
```
"""

import pathlib

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_initialize_a_filesystem_data_context_in_python.py path_to_empty_folder">
path_to_empty_folder = "/my_gx_project/"
# </snippet>

project_root_dir = pathlib.Path.cwd().absolute()
path_to_context_root_folder = project_root_dir / "great_expectations"

path_to_empty_folder = project_root_dir

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_initialize_a_filesystem_data_context_in_python.py initialize_filesystem_data_context">
from great_expectations.data_context import FileDataContext

context = FileDataContext.create(project_root_dir=path_to_empty_folder)
# </snippet>

assert context
assert context.root_directory == str(path_to_context_root_folder)
