"""
To run this code as a local test, use the following console command:
```
pytest -v --docs-tests -k "how_to_instantiate_a_specific_filesystem_data_context" tests/integration/test_script_runner.py
```
"""

import great_expectations as gx
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
import pathlib
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_instantiate_a_specific_filesystem_data_context.py path_to_project_root">
path_to_project_root = "./my_project/"
# </snippet>

project_root_dir = pathlib.Path.cwd().absolute()
path_to_context_root_folder = project_root_dir / FileDataContext.GX_DIR
context = gx.data_context.FileDataContext.create(project_root_dir=project_root_dir)
assert context
assert context.root_directory == str(path_to_context_root_folder)

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_instantiate_a_specific_filesystem_data_context.py get_filesystem_data_context">
context = gx.get_context(project_root_dir=path_to_project_root)
# </snippet>

assert context

context_root_dir = pathlib.Path(context.root_directory)
assert context_root_dir.stem == FileDataContext.GX_DIR
assert context_root_dir.parent.stem == "my_project"
