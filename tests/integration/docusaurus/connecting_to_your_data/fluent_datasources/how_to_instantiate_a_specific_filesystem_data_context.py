"""
To run this code as a local test, use the following console command:
```
pytest -v --docs-tests -m integration -k "how_to_instantiate_a_specific_filesystem_data_context" tests/integration/test_script_runner.py
```
"""

import pathlib

import great_expectations as gx

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_instantiate_a_specific_filesystem_data_context.py path_to_context_root_folder">
path_to_context_root_folder = "/my_gx_project/"
# </snippet>

project_root_dir = pathlib.Path.cwd().absolute()
path_to_context_root_folder = project_root_dir / "great_expectations"
context = gx.data_context.FileDataContext.create(project_root_dir=project_root_dir)
assert context
assert context.root_directory == str(path_to_context_root_folder)


# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_instantiate_a_specific_filesystem_data_context.py get_filesystem_data_context">
context = gx.get_context(context_root_dir=path_to_context_root_folder)
# </snippet>

assert context
assert context.root_directory == str(path_to_context_root_folder)
