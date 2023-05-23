"""
To run this code as a local test, use the following console command:
```
pytest -v --docs-tests -m integration -k "how_to_explicitly_instantiate_an_ephemeral_data_context" tests/integration/test_script_runner.py
```
"""

import pathlib

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_explicitly_instantiate_an_ephemeral_data_context.py import_data_context_config_with_in_memory_store_backend">
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)

# </snippet>

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_explicitly_instantiate_an_ephemeral_data_context.py import_ephemeral_data_context">
from great_expectations.data_context import EphemeralDataContext

# </snippet>

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_explicitly_instantiate_an_ephemeral_data_context.py instantiate_data_context_config_with_in_memory_store_backend">
project_config = DataContextConfig(
    store_backend_defaults=InMemoryStoreBackendDefaults()
)
# </snippet>

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_explicitly_instantiate_an_ephemeral_data_context.py instantiate_ephemeral_data_context">
context = EphemeralDataContext(project_config=project_config)
# </snippet>

assert context

assert not context.root_directory

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_explicitly_instantiate_an_ephemeral_data_context.py check_data_context_is_ephemeral">
from great_expectations.data_context import EphemeralDataContext

# ...

if isinstance(context, EphemeralDataContext):
    print("It's Ephemeral!")
# </snippet>

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_explicitly_instantiate_an_ephemeral_data_context.py convert_ephemeral_data_context_filesystem_data_context">
context = context.convert_to_file_context()
# </snippet>

assert context

project_root_dir = pathlib.Path.cwd().absolute()
path_to_context_root_folder = project_root_dir / "great_expectations"
assert context.root_directory == str(path_to_context_root_folder)
