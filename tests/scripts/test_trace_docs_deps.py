import pprint

import pytest

from scripts.trace_docs_deps import (
    find_docusaurus_refs_in_file,
    parse_definition_nodes_from_file,
    retrieve_symbols_from_file,
)


@pytest.mark.filesystem
def test_parse_definition_nodes_from_file(tmpdir):
    f = tmpdir.mkdir("tmp").join("foo.py")
    f.write(
        """
logger = logging.getLogger(__name__)

def test_yaml_config():
    pass

class DataContext(BaseDataContext):
    def add_store(self, store_name, store_config):
        pass

    @classmethod
    def find_context_root_dir(cls):
        pass
    """
    )

    definition_map = parse_definition_nodes_from_file(f)
    pprint.pprint(definition_map)

    # Only parses from global scope
    assert all(
        symbol in definition_map
        for symbol in (
            "test_yaml_config",
            "DataContext",
        )
    )
    assert all(len(paths) == 1 and f in paths for paths in definition_map.values())


@pytest.mark.filesystem
def test_find_docusaurs_refs_in_file(tmpdir):
    f = tmpdir.mkdir("tmp").join("foo.md")
    f.write(
        """
```bash
great_expectations datasource new
```
```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/s3/spark/inferred_and_runtime_python_example.py#L53
```
```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_python_example.py
```
```python
print("Hello World")
```
    """
    )

    refs = find_docusaurus_refs_in_file(f)
    print(refs)

    assert len(refs) == 2
    assert all(ref.endswith("python_example.py") for ref in refs)


@pytest.mark.filesystem
def test_retrieve_symbols_from_file(tmpdir):
    f = tmpdir.mkdir("tmp").join("foo.py")
    f.write(
        """
context = DataContext()
assert is_numeric(1)

batch_request = get_batch_request_dict()
    """
    )

    symbols = retrieve_symbols_from_file(f)
    assert all(
        symbol in symbols
        for symbol in ("DataContext", "is_numeric", "get_batch_request_dict")
    )
