import pytest
import tempfile
import os

def test_empty_store(empty_data_context):

    my_expectation_store = empty_data_context.test_yaml_config(
        yaml_config="""
class_name: ExpectationsStore
store_backend:

    module_name: "great_expectations.data_context.store.store_backend"
    class_name: InMemoryStoreBackend
""")


def test_config_with_yaml_error(empty_data_context):

    with pytest.raises(Exception):
        my_expectation_store = empty_data_context.test_yaml_config(
            yaml_config="""
class_name: ExpectationsStore
store_backend:
    module_name: "great_expectations.data_context.store.store_backend"
    class_name: InMemoryStoreBackend
EGREGIOUS FORMATTING ERROR
""")

def test_filesystem_store(empty_data_context):
    tmp_dir = str(tempfile.mkdtemp())
    with open(os.path.join(tmp_dir, "expectations_A1.json"), "w") as f_:
        f_.write("\n")
    with open(os.path.join(tmp_dir, "expectations_A2.json"), "w") as f_:
        f_.write("\n")


    my_expectation_store = empty_data_context.test_yaml_config(
        yaml_config=f"""
class_name: ExpectationsStore
store_backend:

    module_name: "great_expectations.data_context.store"
    class_name: TupleFilesystemStoreBackend
    base_directory: {tmp_dir}
""")