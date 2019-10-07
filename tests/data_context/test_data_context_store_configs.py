import pytest
import os

from ruamel.yaml import YAML
yaml = YAML()
yaml.default_flow_style = False

import great_expectations as ge

@pytest.fixture(scope="function")
def totally_empty_data_context(tmp_path_factory):
    # NOTE: This sets up a DataContext with a real path and a config saved to that path.
    # Now that ConfigOnlyDataContext exists, it's possible to test most DataContext methods without touching the file system.
    # However, as of 2019/08/22, most tests still use filesystem-based fixtures.
    # TODO: Where appropriate, switch DataContext tests to the new method.
    project_root_dir = str(tmp_path_factory.mktemp('totally_empty_data_context'))
    os.mkdir(os.path.join(project_root_dir, 'great_expectations'))

    config = {
        "config_version": 1,
        "plugins_directory": "plugins/",
        "evaluation_parameter_store_name": "not_a_real_store_name",
        "validations_store_name": "another_fake_store",
        "expectations_store_name": "expectations_store",
        "datasources": {},
        "stores": {
            "expectations_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "FixedLengthTupleFilesystemStoreBackend",
                    "base_directory": "expectations/"
                }
            },
        },
        "data_docs_sites": {},
        "validation_operators": {}
    }
    with open(os.path.join(project_root_dir, "great_expectations/great_expectations.yml"), 'w') as config_file:
        yaml.dump(
            config,
            config_file
        )

    context = ge.data_context.DataContext(os.path.join(project_root_dir, "great_expectations"))
    # print(json.dumps(context._project_config, indent=2))
    return context


def test_create(tmp_path_factory):
    project_path = str(tmp_path_factory.mktemp('path_001'))
    context = ge.data_context.DataContext.create(project_path)

    assert isinstance(context, ge.data_context.DataContext)


def test_add_store(totally_empty_data_context):
    assert len(totally_empty_data_context.stores.keys()) == 1

    totally_empty_data_context.add_store(
        "my_inmemory_store",
        {
            "module_name": "great_expectations.data_context.store",
            "class_name": "BasicInMemoryStore"
        }
    )
    assert "my_inmemory_store" in totally_empty_data_context.stores.keys()
    assert len(totally_empty_data_context.stores.keys()) == 2


def test_config_from_absolute_zero(totally_empty_data_context):

    assert len(totally_empty_data_context.stores.keys()) == 1

    totally_empty_data_context.add_store(
        "my_inmemory_store",
        {
            "module_name": "great_expectations.data_context.store",
            "class_name": "BasicInMemoryStore",
            "serialization_type": "json"
        }
    )
    assert "my_inmemory_store" in totally_empty_data_context.stores.keys()
    assert len(totally_empty_data_context.stores.keys()) == 2


def test_default_config_yml_stores(tmp_path_factory):
    project_path = str(tmp_path_factory.mktemp('totally_empty_data_context'))
    context = ge.data_context.DataContext.create(project_path)

    assert set(context.stores.keys()) == {
            "expectations_store",
            "validations_store",
            "evaluation_parameter_store"
        }

    context.add_store(
        "my_new_in_memory_store",
        {
            "module_name": "great_expectations.data_context.store",
            "class_name": "BasicInMemoryStore",
            "serialization_type": "json",
        }
    )

    assert set(context.stores.keys()) == {
            "expectations_store",
            "validations_store",
            "evaluation_parameter_store",
            "my_new_in_memory_store"
        }