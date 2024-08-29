import os

import pytest

import great_expectations as gx
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)

yaml = YAMLHandler()


@pytest.fixture(scope="function")
def totally_empty_data_context(tmp_path_factory):
    # NOTE: This sets up a DataContext with a real path and a config saved to that path.
    # Now that BaseDataContext exists, it's possible to test most DataContext methods without touching the file system.  # noqa: E501
    # However, as of 2019/08/22, most tests still use filesystem-based fixtures.
    # TODO: Where appropriate, switch DataContext tests to the new method.
    project_root_dir = str(tmp_path_factory.mktemp("totally_empty_data_context"))
    os.mkdir(  # noqa: PTH102
        os.path.join(project_root_dir, FileDataContext.GX_DIR)  # noqa: PTH118
    )

    config = {
        "config_version": 3,
        "plugins_directory": "plugins/",
        "validation_results_store_name": "another_fake_store",
        "expectations_store_name": "expectations_store",
        "checkpoint_store_name": "checkpoint_store",
        "stores": {
            "expectations_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": "expectations/",
                },
            },
            "checkpoint_store": {
                "class_name": "CheckpointStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": "checkpoints/",
                },
            },
        },
        "data_docs_sites": {},
    }
    with open(
        os.path.join(project_root_dir, "gx/great_expectations.yml"),  # noqa: PTH118
        "w",
    ) as config_file:
        yaml.dump(config, config_file)

    context = gx.get_context(
        context_root_dir=os.path.join(  # noqa: PTH118
            project_root_dir, FileDataContext.GX_DIR
        )
    )
    # print(json.dumps(context._project_config, indent=2))
    return context


@pytest.mark.filesystem
def test_add_store(totally_empty_data_context):
    assert len(totally_empty_data_context.stores.keys()) == 4

    totally_empty_data_context.add_store(
        "my_new_store",
        {
            "module_name": "great_expectations.data_context.store",
            "class_name": "ValidationResultsStore",
        },
    )
    assert "my_new_store" in totally_empty_data_context.stores
    assert len(totally_empty_data_context.stores.keys()) == 5


@pytest.mark.filesystem
def test_default_config_yml_stores(tmp_path_factory):
    project_path = str(tmp_path_factory.mktemp("totally_empty_data_context"))
    context = gx.get_context(project_root_dir=project_path)

    assert set(context.stores.keys()) == {
        "expectations_store",
        "validation_results_store",
        "checkpoint_store",
        "validation_definition_store",
    }

    context.add_store(
        "my_new_validation_results_store",
        {
            "module_name": "great_expectations.data_context.store",
            "class_name": "ValidationResultsStore",
        },
    )

    assert set(context.stores.keys()) == {
        "checkpoint_store",
        "expectations_store",
        "validation_results_store",
        "validation_definition_store",
        "my_new_validation_results_store",
    }
