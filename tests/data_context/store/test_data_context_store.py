from unittest import mock

import pytest

from great_expectations.data_context.store.data_context_store import DataContextStore
from great_expectations.data_context.types.base import DataContextConfig


@pytest.mark.unit
def test_serialize(basic_data_context_config: DataContextConfig):
    store = DataContextStore(store_name="data_context_store")

    actual = store.serialize(basic_data_context_config)
    expected = basic_data_context_config.to_yaml_str()

    assert actual == expected


@pytest.mark.cloud
def test_serialize_cloud_mode(basic_data_context_config: DataContextConfig):
    store = DataContextStore(store_name="data_context_store")

    with mock.patch(
        "great_expectations.data_context.store.DataContextStore.cloud_mode"
    ) as mock_cloud_mode:
        type(mock_cloud_mode.return_value).ok = mock.PropertyMock(return_value=True)
        actual = store.serialize(basic_data_context_config)

    expected = {
        "analytics_enabled": True,
        "data_context_id": "6a52bdfa-e182-455b-a825-e69f076e67d6",
        "config_variables_file_path": "uncommitted/config_variables.yml",
        "config_version": 2.0,
        "data_docs_sites": {},
        "plugins_directory": "plugins/",
        "stores": {
            "checkpoint_store": {
                "class_name": "CheckpointStore",
                "store_backend": {
                    "base_directory": "checkpoints/",
                    "class_name": "TupleFilesystemStoreBackend",
                },
            },
            "expectations_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "base_directory": "expectations/",
                    "class_name": "TupleFilesystemStoreBackend",
                },
            },
            "validation_definition_store": {
                "class_name": "ValidationDefinitionStore",
            },
            "validation_results_store": {
                "class_name": "ValidationResultsStore",
            },
        },
    }

    assert actual == expected
