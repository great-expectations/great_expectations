"""Test v3 API data connector serialization."""

import pytest

from great_expectations.data_context.types.base import (
    DataConnectorConfig,
    dataConnectorConfigSchema,
)


@pytest.mark.unit
@pytest.mark.parametrize(
    "data_connector_config,expected_serialized_data_connector_config",
    [
        pytest.param(
            DataConnectorConfig(
                class_name="RuntimeDataConnector",
                batch_identifiers=["default_identifier_name"],
            ),
            {
                "class_name": "RuntimeDataConnector",
                "module_name": None,
                "batch_identifiers": ["default_identifier_name"],
            },
            id="no id",
        ),
        pytest.param(
            DataConnectorConfig(
                class_name="RuntimeDataConnector",
                id="dd8fe6df-254b-4e37-9c0e-2c8205d1e988",
                batch_identifiers=["default_identifier_name"],
            ),
            {
                "class_name": "RuntimeDataConnector",
                "module_name": None,
                "id": "dd8fe6df-254b-4e37-9c0e-2c8205d1e988",
                "batch_identifiers": ["default_identifier_name"],
            },
            id="with id",
        ),
    ],
)
def test_data_connector_config_is_serialized(
    data_connector_config: DataConnectorConfig,
    expected_serialized_data_connector_config: dict,
):
    """DataConnector Config should be serialized appropriately with/without optional params."""
    observed_dump = dataConnectorConfigSchema.dump(data_connector_config)
    assert observed_dump == expected_serialized_data_connector_config

    observed_load = dataConnectorConfigSchema.load(observed_dump)
    assert observed_load.to_json_dict() == data_connector_config.to_json_dict()
