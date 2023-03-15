"""Test v3 API datasource serialization."""
import pytest

from great_expectations.data_context.types.base import (
    DatasourceConfig,
    datasourceConfigSchema,
)
from great_expectations.datasource.datasource_serializer import (
    YAMLReadyDictDatasourceConfigSerializer,
)
from tests.core.test_serialization import generic_config_serialization_assertions


@pytest.mark.unit
@pytest.mark.parametrize(
    "datasource_config,expected_serialized_datasource_config,expected_roundtrip_config",
    [
        pytest.param(
            DatasourceConfig(
                class_name="Datasource",
            ),
            {
                "class_name": "Datasource",
                "module_name": "great_expectations.datasource",
            },
            {
                "class_name": "Datasource",
                "module_name": "great_expectations.datasource",
                "name": None,
                "id": None,
            },
            id="minimal",
        ),
        pytest.param(
            DatasourceConfig(
                name="my_datasource",
                id="d3a14abd-d4cb-4343-806e-55b555b15c28",
                class_name="Datasource",
            ),
            {
                "name": "my_datasource",
                "id": "d3a14abd-d4cb-4343-806e-55b555b15c28",
                "class_name": "Datasource",
                "module_name": "great_expectations.datasource",
            },
            {
                "name": "my_datasource",
                "id": "d3a14abd-d4cb-4343-806e-55b555b15c28",
                "class_name": "Datasource",
                "module_name": "great_expectations.datasource",
            },
            id="minimal_with_name_and_id",
        ),
        pytest.param(
            DatasourceConfig(
                name="my_datasource",
                class_name="Datasource",
                data_connectors={
                    "my_data_connector": DatasourceConfig(
                        class_name="RuntimeDataConnector",
                        batch_identifiers=["default_identifier_name"],
                        id="dd8fe6df-254b-4e37-9c0e-2c8205d1e988",
                    )
                },
            ),
            {
                "name": "my_datasource",
                "class_name": "Datasource",
                "module_name": "great_expectations.datasource",
                "data_connectors": {
                    "my_data_connector": {
                        "class_name": "RuntimeDataConnector",
                        "module_name": "great_expectations.datasource",
                        "id": "dd8fe6df-254b-4e37-9c0e-2c8205d1e988",
                        "batch_identifiers": ["default_identifier_name"],
                    },
                },
            },
            {
                "name": "my_datasource",
                "class_name": "Datasource",
                "module_name": "great_expectations.datasource",
                "data_connectors": {
                    "my_data_connector": {
                        "class_name": "RuntimeDataConnector",
                        "module_name": "great_expectations.datasource",
                        "id": "dd8fe6df-254b-4e37-9c0e-2c8205d1e988",
                        "batch_identifiers": ["default_identifier_name"],
                        "name": "my_data_connector",
                    },
                },
                "id": None,
            },
            id="nested_data_connector_id",
        ),
        pytest.param(
            DatasourceConfig(
                name="my_datasource",
                class_name="Datasource",
                data_connectors={
                    "my_data_connector": DatasourceConfig(
                        class_name="RuntimeDataConnector",
                        batch_identifiers=["default_identifier_name"],
                        id="dd8fe6df-254b-4e37-9c0e-2c8205d1e988",
                    )
                },
                id="e25279d2-a809-4d1f-814c-76e892e645bf",
            ),
            {
                "name": "my_datasource",
                "class_name": "Datasource",
                "module_name": "great_expectations.datasource",
                "data_connectors": {
                    "my_data_connector": {
                        "class_name": "RuntimeDataConnector",
                        "module_name": "great_expectations.datasource",
                        "id": "dd8fe6df-254b-4e37-9c0e-2c8205d1e988",
                        "batch_identifiers": ["default_identifier_name"],
                    },
                },
                "id": "e25279d2-a809-4d1f-814c-76e892e645bf",
            },
            {
                "name": "my_datasource",
                "class_name": "Datasource",
                "module_name": "great_expectations.datasource",
                "data_connectors": {
                    "my_data_connector": {
                        "class_name": "RuntimeDataConnector",
                        "module_name": "great_expectations.datasource",
                        "id": "dd8fe6df-254b-4e37-9c0e-2c8205d1e988",
                        "batch_identifiers": ["default_identifier_name"],
                        "name": "my_data_connector",
                    },
                },
                "id": "e25279d2-a809-4d1f-814c-76e892e645bf",
            },
            id="datasource_id_and_nested_data_connector_id",
        ),
    ],
)
class TestDatasourceConfigSerialization:
    def test_is_serialized(
        self,
        datasource_config: DatasourceConfig,
        expected_serialized_datasource_config: dict,
        expected_roundtrip_config: dict,
    ):
        """Datasource Config should be serialized appropriately with/without optional params."""
        generic_config_serialization_assertions(
            datasource_config,
            datasourceConfigSchema,
            expected_serialized_datasource_config,
            expected_roundtrip_config,
        )

    def test_dict_round_trip_serialization(
        self,
        datasource_config: DatasourceConfig,
        expected_serialized_datasource_config: dict,
        expected_roundtrip_config: dict,
    ):
        observed_dump = datasourceConfigSchema.dump(datasource_config)

        round_tripped = DatasourceConfig._dict_round_trip(
            datasourceConfigSchema, observed_dump
        )

        assert round_tripped == expected_roundtrip_config

        assert (
            round_tripped.get("id")
            == observed_dump.get("id")
            == expected_serialized_datasource_config.get("id")
            == expected_roundtrip_config.get("id")
        )

        if (
            hasattr(datasource_config, "data_connectors")
            and datasource_config.data_connectors
        ):
            for (
                data_connector_name,
                data_connector_config,
            ) in datasource_config.data_connectors.items():
                assert (
                    observed_dump["data_connectors"][data_connector_name]["id"]
                    == round_tripped["data_connectors"][data_connector_name]["id"]
                    == data_connector_config.id
                )


def test_yaml_ready_dict_datasource_config_serializer(
    datasource_config_with_names: DatasourceConfig,
):
    """Make sure the name fields are appropriately removed from datasource and data_connector."""

    serializer = YAMLReadyDictDatasourceConfigSerializer(schema=datasourceConfigSchema)

    assert serializer.serialize(datasource_config_with_names) == {
        "class_name": "Datasource",
        "data_connectors": {
            "tripdata_monthly_configured": {
                "assets": {
                    "yellow": {
                        "class_name": "Asset",
                        "group_names": ["year", "month"],
                        "module_name": "great_expectations.datasource.data_connector.asset",
                        "pattern": "yellow_tripdata_(\\d{4})-(\\d{2})\\.csv$",
                    }
                },
                "base_directory": "/path/to/trip_data",
                "class_name": "ConfiguredAssetFilesystemDataConnector",
                "module_name": "great_expectations.datasource.data_connector",
            }
        },
        "execution_engine": {
            "class_name": "PandasExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
        "module_name": "great_expectations.datasource",
    }
