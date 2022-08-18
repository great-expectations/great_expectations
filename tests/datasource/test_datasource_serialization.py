"""Test v3 API datasource serialization."""
import pytest

from great_expectations.data_context.types.base import (
    DatasourceConfig,
    datasourceConfigSchema,
)


@pytest.mark.unit
@pytest.mark.parametrize(
    "datasource_config,expected_serialized_datasource_config",
    [
        pytest.param(
            DatasourceConfig(
                class_name="Datasource",
            ),
            {
                "class_name": "Datasource",
                "module_name": "great_expectations.datasource",
            },
            id="minimal",
        ),
        pytest.param(
            DatasourceConfig(
                name="my_datasource",
                id_="d3a14abd-d4cb-4343-806e-55b555b15c28",
                class_name="Datasource",
            ),
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
                        id_="dd8fe6df-254b-4e37-9c0e-2c8205d1e988",
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
            id="nested_data_connector_id",
        ),
    ],
)
class TestDatasourceConfigSerialization:
    def test_is_serialized(
        self,
        datasource_config: DatasourceConfig,
        expected_serialized_datasource_config: dict,
    ):
        """Datasource Config should be serialized appropriately with/without optional params."""
        observed_dump = datasourceConfigSchema.dump(datasource_config)
        assert observed_dump == expected_serialized_datasource_config

        loaded_data = datasourceConfigSchema.load(observed_dump)
        observed_load = DatasourceConfig(**loaded_data)
        assert observed_load.to_json_dict() == datasource_config.to_json_dict()

    def test_dict_round_trip_serialization(
        self,
        datasource_config: DatasourceConfig,
        expected_serialized_datasource_config: dict,
    ):
        observed_dump = datasourceConfigSchema.dump(datasource_config)

        round_tripped = DatasourceConfig._dict_round_trip(
            datasourceConfigSchema, observed_dump
        )

        assert round_tripped == datasource_config.to_json_dict()

        assert (
            round_tripped.get("id_")
            == observed_dump.get("id")
            == expected_serialized_datasource_config.get("id")
        )
