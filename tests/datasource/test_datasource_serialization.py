"""Test v3 API datasource serialization."""
import pytest

from great_expectations.data_context.types.base import (
    DatasourceConfig,
    datasourceConfigSchema,
)


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
    ],
)
def test_datasource_config_is_serialized(
    datasource_config: DatasourceConfig, expected_serialized_datasource_config: dict
):
    """Datasource Config should be serialized appropriately with/without optional params."""
    observed = datasourceConfigSchema.dump(datasource_config)

    assert observed == expected_serialized_datasource_config
