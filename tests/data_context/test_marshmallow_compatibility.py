import pytest
from marshmallow import ValidationError

from great_expectations.data_context.types.base import (
    AssetConfigSchema,
    DataConnectorConfigSchema,
    DataContextConfigSchema,
    ExecutionEngineConfigSchema,
    SorterConfigSchema,
)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("schema_class", "data", "expected_defaults"),
    [
        (
            SorterConfigSchema,
            {"name": "sorter", "class_name": "Sorter"},
            {
                "module_name": "great_expectations.datasource.data_connector.sorter",
                "orderby": "asc",
                "reference_list": None,
                "key_reference_list": None,
                "datetime_format": None,
            },
        ),
        (
            AssetConfigSchema,
            {},
            {
                "class_name": "Asset",
                "module_name": "great_expectations.datasource.data_connector.asset",
            },
        ),
        (
            DataConnectorConfigSchema,
            {"class_name": "DataConnector"},
            {"module_name": "great_expectations.datasource.data_connector"},
        ),
        (
            ExecutionEngineConfigSchema,
            {"class_name": "ExecutionEngine"},
            {"module_name": "great_expectations.execution_engine"},
        ),
    ],
)
def test_configuration_schema_defaults_are_preserved(schema_class, data, expected_defaults):
    schema = schema_class()
    config = schema.load(data)

    for attribute, expected in expected_defaults.items():
        field = schema.fields[attribute]
        assert field.load_default == expected
        if expected is not None:
            assert getattr(config, attribute) == expected


@pytest.mark.unit
def test_asset_config_accepts_explicit_null_module_name():
    config = AssetConfigSchema().load({"module_name": None})

    assert config.module_name is None


@pytest.mark.unit
def test_config_version_accepts_valid_values():
    field = DataContextConfigSchema().fields["config_version"]

    assert field.deserialize(3) == 3
    assert field.deserialize(3.5) == 3.5


@pytest.mark.unit
@pytest.mark.parametrize("value", [0, -1, 100, 101])
def test_config_version_rejects_values_outside_open_interval(value):
    field = DataContextConfigSchema().fields["config_version"]

    with pytest.raises(ValidationError):
        field.deserialize(value)
