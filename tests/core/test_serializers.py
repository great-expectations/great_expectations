import pytest
from marshmallow import Schema

from great_expectations.core.configuration import AbstractConfigSchema
from great_expectations.core.serializer import (
    DictConfigSerializer,
    JsonConfigSerializer,
)


@pytest.fixture
def empty_abstract_config_schema():
    class EmptyAbstractConfigSchema(AbstractConfigSchema):
        pass

    schema = EmptyAbstractConfigSchema()
    return schema


@pytest.mark.unit
def test_init_dict_config_serializer(empty_abstract_config_schema: Schema):
    serializer = DictConfigSerializer(schema=empty_abstract_config_schema)

    assert serializer.schema == empty_abstract_config_schema


@pytest.mark.unit
def test_init_json_config_serializer(empty_abstract_config_schema: Schema):
    serializer = JsonConfigSerializer(schema=empty_abstract_config_schema)

    assert serializer.schema == empty_abstract_config_schema
