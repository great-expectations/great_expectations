import pytest

from great_expectations.core.configuration import AbstractConfigSchema
from great_expectations.core.serializer import (
    DictConfigSerializer,
    JsonConfigSerializer,
)
from great_expectations.marshmallow__shade import Schema


@pytest.fixture
def empty_abstract_config_schema():
    class EmptyAbstractConfigSchema(AbstractConfigSchema):
        pass

    schema = EmptyAbstractConfigSchema()
    return schema


def test_init_dict_config_serializer(empty_abstract_config_schema: Schema):
    serializer = DictConfigSerializer(schema=empty_abstract_config_schema)

    assert serializer.schema == empty_abstract_config_schema


def test_init_json_config_serializer(empty_abstract_config_schema: Schema):

    serializer = JsonConfigSerializer(schema=empty_abstract_config_schema)

    assert serializer.schema == empty_abstract_config_schema
