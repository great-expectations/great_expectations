import pytest

from great_expectations.core.configuration import AbstractConfigSchema

from great_expectations.core.serializer import DictConfigSerializer
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


@pytest.mark.xfail(
    raises=(ImportError, NameError),
    reason="xfail until PR 5778 merges with JsonConfigSerializer",
    strict=True,
)
def test_init_json_config_serializer(empty_abstract_config_schema: Schema):

    # Note: move this import statement to the top of the file once this test is meant to pass
    from great_expectations.core.serializer import JsonConfigSerializer

    serializer = JsonConfigSerializer(schema=empty_abstract_config_schema)

    assert serializer.schema == empty_abstract_config_schema
