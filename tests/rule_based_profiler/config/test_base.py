from great_expectations.rule_based_profiler.config.base import (
    DomainBuilderConfig,
    DomainBuilderConfigSchema,
)


def test_DomainBuilderConfigSchema_valid_load():
    data = {"class_name": "DomainBuilder"}
    schema = DomainBuilderConfigSchema()
    config = schema.load(data)
    assert isinstance(config, DomainBuilderConfig)


def test_DomainBuilderConfigSchema_invalid_load():
    data = {"klass": "DomainBuilder"}
    schema = DomainBuilderConfigSchema()

    config = schema.load(data)
    assert isinstance(config, DomainBuilderConfig)


"""
great_expectations.marshmallow__shade.exceptions.ValidationError: {'class_name': ['Missing data for required field.']}
"""
