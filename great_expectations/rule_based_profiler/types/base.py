from great_expectations.marshmallow__shade import INCLUDE, Schema, fields
from great_expectations.types import SerializableDictDot


class DomainBuilderConfig(SerializableDictDot):
    pass


class DomainBuilderConfigSchema(Schema):
    class Meta:
        unknown = INCLUDE

    class_name = fields.String(required=True)
    module_name = fields.String(required=False, allow_none=True)
    ...


class ParameterBuilderConfig(SerializableDictDot):
    pass


class ParameterBuilderConfigSchema(Schema):
    class Meta:
        unknown = INCLUDE

    class_name = fields.String(required=True)
    module_name = fields.String(required=False, allow_none=True)
    parameter_name = fields.String(required=True)
    ...


class ExpectationConfigurationBuilderConfig(SerializableDictDot):
    pass


class ExpectationConfigurationBuilderConfigSchema(Schema):
    class Meta:
        unknown = INCLUDE

    class_name = fields.String(required=True)
    module_name = fields.String(required=False, allow_none=True)
    expectation_type = fields.String(required=True)
    ...


class RuleConfig(SerializableDictDot):
    pass


class RuleConfigSchema(Schema):
    class Meta:
        unknown = INCLUDE

    name = fields.String(required=True)
    domain_builder = fields.Nested(DomainBuilderConfigSchema, required=True)
    parameter_builders = fields.List(
        cls_or_instance=fields.Nested(ParameterBuilderConfigSchema, required=True),
        required=True,
    )
    expectation_configuration_builders = fields.List(
        cls_or_instance=fields.Nested(
            ExpectationConfigurationBuilderConfigSchema, required=True
        ),
        required=True,
    )


class RuleBasedProfilerConfig(SerializableDictDot):
    pass


class RuleBasedProfilerConfigSchema(Schema):
    class Meta:
        unknown = INCLUDE

    name = fields.String(required=True)
    config_version = fields.Integer(required=True)
    variables = fields.Dict(keys=fields.Str(), required=False, allow_none=True)
    rules = fields.Dict(
        keys=fields.Str(),
        values=fields.Nested(RuleConfigSchema, required=False, allow_none=True),
        required=False,
        allow_none=True,
    )
