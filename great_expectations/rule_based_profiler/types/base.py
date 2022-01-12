from typing import Any, Dict, List, Optional

from great_expectations.marshmallow__shade import INCLUDE, Schema, fields
from great_expectations.types import DictDot


class DomainBuilderConfig(DictDot):
    def __init__(
        self,
        class_name: str,
        module_name: Optional[str] = None,
        batch_request: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        self._class_name = class_name
        self._module_name = module_name
        self.batch_request = batch_request
        for k, v in kwargs.items():
            setattr(self, k, v)

    @property
    def class_name(self) -> str:
        return self._class_name

    @property
    def module_name(self) -> Optional[str]:
        return self._module_name


class DomainBuilderConfigSchema(Schema):
    class Meta:
        unknown = INCLUDE

    class_name = fields.String(required=True)
    module_name = fields.String(required=False, allow_none=True)
    batch_request = fields.Dict(keys=fields.String(), required=False, allow_none=True)


class ParameterBuilderConfig(DictDot):
    def __init__(
        self,
        parameter_name: str,
        class_name: str,
        module_name: Optional[str] = None,
        batch_request: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        self.parameter_name = parameter_name
        self._class_name = class_name
        self._module_name = module_name
        self.batch_request = batch_request
        for k, v in kwargs.items():
            setattr(self, k, v)

    @property
    def class_name(self) -> str:
        return self._class_name

    @property
    def module_name(self) -> Optional[str]:
        return self._module_name


class ParameterBuilderConfigSchema(Schema):
    class Meta:
        unknown = INCLUDE

    class_name = fields.String(required=True)
    module_name = fields.String(required=False, allow_none=True)
    parameter_name = fields.String(required=True)
    batch_request = fields.Dict(keys=fields.String(), required=False, allow_none=True)


class ExpectationConfigurationBuilderConfig(DictDot):
    def __init__(
        self,
        expectation_type: str,
        class_name: str,
        module_name: Optional[str] = None,
        mostly: Optional[float] = None,
        meta: Optional[str] = None,
        **kwargs,
    ):
        self.expectation_type = expectation_type
        self._class_name = class_name
        self._module_name = module_name
        self.mostly = mostly
        self.meta = meta
        for k, v in kwargs.items():
            setattr(self, k, v)

    @property
    def class_name(self) -> str:
        return self._class_name

    @property
    def module_name(self) -> Optional[str]:
        return self._module_name


class ExpectationConfigurationBuilderConfigSchema(Schema):
    class Meta:
        unknown = INCLUDE

    class_name = fields.String(required=True)
    module_name = fields.String(required=False, allow_none=True)
    expectation_type = fields.String(required=True)
    mostly = fields.Float(required=False, allow_none=True)
    meta = fields.String(required=False, allow_none=True)


class RuleConfig(DictDot):
    def __init__(
        self,
        name: str,
        domain_builder: DomainBuilderConfig,
        parameter_builders: List[ParameterBuilderConfig],
        expectation_configuration_builders: List[ExpectationConfigurationBuilderConfig],
    ):
        self.name = name
        self.domain_builder = domain_builder
        self.parameter_builder = parameter_builders
        self.expectation_configuration_builders = expectation_configuration_builders


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


class RuleBasedProfilerConfig(DictDot):
    def __init__(
        self,
        name: str,
        config_version: float,
        rules: Dict[str, RuleConfig],
        variables: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        self.name = name
        self.config_version = config_version
        self.rules = rules
        if variables is None:
            variables = {}
        self.variables = variables
        for k, v in kwargs.items():
            setattr(self, k, v)


class RuleBasedProfilerConfigSchema(Schema):
    class Meta:
        unknown = INCLUDE

    name = fields.String(required=True)
    config_version = fields.Float(required=True)
    variables = fields.Dict(keys=fields.String(), required=False, allow_none=True)
    rules = fields.Dict(
        keys=fields.String(),
        values=fields.Nested(RuleConfigSchema, required=True),
        required=True,
    )


ruleBasedProfilerConfigSchema = RuleBasedProfilerConfigSchema()
