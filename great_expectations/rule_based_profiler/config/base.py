import copy
import logging
from typing import Any, Dict, List, Optional

from great_expectations.data_context.types.base import BaseYamlConfig
from great_expectations.marshmallow__shade import INCLUDE, Schema, fields, post_load
from great_expectations.marshmallow__shade.decorators import post_dump
from great_expectations.types import DictDot

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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
            logger.info(
                'Set "self.%s = %s" during DomainBuilderConfig instantiation', k, v
            )

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
    module_name = fields.String(
        required=False,
        all_none=True,
        missing="great_expectations.rule_based_profiler.domain_builder",
    )
    batch_request = fields.Dict(keys=fields.String(), required=False, allow_none=True)

    @post_load
    def make_config(self, data: dict, **kwargs) -> DomainBuilderConfig:
        return DomainBuilderConfig(**data)

    @post_dump
    def remove_nulls(self, data: dict, **kwargs) -> dict:
        res = copy.deepcopy(data)
        for k, v in data.items():
            if v is None:
                res.pop(k)
                logger.info("Removed '%s' due to null value", k)
        return res


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
            logger.info(
                'Set "self.%s = %s" during ParameterBuilderConfig instantiation', k, v
            )

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
    module_name = fields.String(
        required=False,
        all_none=True,
        missing="great_expectations.rule_based_profiler.parameter_builder",
    )
    parameter_name = fields.String(required=True)
    batch_request = fields.Dict(keys=fields.String(), required=False, allow_none=True)

    @post_load
    def make_config(self, data: dict, **kwargs) -> ParameterBuilderConfig:
        return ParameterBuilderConfig(**data)

    @post_dump
    def remove_nulls(self, data: dict, **kwargs) -> dict:
        res = copy.deepcopy(data)
        for k, v in data.items():
            if v is None:
                res.pop(k)
                logger.info("Removed '%s' due to null value", k)
        return res


class ExpectationConfigurationBuilderConfig(DictDot):
    def __init__(
        self,
        expectation_type: str,
        class_name: str,
        module_name: Optional[str] = None,
        mostly: Optional[float] = None,
        meta: Optional[Dict] = None,
        **kwargs,
    ):
        self.expectation_type = expectation_type
        self._class_name = class_name
        self._module_name = module_name
        self.mostly = mostly
        self.meta = meta or {}
        for k, v in kwargs.items():
            setattr(self, k, v)
            logger.info(
                'Set "self.%s = %s" during ExpectationConfigurationBuilderConfig instantiation',
                k,
                v,
            )

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
    module_name = fields.String(
        required=False,
        all_none=True,
        missing="great_expectations.rule_based_profiler.expectation_configuration_builder",
    )
    expectation_type = fields.String(required=True)
    mostly = fields.Float(required=False, allow_none=True)
    meta = fields.Dict(required=False, allow_none=True)

    @post_load
    def make_config(
        self, data: dict, **kwargs
    ) -> ExpectationConfigurationBuilderConfig:
        return ExpectationConfigurationBuilderConfig(**data)

    @post_dump
    def remove_nulls(self, data: dict, **kwargs) -> dict:
        res = copy.deepcopy(data)
        for k, v in data.items():
            if v is None:
                res.pop(k)
                logger.info("Removed '%s' due to null value", k)
        return res


class RuleConfig(DictDot):
    def __init__(
        self,
        name: str,
        domain_builder: DomainBuilderConfig,
        parameter_builders: List[ParameterBuilderConfig],
        expectation_configuration_builders: List[ExpectationConfigurationBuilderConfig],
        **kwargs,
    ):
        self.name = name
        self.domain_builder = domain_builder
        self.parameter_builders = parameter_builders
        self.expectation_configuration_builders = expectation_configuration_builders
        for k, v in kwargs.items():
            setattr(self, k, v)
            logger.info(
                'Set "self.%s = %s" during RuleConfig instantiation',
                k,
                v,
            )


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

    @post_load
    def make_config(self, data: dict, **kwargs) -> RuleConfig:
        return RuleConfig(**data)

    @post_dump
    def remove_nulls(self, data: dict, **kwargs) -> dict:
        res = copy.deepcopy(data)
        for k, v in data.items():
            if v is None:
                res.pop(k)
                logger.info("Removed '%s' due to null value", k)
        return res


class RuleBasedProfilerConfig(BaseYamlConfig):
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
        self.variables = variables or {}
        for k, v in kwargs.items():
            setattr(self, k, v)
            logger.info(
                'Set "self.%s = %s" during RuleBasedProfilerConfig instantiation',
                k,
                v,
            )


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

    @post_load
    def make_config(self, data: dict, **kwargs) -> RuleBasedProfilerConfig:
        return RuleBasedProfilerConfig(**data)

    @post_dump
    def remove_nulls(self, data: dict, **kwargs) -> dict:
        res = copy.deepcopy(data)
        for k, v in data.items():
            if v is None:
                res.pop(k)
                logger.info("Removed '%s' due to null value", k)
        return res
