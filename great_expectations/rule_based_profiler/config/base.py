import copy
import dataclasses
import logging
from typing import Any, Dict, List, Optional, Type

from great_expectations.data_context.types.base import BaseYamlConfig
from great_expectations.marshmallow__shade import INCLUDE, Schema, fields, post_load
from great_expectations.marshmallow__shade.decorators import post_dump
from great_expectations.types import DictDot

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class NotNullSchema(Schema):
    """
    TODO(cdkini): Write docstr!
    """

    @post_load
    def make_config(self, data: dict, **kwargs) -> Type[DictDot]:
        """
        TODO(cdkini): Write docstr!
        """
        if not hasattr(self, "__config__"):
            raise NotImplementedError(
                "The subclass extending NotNullSchema must define its own custom __config__"
            )

        recognized_attrs = {f.name for f in dataclasses.fields(self.__config__)}
        cleaned_data = copy.deepcopy(data)
        for k, v in data.items():
            if k not in recognized_attrs:
                del cleaned_data[k]
                logger.info(
                    f"Ignoring unknown key-value pair during instantiation: (%s, %s)",
                    k,
                    v,
                )
            elif v is not None:
                del cleaned_data[k]
                logger.info(f"Ignoring '%s' due to null value.", k)

        return self.__config__(**cleaned_data)

    @post_dump
    def remove_nulls(self, data: dict, **kwargs) -> dict:
        """
        TODO(cdkini): Write docstr!
        """
        res = copy.deepcopy(data)
        for k, v in data.items():
            if v is None:
                res.pop(k)
                logger.info("Removed '%s' due to null value", k)
        return res


@dataclasses.dataclass(frozen=True)
class DomainBuilderConfig(DictDot):
    class_name: str
    module_name: Optional[str] = None
    batch_request: Optional[Dict[str, Any]] = None


class DomainBuilderConfigSchema(NotNullSchema):
    class Meta:
        unknown = INCLUDE

    __config__ = DomainBuilderConfig

    class_name = fields.String(required=True)
    module_name = fields.String(
        required=False,
        all_none=True,
        missing="great_expectations.rule_based_profiler.domain_builder",
    )
    batch_request = fields.Dict(keys=fields.String(), required=False, allow_none=True)


@dataclasses.dataclass(frozen=True)
class ParameterBuilderConfig(DictDot):
    parameter_name: str
    class_name: str
    module_name: Optional[str] = None
    batch_request: Optional[Dict[str, Any]] = None


class ParameterBuilderConfigSchema(NotNullSchema):
    class Meta:
        unknown = INCLUDE

    __config__ = ParameterBuilderConfig

    class_name = fields.String(required=True)
    module_name = fields.String(
        required=False,
        all_none=True,
        missing="great_expectations.rule_based_profiler.parameter_builder",
    )
    parameter_name = fields.String(required=True)
    batch_request = fields.Dict(keys=fields.String(), required=False, allow_none=True)


@dataclasses.dataclass(frozen=True)
class ExpectationConfigurationBuilderConfig(DictDot):
    expectation_type: str
    class_name: str
    module_name: Optional[str] = None
    mostly: Optional[float] = None
    meta: Optional[Dict] = None


class ExpectationConfigurationBuilderConfigSchema(NotNullSchema):
    class Meta:
        unknown = INCLUDE

    __config__ = ExpectationConfigurationBuilderConfig

    class_name = fields.String(required=True)
    module_name = fields.String(
        required=False,
        all_none=True,
        missing="great_expectations.rule_based_profiler.expectation_configuration_builder",
    )
    expectation_type = fields.String(required=True)
    mostly = fields.Float(required=False, allow_none=True)
    meta = fields.Dict(required=False, allow_none=True)


@dataclasses.dataclass(frozen=True)
class RuleConfig(DictDot):
    name: str
    domain_builder: DomainBuilderConfig
    parameter_builders: List[ParameterBuilderConfig]
    expectation_configuration_builders: List[ExpectationConfigurationBuilderConfig]


class RuleConfigSchema(NotNullSchema):
    class Meta:
        unknown = INCLUDE

    __config__ = RuleConfig

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


@dataclasses.dataclass(frozen=True)
class RuleBasedProfilerConfig(BaseYamlConfig):
    name: str
    config_version: float
    rules: Dict[str, RuleConfig]
    variables: Optional[Dict[str, Any]] = None


class RuleBasedProfilerConfigSchema(NotNullSchema):
    class Meta:
        unknown = INCLUDE

    __config__ = RuleBasedProfilerConfig

    name = fields.String(required=True)
    config_version = fields.Float(required=True)
    variables = fields.Dict(keys=fields.String(), required=False, allow_none=True)
    rules = fields.Dict(
        keys=fields.String(),
        values=fields.Nested(RuleConfigSchema, required=True),
        required=True,
    )
