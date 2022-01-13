import dataclasses
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Type

from ruamel.yaml.comments import CommentedMap

from great_expectations.data_context.types.base import BaseYamlConfig
from great_expectations.marshmallow__shade import INCLUDE, Schema, fields, post_load
from great_expectations.marshmallow__shade.decorators import post_dump
from great_expectations.types import DictDot
from great_expectations.util import filter_properties_dict


class NotNullSchema(Schema):
    """
    Extension of Marshmallow Schema to facilitate implicit removal of null values before serialization.

    The __config__ attribute is utilized to point a Schema to a configuration. It is the responsibility
    of the child class to define its own __config__ to ensure proper serialization/deserialization.

    Reference: https://marshmallow.readthedocs.io/en/stable/extending.html

    """

    @post_load
    def make_config(self, data: dict, **kwargs) -> Type[DictDot]:
        """Hook to convert the schema object into its respective config type.

        Checks against config dataclass signature to ensure that unidentified kwargs are omitted
        from the result object. This design allows us to maintain forwards comptability without
        altering expected behavior.

        Args:
            data: The dictionary representation of the configuration object
            kwargs: Marshmallow-specific kwargs required to maintain hook signature (unused herein)

        Returns:
            An instance of configuration class, which subclasses the DictDot serialization class

        Raises:
            NotImplementedError: If the subclass inheriting NotNullSchema fails to define a __config__

        """
        if not hasattr(self, "__config__"):
            raise NotImplementedError(
                "The subclass extending NotNullSchema must define its own custom __config__"
            )

        # Removing **kwargs before creating config object
        recognized_attrs = {f.name for f in dataclasses.fields(self.__config__)}
        cleaned_data = filter_properties_dict(
            properties=data,
            keep_fields=recognized_attrs,
            clean_nulls=False,
            clean_falsy=False,
        )

        return self.__config__(**cleaned_data)

    @post_dump
    def remove_nulls(self, data: dict, **kwargs) -> dict:
        """Hook to clear the config object of any null values before being written as a dictionary.

        Args:
            data: The dictionary representation of the configuration object
            kwargs: Marshmallow-specific kwargs required to maintain hook signature (unused herein)

        Returns:
            A cleaned dictionary that has no null values

        """
        cleaned_data = filter_properties_dict(
            properties=data,
            clean_nulls=True,
            clean_falsy=False,
        )
        return cleaned_data


@dataclass(frozen=True)
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


@dataclass(frozen=True)
class ParameterBuilderConfig(DictDot):
    name: str
    class_name: str
    module_name: Optional[str] = None
    batch_request: Optional[Dict[str, Any]] = None


class ParameterBuilderConfigSchema(NotNullSchema):
    class Meta:
        unknown = INCLUDE

    __config__ = ParameterBuilderConfig

    name = fields.String(required=True)
    class_name = fields.String(required=True)
    module_name = fields.String(
        required=False,
        all_none=True,
        missing="great_expectations.rule_based_profiler.parameter_builder",
    )
    batch_request = fields.Dict(keys=fields.String(), required=False, allow_none=True)


@dataclass(frozen=True)
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


@dataclass(frozen=True)
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


@dataclass
class RuleBasedProfilerConfig(BaseYamlConfig):
    name: str
    config_version: float
    rules: Dict[str, RuleConfig]
    variables: Optional[Dict[str, Any]] = None
    commented_map: Optional[CommentedMap] = None

    def __post_init__(self):
        # Required to fully set up the commented map and enable serialization
        super().__init__(commented_map=self.commented_map)

    @classmethod
    def get_config_class(cls) -> Type["RuleBasedProfilerConfig"]:
        return cls

    @classmethod
    def get_schema_class(cls) -> Type["RuleBasedProfilerConfigSchema"]:
        return RuleBasedProfilerConfigSchema


class RuleBasedProfilerConfigSchema(NotNullSchema):
    class Meta:
        unknown = INCLUDE

    __config__ = RuleBasedProfilerConfig

    name = fields.String(required=True)
    config_version = fields.Float(
        required=True,
        validate=lambda x: x == 1.0,
        error_messages={
            "Invalid: config version is not supported; it must be 1.0 per the current version of Great Expectations"
        },
    )
    variables = fields.Dict(keys=fields.String(), required=False, allow_none=True)
    rules = fields.Dict(
        keys=fields.String(),
        values=fields.Nested(RuleConfigSchema, required=True),
        required=True,
    )
