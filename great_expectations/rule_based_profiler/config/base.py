import logging
from typing import Any, Dict, List, Optional, Type, Union

from ruamel.yaml.comments import CommentedMap

from great_expectations.data_context.types.base import BaseYamlConfig
from great_expectations.marshmallow__shade import (
    INCLUDE,
    Schema,
    fields,
    post_dump,
    post_load,
)
from great_expectations.types import DictDot
from great_expectations.util import filter_properties_dict

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class NotNullSchema(Schema):
    """
    Extension of Marshmallow Schema to facilitate implicit removal of null values before serialization.

    The __config_class__ attribute is utilized to point a Schema to a configuration. It is the responsibility
    of the child class to define its own __config_class__ to ensure proper serialization/deserialization.

    Reference: https://marshmallow.readthedocs.io/en/stable/extending.html

    """

    # noinspection PyUnusedLocal
    @post_load
    def make_config(self, data: dict, **kwargs) -> Type[DictDot]:
        """Hook to convert the schema object into its respective config type.

        Args:
            data: The dictionary representation of the configuration object
            kwargs: Marshmallow-specific kwargs required to maintain hook signature (unused herein)

        Returns:
            An instance of configuration class, which subclasses the DictDot serialization class

        Raises:
            NotImplementedError: If the subclass inheriting NotNullSchema fails to define a __config_class__

        """
        if not hasattr(self, "__config_class__"):
            raise NotImplementedError(
                "The subclass extending NotNullSchema must define its own custom __config_class__"
            )

        # noinspection PyUnresolvedReferences
        return self.__config_class__(**data)

    # noinspection PyUnusedLocal
    @post_dump(pass_original=True)
    def remove_nulls_and_keep_unknowns(
        self, output: dict, original: Type[DictDot], **kwargs
    ) -> dict:
        """Hook to clear the config object of any null values before being written as a dictionary.
        Additionally, it bypasses strict schema validation before writing to dict to ensure that dynamic
        attributes set through `setattr` are captured in the resulting object.
        It is important to note that only public attributes are captured through this process.
        Chetan - 20220126 - Note that if we tighten up the schema (remove the dynamic `setattr` behavior),
        the functionality to keep unknowns should also be removed.

        Args:
            output: Processed dictionary representation of the configuration object (leaving original intact)
            original: The dictionary representation of the configuration object
            kwargs: Marshmallow-specific kwargs required to maintain hook signature (unused herein)

        Returns:
            A cleaned dictionary that has no null values
        """
        for key in original.keys():
            if key not in output and not key.startswith("_"):
                output[key] = original[key]

        cleaned_output = filter_properties_dict(
            properties=output,
            clean_nulls=True,
            clean_falsy=False,
        )

        return cleaned_output


class DomainBuilderConfig(DictDot):
    def __init__(
        self,
        class_name: str,
        module_name: Optional[str] = None,
        batch_request: Optional[Union[dict, str]] = None,
        **kwargs,
    ):
        self.class_name = class_name
        self.module_name = module_name
        self.batch_request = batch_request
        for k, v in kwargs.items():
            setattr(self, k, v)
            logger.debug(
                'Setting unknown kwarg (%s, %s) provided to constructor as argument in "%s".',
                k,
                v,
                self.__class__.__name__,
            )


class DomainBuilderConfigSchema(NotNullSchema):
    class Meta:
        unknown = INCLUDE

    __config_class__ = DomainBuilderConfig

    class_name = fields.String(
        required=False,
        all_none=True,
    )
    module_name = fields.String(
        required=False,
        all_none=True,
        missing="great_expectations.rule_based_profiler.domain_builder",
    )
    batch_request = fields.Raw(
        required=False,
        allow_none=True,
    )


class ParameterBuilderConfig(DictDot):
    def __init__(
        self,
        name: str,
        class_name: str,
        module_name: Optional[str] = None,
        batch_request: Optional[Union[dict, str]] = None,
        **kwargs,
    ):
        self.name = name
        self.class_name = class_name
        self.module_name = module_name
        self.batch_request = batch_request
        for k, v in kwargs.items():
            setattr(self, k, v)
            logger.debug(
                'Setting unknown kwarg (%s, %s) provided to constructor as argument in "%s".',
                k,
                v,
                self.__class__.__name__,
            )


class ParameterBuilderConfigSchema(NotNullSchema):
    class Meta:
        unknown = INCLUDE

    __config_class__ = ParameterBuilderConfig

    name = fields.String(
        required=True,
        allow_none=False,
    )
    class_name = fields.String(
        required=False,
        all_none=True,
    )
    module_name = fields.String(
        required=False,
        all_none=True,
        missing="great_expectations.rule_based_profiler.parameter_builder",
    )
    batch_request = fields.Raw(
        required=False,
        allow_none=True,
    )


class ExpectationConfigurationBuilderConfig(DictDot):
    def __init__(
        self,
        expectation_type: str,
        class_name: str,
        module_name: Optional[str] = None,
        meta: Optional[dict] = None,
        **kwargs,
    ):
        self.expectation_type = expectation_type
        self.class_name = class_name
        self.module_name = module_name
        self.meta = meta
        for k, v in kwargs.items():
            setattr(self, k, v)
            logger.debug(
                'Setting unknown kwarg (%s, %s) provided to constructor as argument in "%s".',
                k,
                v,
                self.__class__.__name__,
            )


class ExpectationConfigurationBuilderConfigSchema(NotNullSchema):
    class Meta:
        unknown = INCLUDE

    __config_class__ = ExpectationConfigurationBuilderConfig

    class_name = fields.String(
        required=False,
        all_none=True,
    )
    module_name = fields.String(
        required=False,
        all_none=True,
        missing="great_expectations.rule_based_profiler.expectation_configuration_builder",
    )
    expectation_type = fields.Str(
        required=True,
        error_messages={
            "required": "expectation_type missing in expectation configuration builder"
        },
    )
    meta = fields.Dict(
        keys=fields.String(
            required=True,
            allow_none=False,
        ),
        required=False,
        allow_none=True,
    )


class RuleConfig(DictDot):
    def __init__(
        self,
        expectation_configuration_builders: List[
            dict
        ],  # see ExpectationConfigurationBuilderConfig
        domain_builder: Optional[dict] = None,  # see DomainBuilderConfig
        parameter_builders: Optional[List[dict]] = None,  # see ParameterBuilderConfig
    ):
        self.domain_builder = domain_builder
        self.parameter_builders = parameter_builders
        self.expectation_configuration_builders = expectation_configuration_builders


class RuleConfigSchema(NotNullSchema):
    class Meta:
        unknown = INCLUDE

    __config_class__ = RuleConfig

    domain_builder = fields.Nested(
        DomainBuilderConfigSchema,
        required=False,
        allow_none=True,
    )
    parameter_builders = fields.List(
        cls_or_instance=fields.Nested(
            ParameterBuilderConfigSchema,
            required=True,
            allow_none=False,
        ),
        required=False,
        allow_none=True,
    )
    expectation_configuration_builders = fields.List(
        cls_or_instance=fields.Nested(
            ExpectationConfigurationBuilderConfigSchema,
            required=True,
            allow_none=False,
        ),
        required=True,
        allow_none=False,
    )


class RuleBasedProfilerConfig(BaseYamlConfig):
    def __init__(
        self,
        name: str,
        config_version: float,
        rules: Dict[str, dict],  # see RuleConfig
        class_name: Optional[str] = None,
        module_name: Optional[str] = None,
        variables: Optional[Dict[str, Any]] = None,
        commented_map: Optional[CommentedMap] = None,
    ):
        self.name = name
        self.config_version = config_version
        self.rules = rules
        self.class_name = class_name
        self.module_name = module_name
        self.variables = variables

        super().__init__(commented_map=commented_map)

    @classmethod
    def get_config_class(cls) -> Type["RuleBasedProfilerConfig"]:  # noqa: F821
        return cls

    @classmethod
    def get_schema_class(cls) -> Type["RuleBasedProfilerConfigSchema"]:  # noqa: F821
        return RuleBasedProfilerConfigSchema

    # noinspection PyUnusedLocal,PyUnresolvedReferences
    @staticmethod
    def resolve_config_using_acceptable_arguments(
        profiler: "RuleBasedProfiler",  # noqa: F821
        variables: Optional[Dict[str, Any]] = None,
        rules: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> dict:
        runtime_config: dict = profiler.config.to_dict()

        # If applicable, override config attributes with runtime args
        if variables:
            runtime_config["variables"] = variables
        if rules:
            runtime_config["rules"] = rules

        runtime_config["variable_count"] = len(runtime_config["variables"])
        runtime_config["rule_count"] = len(runtime_config["rules"])

        for attr in ("class_name", "module_name", "variables"):
            runtime_config.pop(attr)
            logger.debug("Removed unnecessary attr %s from profiler config", attr)

        return runtime_config


class RuleBasedProfilerConfigSchema(Schema):
    """
    Schema classes for configurations which extend from BaseYamlConfig must extend top-level Marshmallow Schema class.
    Schema classes for their constituent configurations which extend DictDot leve must extend NotNullSchema class.
    """

    class Meta:
        unknown = INCLUDE

    name = fields.String(
        required=True,
        allow_none=False,
    )
    class_name = fields.String(
        required=False,
        all_none=True,
        allow_none=True,
        missing="RuleBasedProfiler",
    )
    module_name = fields.String(
        required=False,
        all_none=True,
        allow_none=True,
        missing="great_expectations.rule_based_profiler",
    )
    config_version = fields.Float(
        required=True,
        allow_none=False,
        validate=lambda x: x == 1.0,
        error_messages={
            "invalid": "config version is not supported; it must be 1.0 per the current version of Great Expectations"
        },
    )
    variables = fields.Dict(
        keys=fields.String(
            required=True,
            allow_none=False,
        ),
        required=False,
        allow_none=True,
    )
    rules = fields.Dict(
        keys=fields.String(
            required=True,
            allow_none=False,
        ),
        values=fields.Nested(
            RuleConfigSchema,
            required=True,
            allow_none=False,
        ),
        required=True,
        allow_none=False,
    )


expectationConfigurationBuilderConfigSchema = (
    ExpectationConfigurationBuilderConfigSchema()
)
parameterBuilderConfigSchema = ParameterBuilderConfigSchema()
domainBuilderConfigSchema = DomainBuilderConfigSchema()
ruleConfigSchema = RuleConfigSchema()
ruleBasedProfilerConfigSchema = RuleBasedProfilerConfigSchema()
