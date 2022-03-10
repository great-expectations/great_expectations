import json
import logging
from typing import Any, Dict, List, Optional, Type, Union

from ruamel.yaml.comments import CommentedMap

from great_expectations.core.util import convert_to_json_serializable
from great_expectations.data_context.types.base import BaseYamlConfig
from great_expectations.marshmallow__shade import (
    INCLUDE,
    Schema,
    fields,
    post_dump,
    post_load,
)
from great_expectations.rule_based_profiler.helpers.util import (
    convert_variables_to_dict,
    get_parameter_value_and_validate_return_type,
)
from great_expectations.rule_based_profiler.types.parameter_container import (
    VARIABLES_KEY,
    ParameterContainer,
)
from great_expectations.types import DictDot, SerializableDictDot
from great_expectations.util import (
    deep_filter_properties_iterable,
    filter_properties_dict,
)

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
        # noinspection PyArgumentList
        for key in original.keys():
            if key not in output and not key.startswith("_"):
                # noinspection PyUnresolvedReferences
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
        required=True,
        allow_none=False,
    )
    module_name = fields.String(
        required=False,
        allow_none=True,
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
        required=True,
        allow_none=False,
    )
    module_name = fields.String(
        required=False,
        allow_none=True,
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
        required=True,
        allow_none=False,
    )
    module_name = fields.String(
        required=False,
        allow_none=True,
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


class RuleConfig(SerializableDictDot):
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

    def to_json_dict(self) -> dict:
        """
        # TODO: <Alex>2/4/2022</Alex>
        This implementation of "SerializableDictDot.to_json_dict() occurs frequently and should ideally serve as the
        reference implementation in the "SerializableDictDot" class itself.  However, the circular import dependencies,
        due to the location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules
        make this refactoring infeasible at the present time.
        """
        dict_obj: dict = self.to_dict()
        serializeable_dict: dict = convert_to_json_serializable(data=dict_obj)
        return serializeable_dict

    def __repr__(self) -> str:
        """
        # TODO: <Alex>2/4/2022</Alex>
        This implementation of a custom "__repr__()" occurs frequently and should ideally serve as the reference
        implementation in the "SerializableDictDot" class.  However, the circular import dependencies, due to the
        location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules make this
        refactoring infeasible at the present time.
        """
        json_dict: dict = self.to_json_dict()
        deep_filter_properties_iterable(
            properties=json_dict,
            inplace=True,
        )

        keys: List[str] = sorted(list(json_dict.keys()))

        key: str
        sorted_json_dict: dict = {key: json_dict[key] for key in keys}

        return json.dumps(sorted_json_dict, indent=2)

    def __str__(self) -> str:
        """
        # TODO: <Alex>2/4/2022</Alex>
        This implementation of a custom "__str__()" occurs frequently and should ideally serve as the reference
        implementation in the "SerializableDictDot" class.  However, the circular import dependencies, due to the
        location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules make this
        refactoring infeasible at the present time.
        """
        return self.__repr__()


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
        if class_name is not None:
            self.class_name = class_name
        if module_name is not None:
            self.module_name = module_name
        self.variables = variables

        super().__init__(commented_map=commented_map)

    @classmethod
    def get_config_class(cls) -> Type["RuleBasedProfilerConfig"]:  # noqa: F821
        return cls

    @classmethod
    def get_schema_class(cls) -> Type["RuleBasedProfilerConfigSchema"]:  # noqa: F821
        return RuleBasedProfilerConfigSchema

    def to_json_dict(self) -> dict:
        """
        # TODO: <Alex>2/4/2022</Alex>
        This implementation of "SerializableDictDot.to_json_dict() occurs frequently and should ideally serve as the
        reference implementation in the "SerializableDictDot" class itself.  However, the circular import dependencies,
        due to the location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules
        make this refactoring infeasible at the present time.
        """
        dict_obj: dict = self.to_dict()
        serializeable_dict: dict = convert_to_json_serializable(data=dict_obj)
        return serializeable_dict

    def __repr__(self) -> str:
        """
        # TODO: <Alex>2/4/2022</Alex>
        This implementation of a custom "__repr__()" occurs frequently and should ideally serve as the reference
        implementation in the "SerializableDictDot" class.  However, the circular import dependencies, due to the
        location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules make this
        refactoring infeasible at the present time.
        """
        json_dict: dict = self.to_json_dict()
        deep_filter_properties_iterable(
            properties=json_dict,
            inplace=True,
        )

        keys: List[str] = sorted(list(json_dict.keys()))

        key: str
        sorted_json_dict: dict = {key: json_dict[key] for key in keys}

        return json.dumps(sorted_json_dict, indent=2)

    def __str__(self) -> str:
        """
        # TODO: <Alex>2/4/2022</Alex>
        This implementation of a custom "__str__()" occurs frequently and should ideally serve as the reference
        implementation in the "SerializableDictDot" class.  However, the circular import dependencies, due to the
        location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules make this
        refactoring infeasible at the present time.
        """
        return self.__repr__()

    @classmethod
    def resolve_config_using_acceptable_arguments(
        cls,
        profiler: "RuleBasedProfiler",  # noqa: F821
        variables: Optional[Dict[str, Any]] = None,
        rules: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> "RuleBasedProfilerConfig":  # noqa: F821
        """Reconciles variables/rules by taking into account runtime overrides and variable substitution.

        Utilized in usage statistics to interact with the args provided in `RuleBasedProfiler.run()`.
        NOTE: This is a lightweight version of the RBP's true reconiliation logic - see
        `reconcile_profiler_variables` and `reconcile_profiler_rules`.

        Args:
            profiler: The profiler used to invoke `run()`.
            variables: Any runtime override variables.
            rules: Any runtime override rules.

        Returns:
            An instance of RuleBasedProfilerConfig that represents the reconciled profiler.
        """
        effective_variables: Optional[
            ParameterContainer
        ] = profiler.reconcile_profiler_variables(
            variables=variables,
        )
        runtime_variables: Optional[Dict[str, Any]] = convert_variables_to_dict(
            variables=effective_variables
        )

        effective_rules: List["Rule"] = profiler.reconcile_profiler_rules(  # noqa: F821
            rules=rules,
        )

        rule: "Rule"  # noqa: F821
        effective_rules_dict: Dict[str, "Rule"] = {  # noqa: F821
            rule.name: rule for rule in effective_rules
        }
        runtime_rules: Dict[str, dict] = {
            name: RuleBasedProfilerConfig._substitute_variables_in_config(
                rule=rule,
                variables_container=effective_variables,
            )
            for name, rule in effective_rules_dict.items()
        }

        return cls(
            class_name=profiler.__class__.__name__,
            module_name=profiler.__class__.__module__,
            name=profiler.config.name,
            config_version=profiler.config.config_version,
            variables=runtime_variables,
            rules=runtime_rules,
        )

    @staticmethod
    def _substitute_variables_in_config(
        rule: "Rule",  # noqa: F821
        variables_container: ParameterContainer,
    ) -> dict:
        """Recursively updates a given rule to substitute $variable references.

        Args:
            rule: The Rule object to update.
            variables_container: Keeps track of $variable values to be substituted.

        Returns:
            The dictionary representation of the rule with all $variable references substituted.
        """

        def _traverse_and_substitute(node: Any) -> None:
            if isinstance(node, dict):
                for key, val in node.copy().items():
                    if isinstance(val, str) and val.startswith(VARIABLES_KEY):
                        node[key] = get_parameter_value_and_validate_return_type(
                            domain=None,
                            parameter_reference=val,
                            variables=variables_container,
                            parameters=None,
                        )
                    _traverse_and_substitute(node=val)
            elif isinstance(node, list):
                for val in node:
                    _traverse_and_substitute(node=val)

        rule_dict: dict = rule.to_json_dict()
        _traverse_and_substitute(node=rule_dict)

        return rule_dict


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
        allow_none=True,
        missing="RuleBasedProfiler",
    )
    module_name = fields.String(
        required=False,
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
