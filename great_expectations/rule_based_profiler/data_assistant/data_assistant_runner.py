from __future__ import annotations

from enum import Enum
from inspect import Parameter, Signature, getattr_static, signature
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Type, Union

from makefun import create_function

import great_expectations.exceptions as gx_exceptions
from great_expectations.core.batch import BatchRequestBase
from great_expectations.core.config_peer import ConfigOutputModes
from great_expectations.data_context.types.base import BaseYamlConfig  # noqa: TCH001
from great_expectations.rule_based_profiler import BaseRuleBasedProfiler  # noqa: TCH001
from great_expectations.rule_based_profiler.data_assistant import (
    DataAssistant,  # noqa: TCH001
)
from great_expectations.rule_based_profiler.data_assistant_result import (
    DataAssistantResult,
)
from great_expectations.rule_based_profiler.domain_builder import (
    DomainBuilder,  # noqa: TCH001
)
from great_expectations.rule_based_profiler.helpers.util import (
    convert_variables_to_dict,
    get_validator_with_expectation_suite,
)
from great_expectations.rule_based_profiler.rule import Rule  # noqa: TCH001
from great_expectations.util import deep_filter_properties_iterable
from great_expectations.validator.validator import Validator  # noqa: TCH001

from great_expectations.rule_based_profiler.helpers.runtime_environment import (  # isort:skip
    RuntimeEnvironmentVariablesDirectives,
    RuntimeEnvironmentDomainTypeDirectives,
    build_domain_type_directives,
    build_variables_directives,
)

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )


class NumericRangeEstimatorType(Enum):
    EXACT = "exact"
    FLAG_OUTLIERS = "flag_outliers"


class DataAssistantRunner:
    """
    DataAssistantRunner processes invocations of calls to "run()" methods of registered "DataAssistant" classes.

    The approach is to instantiate "DataAssistant" class of specified type with "Validator", containing "Batch" objects,
    specified by "batch_request", loaded into memory.  Then, "DataAssistant.run()" is issued with given directives.
    """

    def __init__(
        self,
        data_assistant_cls: Type[DataAssistant],
        data_context: AbstractDataContext,
    ) -> None:
        """
        Args:
            data_assistant_cls: DataAssistant class associated with this DataAssistantRunner
            data_context: AbstractDataContext associated with this DataAssistantRunner
        """
        self._data_assistant_cls: Type[DataAssistant] = data_assistant_cls
        self._data_context: AbstractDataContext = data_context

        self._profiler: BaseRuleBasedProfiler = self.get_profiler()

        self.run: Callable = self.run_impl()

    def get_profiler(self) -> BaseRuleBasedProfiler:
        """
        This method builds specified "DataAssistant" object and returns its effective "BaseRuleBasedProfiler" object.

        Returns:
            BaseRuleBasedProfiler: The "BaseRuleBasedProfiler" object, corresponding to this instance's "DataAssistant".
        """
        return self._build_data_assistant().profiler

    def get_profiler_config(
        self,
        mode: ConfigOutputModes = ConfigOutputModes.JSON_DICT,
    ) -> Union[BaseYamlConfig, dict, str]:
        """
        This method returns configuration of effective "BaseRuleBasedProfiler", corresponding to this instance's
        "DataAssistant", according to specified "mode" (formatting) directive.

        Args:
            mode: One of "ConfigOutputModes" Enum typed values (corresponding string typed values are also supported)

        Returns:
            Union[BaseYamlConfig, dict, str]: Configuration of effective "BaseRuleBasedProfiler" object in given format.
        """
        return self._profiler.get_config(mode=mode)

    def run_impl(self) -> Callable:
        """
        Dynamically constructs method signature and implementation of "DataAssistant.run()" method for this instance's
        "DataAssistant" object (which corresponds to this instance's "DataAssistant" type, specified in constructor).

        Returns:
            Callable: Template "DataAssistant.run()" method implementation, customized with signature appropriate for
            "DataAssistant.run()" method of "DataAssistant" class (corresponding to this object's "DataAssistant" type).
        """

        def run(
            batch_request: Optional[Union[BatchRequestBase, dict]] = None,
            estimation: Optional[Union[str, NumericRangeEstimatorType]] = None,
            **kwargs,
        ) -> DataAssistantResult:
            """
            Generic "DataAssistant.run()" template method, its signature built dynamically by introspecting effective
            "BaseRuleBasedProfiler", corresponding to this instance's "DataAssistant" class, and returned to dispatcher.

            Args:
                batch_request: Explicit batch_request used to supply data at runtime
                estimation: Global type directive for applicable "Rule" objects that utilize numeric range estimation.
                    If set to "exact" (default), all "Rule" objects using "NumericMetricRangeMultiBatchParameterBuilder"
                    will have the value of "estimator" property (referred to by "$variables.estimator") equal "exact".
                    If set to "flag_outliers", then "bootstrap" estimator (default in "Rule" variables) takes effect.
                kwargs: placeholder for "makefun.create_function()" to propagate dynamically generated signature

            Returns:
                DataAssistantResult: The result object for the DataAssistant
            """
            if batch_request is None:
                data_assistant_name: str = self._data_assistant_cls.data_assistant_type
                raise gx_exceptions.DataAssistantExecutionError(
                    message=f"""Utilizing "{data_assistant_name}.run()" requires valid "batch_request" to be specified \
(empty or missing "batch_request" detected)."""
                )

            if estimation is None:
                estimation = NumericRangeEstimatorType.EXACT

            # The "estimation" directive needs to be a recognized member of "NumericRangeEstimatorType" "Enum" type.
            if isinstance(estimation, str):
                estimation = estimation.lower()
                estimation = NumericRangeEstimatorType(estimation)

            data_assistant: DataAssistant = self._build_data_assistant(
                batch_request=batch_request
            )
            directives: dict = deep_filter_properties_iterable(
                properties=kwargs,
            )
            """
            To supply user-configurable override arguments/directives:
            1) obtain "Domain"-level attributes;
            2) split passed-in arguments/directives into "Domain"-level and "variables"-level.
            """
            rule_based_profiler_domain_type_attributes: List[
                str
            ] = self._get_rule_based_profiler_domain_type_attributes()
            variables_directives_kwargs: dict = dict(
                filter(
                    lambda element: element[0]
                    not in rule_based_profiler_domain_type_attributes,
                    directives.items(),
                )
            )
            domain_type_directives_kwargs: dict = dict(
                filter(
                    lambda element: element[0]
                    in rule_based_profiler_domain_type_attributes,
                    directives.items(),
                )
            )
            """
            Convert "dict"-typed "variables"-level and "Domain"-level arguments/directives into lists of corresponding
            "Enum" typed objects ("RuntimeEnvironmentVariablesDirectives" and "RuntimeEnvironmentDomainTypeDirectives").
            """
            variables_directives_list: List[
                RuntimeEnvironmentVariablesDirectives
            ] = build_variables_directives(
                exact_estimation=(estimation == NumericRangeEstimatorType.EXACT),
                rules=self._profiler.rules,
                **variables_directives_kwargs,
            )
            domain_type_directives_list: List[
                RuntimeEnvironmentDomainTypeDirectives
            ] = build_domain_type_directives(**domain_type_directives_kwargs)
            """
            Run "data_assistant" with thus constructed "variables"-level and "Domain"-level custom user-specified
            overwrite arguments/directives and return comput3ed "data_assistant_result" to caller.
            """
            data_assistant_result: DataAssistantResult = data_assistant.run(
                variables_directives_list=variables_directives_list,
                domain_type_directives_list=domain_type_directives_list,
            )
            return data_assistant_result

        # Construct arguments to "DataAssistantRunner.run()" method, implemented using "DataAssistantRunner.run_impl()".

        # 1. The signature includes "batch_request" and "estimation" arguments for all "DataAssistant" implementations.
        parameters: List[Parameter] = [
            Parameter(
                name="batch_request",
                kind=Parameter.POSITIONAL_OR_KEYWORD,
                annotation=Union[BatchRequestBase, dict],
            ),
            Parameter(
                name="estimation",
                kind=Parameter.POSITIONAL_OR_KEYWORD,
                default="exact",
                annotation=Optional[Union[str, NumericRangeEstimatorType]],
            ),
        ]

        # 2. Extend the signature to include "DataAssistant"-specific "Domain"-level arguments/directives.
        parameters.extend(
            self._get_method_signature_parameters_for_domain_type_directives()
        )
        # 3. Extend the signature to include "DataAssistant"-specific "variables"-level arguments/directives.
        # Use separate call for "variables" so as to organize "domain_type_attributes" and "variables" arguments neatly.
        parameters.extend(
            self._get_method_signature_parameters_for_variables_directives()
        )

        # 4. Encapsulate all arguments/directives into "DataAssistant"-specific "DataAssistantRunner.run()" signature.
        func_sig = Signature(
            parameters=parameters, return_annotation=DataAssistantResult
        )
        # 5. Override the runner docstring with the docstring defined in the implemented DataAssistant child-class.
        run.__doc__ = self._data_assistant_cls.__doc__
        # 6. Create "DataAssistant"-specific "DataAssistantRunner.run()" method and parametrized implementation closure.
        gen_func: Callable = create_function(func_signature=func_sig, func_impl=run)

        return gen_func

    def _build_data_assistant(
        self,
        batch_request: Optional[Union[BatchRequestBase, dict]] = None,
    ) -> DataAssistant:
        """
        This method builds specified "DataAssistant" object and returns its effective "BaseRuleBasedProfiler" object.

        Args:
            batch_request: Explicit batch_request used to supply data at runtime

        Returns:
            DataAssistant: The "DataAssistant" object, corresponding to this instance's specified "DataAssistant" type.
        """
        data_assistant_name: str = self._data_assistant_cls.data_assistant_type

        data_assistant: DataAssistant

        if batch_request is None:
            data_assistant = self._data_assistant_cls(
                name=data_assistant_name,
                validator=None,
            )
        else:
            validator: Validator = get_validator_with_expectation_suite(
                data_context=self._data_context,
                batch_list=None,
                batch_request=batch_request,
                expectation_suite=None,
                expectation_suite_name=None,
                component_name=data_assistant_name,
                persist=False,
            )
            data_assistant = self._data_assistant_cls(
                name=data_assistant_name,
                validator=validator,
            )

        return data_assistant

    def _get_method_signature_parameters_for_variables_directives(
        self,
    ) -> List[Parameter]:
        """
        Using "DataAssistant"-specific configured "Rule" objects of underlying "RuleBasedProfiler", return "Parameter"
        signature components containing "Rule" "variables" (converted to "dictionary" representation) as default values.
        """
        rule: Rule
        parameters: List[Parameter] = [
            Parameter(
                name=rule.name,
                kind=Parameter.POSITIONAL_OR_KEYWORD,
                default=convert_variables_to_dict(variables=rule.variables),
                annotation=dict,
            )
            for rule in self._profiler.rules
        ]
        return parameters

    def _get_method_signature_parameters_for_domain_type_directives(
        self,
    ) -> List[Parameter]:
        """
        Using "DataAssistant"-specific configured "Rule" objects of underlying "RuleBasedProfiler", return "Parameter"
        signature components containing pre-configured "Rule" "DomainBuilder" arguments/directives as default values.

        Each "Rule" in actual "RuleBasedProfiler" architecture includes its own "DomainBuilder", meaning that "Domain"
        type arguments/directives can differ among "Rule" objects within "RuleBasedProfiler" configuration.  However,
        this capability is not currently utilized in "DataAssistantRunner.run()" signature -- only common "Domain"
        type arguments/directives are specified (i.e., not for individual "Rule" objects).  Hence, reconciliation among
        common "Domain" type arguments/directives with those of individual "Rule" objects must be provided.  The logic
        maintains list of arguments/directives that are conflicting among "Rule" configurations and (for now) prevents
        customization if default values conflict, unless default "DomainBuilder" argument/directive value is None.
        Enabling per-"Rule" customization can be added as well (in addition to having this capability at common level).
        """
        domain_type_attribute_name_to_parameter_map: Dict[str, Parameter] = {}
        conflicting_domain_type_attribute_names: List[str] = []

        rule: Rule  # one "Rule" object of underlying "RuleBasedProfiler"
        domain_builder: DomainBuilder | None  # "DomainBuilder" of "Rule"
        domain_builder_attributes: List[
            str
        ]  # list of specific "DomainBuilder" arguments/directives of "Rule"
        key: str  # one argument/directive of "DomainBuilder" of "Rule"
        property_accessor_method: Callable  # property accessor method for one argument/directive of "DomainBuilder" of "Rule"
        property_accessor_method_return_type: Type  # return type of property accessor method for one argument/directive of "DomainBuilder" of "Rule"
        property_value: Any  # default return value of property accessor method for one argument/directive of "DomainBuilder" of "Rule"
        parameter: Parameter | None  #  "Parameter" signature component containing one argument/directive of "DomainBuilder" of "Rule"
        for rule in self._profiler.rules:
            domain_builder = rule.domain_builder
            assert (
                domain_builder
            ), "Must have a non-null domain_builder attr on the underlying RuleBasedProfiler"
            domain_builder_attributes = self._get_rule_domain_type_attributes(rule=rule)
            for key in domain_builder_attributes:
                """
                "getattr_static()" returns "getter" "property" definition object, and "fget" on it gives its "Callable"
                """
                property_accessor_method = getattr_static(
                    domain_builder, key, None
                ).fget
                property_accessor_method_return_type = signature(
                    obj=property_accessor_method, follow_wrapped=False
                ).return_annotation
                property_value = getattr(domain_builder, key, None)
                parameter = domain_type_attribute_name_to_parameter_map.get(key)
                if (key not in conflicting_domain_type_attribute_names) and (
                    (parameter is None)
                    or ((parameter.default is None) and (property_value is not None))
                ):
                    """
                    Condition for incorporating new default value of given "Domain" type argument/directive of
                    "DomainBuilder" of "Rule" consists of two checks:
                    1. Given "Domain" type argument/directive name of "DomainBuilder" of "Rule" is not in conflict with
                    same "Domain" type argument/directive name of "DomainBuilder" of another (already processed) "Rule".
                    2. Either default value of given "Domain" type argument/directive of "DomainBuilder" of "Rule" has
                    not yet been incorporated, or it has already been incorporated, but its previous (processed) default
                    value is None, while configured default value in "DomainBuilder" of current "Rule" is not None (in
                    other words, if not None is encountered during "Rule" processing, not None value overrites None).
                    """
                    if key not in conflicting_domain_type_attribute_names:
                        parameter = Parameter(
                            name=key,
                            kind=Parameter.POSITIONAL_OR_KEYWORD,
                            default=property_value,
                            annotation=property_accessor_method_return_type,
                        )
                        domain_type_attribute_name_to_parameter_map[key] = parameter
                elif (
                    parameter is not None
                    and property_value is not None
                    and parameter.default != property_value
                ):
                    """
                    Handling conflict involves detection of already successful incorporation of "Parameter" for
                    argument/directive name of "DomainBuilder" of "Rule", but with previous (processed) default value
                    being different from configured default value of same-named argument/directive in "DomainBuilder" of
                    current "Rule" being processed.  For now, customization is prevented/disabled if default values
                    conflict, unless configured default value of argument/directive in "DomainBuilder" of current "Rule"
                    being processed is None.  In the future, enable argument/directive customization at "Rule" level.
                    """
                    domain_type_attribute_name_to_parameter_map.pop(key)
                    conflicting_domain_type_attribute_names.append(key)

        return list(domain_type_attribute_name_to_parameter_map.values())

    def _get_rule_based_profiler_domain_type_attributes(
        self, rule: Optional[Rule] = None
    ) -> List[str]:
        if rule is None:
            domain_type_attributes: List[str] = []
            for rule in self._profiler.rules:
                domain_type_attributes.extend(
                    self._get_rule_domain_type_attributes(rule=rule)
                )

            return list(set(domain_type_attributes))

        return self._get_rule_domain_type_attributes(rule=rule)

    @staticmethod
    def _get_rule_domain_type_attributes(rule: Rule) -> List[str]:
        klass: type = rule.domain_builder.__class__
        sig: Signature = signature(obj=klass.__init__)  # type: ignore[misc] # mypy does not like direct __init__ access
        parameters: Dict[str, Parameter] = dict(sig.parameters)

        domain_builder = rule.domain_builder
        assert (
            domain_builder
        ), f"The underlying domain_builder on rule {rule.name} must be non-null"
        exclude_field_names = domain_builder.exclude_field_names

        attribute_names: List[str] = list(
            filter(
                lambda element: element not in exclude_field_names,
                list(parameters.keys())[1:],
            )
        )
        return attribute_names
