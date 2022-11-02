from __future__ import annotations

from enum import Enum
from inspect import Parameter, Signature, getattr_static, signature
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Type, Union

from makefun import create_function

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import BatchRequestBase
from great_expectations.core.config_peer import ConfigOutputModes, ConfigOutputModeType
from great_expectations.data_context.types.base import BaseYamlConfig
from great_expectations.rule_based_profiler import BaseRuleBasedProfiler
from great_expectations.rule_based_profiler.data_assistant import DataAssistant
from great_expectations.rule_based_profiler.data_assistant_result import (
    DataAssistantResult,
)
from great_expectations.rule_based_profiler.domain_builder import DomainBuilder
from great_expectations.rule_based_profiler.helpers.util import (
    convert_variables_to_dict,
    get_validator_with_expectation_suite,
)
from great_expectations.rule_based_profiler.rule import Rule
from great_expectations.util import deep_filter_properties_iterable
from great_expectations.validator.validator import Validator

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
        self._data_assistant_cls = data_assistant_cls
        self._data_context = data_context

        self._profiler = self.get_profiler()

        setattr(self, "run", self.run_impl())

    def get_profiler(self) -> BaseRuleBasedProfiler:
        """
        This method builds specified "DataAssistant" object and returns its effective "BaseRuleBasedProfiler" object.

        Returns:
            BaseRuleBasedProfiler: The "BaseRuleBasedProfiler" object, corresponding to this instance's "DataAssistant".
        """
        return self._build_data_assistant().profiler

    def get_profiler_config(
        self,
        mode: ConfigOutputModeType = ConfigOutputModes.JSON_DICT,
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
                raise ge_exceptions.DataAssistantExecutionError(
                    message=f"""Utilizing "{data_assistant_name}.run()" requires valid "batch_request" to be specified \
(empty or missing "batch_request" detected)."""
                )

            if estimation is None:
                estimation = NumericRangeEstimatorType.EXACT

            if isinstance(estimation, str):
                estimation = estimation.lower()
                estimation = NumericRangeEstimatorType(estimation)

            data_assistant: DataAssistant = self._build_data_assistant(
                batch_request=batch_request
            )
            directives: dict = deep_filter_properties_iterable(
                properties=kwargs,
            )
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
            data_assistant_result: DataAssistantResult = data_assistant.run(
                variables_directives_list=variables_directives_list,
                domain_type_directives_list=domain_type_directives_list,
            )
            return data_assistant_result

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

        parameters.extend(
            self._get_method_signature_parameters_for_domain_type_directives()
        )
        # Use separate loop for "variables" so as to organize "domain_type_attributes" and "variables" arguments neatly.
        parameters.extend(
            self._get_method_signature_parameters_for_variables_directives()
        )

        func_sig = Signature(
            parameters=parameters, return_annotation=DataAssistantResult
        )
        # override the runner docstring with the docstring defined in the implemented DataAssistant child-class
        run.__doc__ = self._data_assistant_cls.__doc__
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
        parameters: List[Parameter] = []

        rule: Rule
        for rule in self._profiler.rules:
            parameters.append(
                Parameter(
                    name=rule.name,
                    kind=Parameter.POSITIONAL_OR_KEYWORD,
                    default=convert_variables_to_dict(variables=rule.variables),
                    annotation=dict,
                )
            )

        return parameters

    def _get_method_signature_parameters_for_domain_type_directives(
        self,
    ) -> List[Parameter]:
        parameters: List[Parameter] = []

        domain_type_attribute_name_to_parameter_map: Dict[str, Parameter] = {}
        conflicting_domain_type_attribute_names: List[str] = []

        rule: Rule
        domain_builder: DomainBuilder
        domain_builder_attributes: List[str]
        key: str
        accessor_method: Callable
        accessor_method_return_type: Type
        property_value: Any
        parameter: Parameter
        for rule in self._profiler.rules:
            domain_builder = rule.domain_builder
            domain_builder_attributes = self._get_rule_domain_type_attributes(rule=rule)
            for key in domain_builder_attributes:
                accessor_method = getattr_static(domain_builder, key, None).fget
                accessor_method_return_type = signature(
                    obj=accessor_method, follow_wrapped=False
                ).return_annotation
                property_value = getattr(domain_builder, key, None)
                parameter = domain_type_attribute_name_to_parameter_map.get(key)
                if parameter is None:
                    if key not in conflicting_domain_type_attribute_names:
                        parameter = Parameter(
                            name=key,
                            kind=Parameter.POSITIONAL_OR_KEYWORD,
                            default=property_value,
                            annotation=accessor_method_return_type,
                        )
                        domain_type_attribute_name_to_parameter_map[key] = parameter
                elif (
                    parameter.default is None
                    and property_value is not None
                    and key not in conflicting_domain_type_attribute_names
                ):
                    parameter = Parameter(
                        name=key,
                        kind=Parameter.POSITIONAL_OR_KEYWORD,
                        default=property_value,
                        annotation=accessor_method_return_type,
                    )
                    domain_type_attribute_name_to_parameter_map[key] = parameter
                elif parameter.default != property_value and property_value is not None:
                    # For now, prevent customization if default values conflict unless the default DomainBuilder value
                    # is None. In the future, enable at "Rule" level.
                    domain_type_attribute_name_to_parameter_map.pop(key)
                    conflicting_domain_type_attribute_names.append(key)

        parameters.extend(domain_type_attribute_name_to_parameter_map.values())

        return parameters

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
        sig: Signature = signature(obj=klass.__init__)
        parameters: Dict[str, Parameter] = dict(sig.parameters)
        attribute_names: List[str] = list(
            filter(
                lambda element: element not in rule.domain_builder.exclude_field_names,
                list(parameters.keys())[1:],
            )
        )
        return attribute_names
