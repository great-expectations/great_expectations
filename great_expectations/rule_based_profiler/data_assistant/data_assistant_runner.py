import copy
from functools import wraps
from inspect import Signature, signature
from typing import Any, Callable, Dict, List, Optional, Type, Union

from great_expectations.core.batch import BatchRequestBase
from great_expectations.core.config_peer import ConfigOutputModes, ConfigOutputModeType
from great_expectations.data_context.types.base import BaseYamlConfig
from great_expectations.rule_based_profiler.data_assistant import DataAssistant
from great_expectations.rule_based_profiler.helpers.util import (
    get_validator_with_expectation_suite,
)
from great_expectations.rule_based_profiler.types.data_assistant_result import (
    DataAssistantResult,
)
from great_expectations.util import deep_filter_properties_iterable
from great_expectations.validator.validator import Validator

from great_expectations.rule_based_profiler.helpers.runtime_environment import (  # isort:skip
    RuntimeEnvironmentVariablesDirectives,
    RuntimeEnvironmentTableDomainTypeDirectivesKeys,
    RuntimeEnvironmentColumnDomainTypeDirectivesKeys,
    RuntimeEnvironmentColumnPairDomainTypeDirectivesKeys,
    RuntimeEnvironmentMulticolumnDomainTypeDirectivesKeys,
    RuntimeEnvironmentDomainTypeDirectivesKeys,
    RuntimeEnvironmentDomainTypeDirectives,
    build_domain_type_directives,
    build_variables_directives,
)


def augment_arguments(**extra_kwargs: dict) -> Callable:
    """
    Decorator factory that defines additional arguments that are passed to every function invocation.
    """

    def signature_modification_decorator(func: Callable) -> Callable:
        """
        Specific decorator definition for including additional arguments that are passed to every function invocation.
        """

        @wraps(func)
        def modify_signature(*args, **kwargs) -> Any:
            """
            Actual implementation logic for including additional arguments that are passed to every function invocation.
            """
            kwargs = kwargs or {}
            augmented_kwargs: dict = copy.deepcopy(extra_kwargs)
            augmented_kwargs.update(kwargs)
            return func(*args, **augmented_kwargs)

        # Override signature.
        sig: Signature = signature(func)
        sig = sig.replace(parameters=tuple(sig.parameters.values())[1:])
        modify_signature.__signature__ = sig

        return modify_signature

    return signature_modification_decorator


def _build_enum_to_default_kwargs_map() -> Dict[str, Any]:
    enum_to_default_kwargs_map: dict = {}

    directives_keys_type: RuntimeEnvironmentDomainTypeDirectivesKeys
    for directive_keys_type in [
        RuntimeEnvironmentTableDomainTypeDirectivesKeys,
        RuntimeEnvironmentColumnDomainTypeDirectivesKeys,
        RuntimeEnvironmentColumnPairDomainTypeDirectivesKeys,
        RuntimeEnvironmentMulticolumnDomainTypeDirectivesKeys,
    ]:
        enum_to_default_kwargs_map.update(
            _enum_to_default_kwargs(enum_obj=directive_keys_type)
        )

    return enum_to_default_kwargs_map


def _enum_to_default_kwargs(
    enum_obj: Type[RuntimeEnvironmentDomainTypeDirectivesKeys],
) -> dict:
    key: type(enum_obj)
    return {key.value: None for key in enum_obj}


class DataAssistantRunner:
    """
    DataAssistantRunner processes invocations of calls to "run()" methods of registered "DataAssistant" classes.

    The approach is to instantiate "DataAssistant" class of specified type with "Validator", containing "Batch" objects,
    specified by "batch_request", loaded into memory.  Then, "DataAssistant.run()" is issued with given directives.
    """

    def __init__(
        self,
        data_assistant_cls: Type["DataAssistant"],  # noqa: F821
        data_context: "BaseDataContext",  # noqa: F821
    ) -> None:
        """
        Args:
            data_assistant_cls: DataAssistant class associated with this DataAssistantRunner
            data_context: BaseDataContext associated with this DataAssistantRunner
        """
        self._data_assistant_cls = data_assistant_cls
        self._data_context = data_context

        self._data_assistant = None

    def _build_data_assistant(
        self,
        batch_request: Optional[Union[BatchRequestBase, dict]] = None,
    ) -> DataAssistant:
        """
        batch_request: Explicit batch_request used to supply data at runtime
        """
        if self._data_assistant:
            return self._data_assistant

        data_assistant_name: str = self._data_assistant_cls.data_assistant_type
        validator: Validator = get_validator_with_expectation_suite(
            data_context=self._data_context,
            batch_list=None,
            batch_request=batch_request,
            expectation_suite=None,
            expectation_suite_name=None,
            component_name=data_assistant_name,
            persist=False,
        )
        data_assistant: DataAssistant = self._data_assistant_cls(
            name=data_assistant_name,
            validator=validator,
        )

        self._data_assistant = data_assistant

        return self._data_assistant

    def get_profiler_config(
        self,
        batch_request: Optional[Union[BatchRequestBase, dict]] = None,
        mode: ConfigOutputModeType = ConfigOutputModes.JSON_DICT,
    ) -> Union[BaseYamlConfig, dict, str]:
        """
        batch_request: Explicit batch_request used to supply data at runtime
        mode: One of "ConfigOutputModes" Enum typed values (corresponding string typed values are also supported)
        """
        return self._build_data_assistant(
            batch_request=batch_request
        ).profiler.get_config(mode=mode)

    @augment_arguments(**_build_enum_to_default_kwargs_map())
    def run(
        self,
        batch_request: Optional[Union[BatchRequestBase, dict]] = None,
        **kwargs: dict,
    ) -> DataAssistantResult:
        """
        batch_request: Explicit batch_request used to supply data at runtime
        kwargs: additional/override directives supplied at runtime (using "runtime_environment")
            "kwargs" directives structure:
            {
                "DomainBuilder" parameters:
                include_column_names=["column_a", "column_b", "column_c", ...],
                exclude_column_names=["column_d", "column_e", "column_f", "column_g", ...],
                max_unexpected_values=0,
                max_unexpected_ratio=None,
                min_max_unexpected_values_proportion=9.75e-1,
                ... Other "DomainBuilder" parameters ...
                "variables" settings for "Rule" configurations:
                numeric_columns_rule={
                    "round_decimals": 12,
                    "false_positive_rate": 0.1,
                    "random_seed": 43792,
                },
                datetime_columns_rule={
                    "truncate_values": {
                        "lower_bound": 0,
                        "upper_bound": 4481049600,  # Friday, January 1, 2112 0:00:00
                    },
                    "round_decimals": 0,
                },
                categorical_columns_rule={
                    "false_positive_rate": 0.1,
                    "round_decimals": 4,
                },
                ... "variables" settings for other "Rule" configurations ...
            }
        Implementation makes best effort at assigning directives to appropriate "MetricDomainTypes" member.

        Returns:
            DataAssistantResult: The result object for the DataAssistant
        """
        data_assistant: DataAssistant = self._build_data_assistant(
            batch_request=batch_request
        )
        directives: dict = deep_filter_properties_iterable(
            properties=kwargs,
        )

        variables_directives_list: List[
            RuntimeEnvironmentVariablesDirectives
        ] = build_variables_directives(**directives)
        domain_type_directives_list: List[
            RuntimeEnvironmentDomainTypeDirectives
        ] = build_domain_type_directives(**directives)
        data_assistant_result: DataAssistantResult = data_assistant.run(
            variables_directives_list=variables_directives_list,
            domain_type_directives_list=domain_type_directives_list,
        )
        return data_assistant_result
