import copy
from functools import wraps
from inspect import Signature, signature
from typing import Any, Callable, Dict, List, Optional, Type, Union

from great_expectations.core.batch import BatchRequestBase
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

    RUNTIME_ENVIRONMENT_KWARGS: dict = _build_enum_to_default_kwargs_map()

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

    @augment_arguments(**RUNTIME_ENVIRONMENT_KWARGS)
    def run(
        self,
        variables: Optional[Dict[str, Any]] = None,
        rules: Optional[Dict[str, Dict[str, Any]]] = None,
        batch_request: Optional[Union[BatchRequestBase, dict]] = None,
        **kwargs: dict,
    ) -> DataAssistantResult:
        """
        variables: attribute name/value pairs, commonly-used in Builder objects, to modify using "runtime_environment"
        rules: name/(configuration-dictionary) to modify using "runtime_environment"
        kwargs: additional/override directives supplied at runtime
            "kwargs" directives structure:
            {
                "include_column_names": ["column_a", "column_b", "column_c", ...],
                "exclude_column_names": ["column_d", "column_e", "column_f", "column_g", ...],
                ...
            }
        Implementation makes best effort at assigning directives to appropriate "MetricDomainTypes" member.

        Returns:
            DataAssistantResult: The result object for the DataAssistant
        """
        data_assistant_name: str = self._data_assistant_cls.data_assistant_type
        validator: Validator = get_validator_with_expectation_suite(
            batch_request=batch_request,
            data_context=self._data_context,
            expectation_suite=None,
            expectation_suite_name=None,
            component_name=data_assistant_name,
            persist=False,
        )
        data_assistant: DataAssistant = self._data_assistant_cls(
            name=data_assistant_name,
            validator=validator,
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
            variables=variables,
            rules=rules,
            variables_directives_list=variables_directives_list,
            domain_type_directives_list=domain_type_directives_list,
        )
        return data_assistant_result
