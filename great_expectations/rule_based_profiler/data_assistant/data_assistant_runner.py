import copy
from functools import wraps
from inspect import Signature, signature
from typing import Any, Callable, Dict, Optional, Type, Union

from great_expectations.core.batch import BatchRequestBase
from great_expectations.rule_based_profiler.data_assistant import DataAssistant
from great_expectations.rule_based_profiler.helpers.runtime_environment import (
    RuntimeEnvironmentColumnDomainTypeDirectivesKeys,
    RuntimeEnvironmentDomainTypeDirectivesKeys,
)
from great_expectations.rule_based_profiler.helpers.util import (
    get_validator_with_expectation_suite,
)
from great_expectations.rule_based_profiler.types.data_assistant_result import (
    DataAssistantResult,
)
from great_expectations.validator.validator import Validator


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

    RUNTIME_ENVIRONMENT_KWARGS: dict = _enum_to_default_kwargs(
        enum_obj=RuntimeEnvironmentColumnDomainTypeDirectivesKeys
    )

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
        data_assistant_name: str = self._data_assistant_cls.data_assistant_type
        validator: Validator = get_validator_with_expectation_suite(
            batch_request=batch_request,
            data_context=self._data_context,
            expectation_suite=None,
            expectation_suite_name=None,
            component_name=data_assistant_name,
        )
        data_assistant: DataAssistant = self._data_assistant_cls(
            name=data_assistant_name,
            validator=validator,
        )
        data_assistant_result: DataAssistantResult = data_assistant.run(
            variables=variables,
            rules=rules,
            **kwargs,
        )
        return data_assistant_result
