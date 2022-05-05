from typing import Optional, Type, Union

from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import BatchRequestBase
from great_expectations.rule_based_profiler.data_assistant import DataAssistant
from great_expectations.rule_based_profiler.helpers.util import (
    get_validator_with_expectation_suite,
)
from great_expectations.rule_based_profiler.types.data_assistant_result import (
    DataAssistantResult,
)
from great_expectations.validator.validator import Validator


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

    def run(
        self,
        batch_request: Union[BatchRequestBase, dict],
        expectation_suite: Optional[ExpectationSuite] = None,
        expectation_suite_name: Optional[str] = None,
        include_citation: bool = True,
        save_updated_expectation_suite: bool = False,
    ) -> DataAssistantResult:
        data_assistant_name: str = self._data_assistant_cls.data_assistant_type
        validator: Validator = get_validator_with_expectation_suite(
            batch_request=batch_request,
            data_context=self._data_context,
            expectation_suite=expectation_suite,
            expectation_suite_name=expectation_suite_name,
            component_name=data_assistant_name,
        )
        data_assistant: DataAssistant = self._data_assistant_cls(
            name=data_assistant_name,
            validator=validator,
        )
        data_assistant_result: DataAssistantResult = data_assistant.run(
            expectation_suite=expectation_suite,
            expectation_suite_name=expectation_suite_name,
            include_citation=include_citation,
            save_updated_expectation_suite=save_updated_expectation_suite,
        )
        return data_assistant_result
