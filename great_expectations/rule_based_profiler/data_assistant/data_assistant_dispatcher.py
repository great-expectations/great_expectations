from typing import Dict, Optional, Type

from great_expectations.expectations.registry import get_data_assistant_impl
from great_expectations.rule_based_profiler.data_assistant import DataAssistant
from great_expectations.rule_based_profiler.data_assistant.data_assistant_runner import (
    DataAssistantRunner,
)


class DataAssistantDispatcher:
    """
    DataAssistantDispatcher intercepts requests for "DataAssistant" classes by their registered names and manages their
    associated "DataAssistantRunner" objects, which process invocations of calls to "DataAssistant" "run()" methods.
    """

    DATA_ASSISTANT_TYPE_ALIASES: Dict[str, str] = {
        "volume": "volume_data_assistant",
    }

    def __init__(self, data_context: "BaseDataContext"):  # noqa: F821
        """
        Args:
            data_context: BaseDataContext associated with DataAssistantDispatcher
        """
        self._data_context = data_context

        self._data_assistant_runner_cache = {}

    def __getattr__(self, name):
        data_assistant_cls: Type["DataAssistant"] = get_data_assistant_impl(
            data_assistant_type=name.lower()
        ) or get_data_assistant_impl(
            data_assistant_type=DataAssistantDispatcher.DATA_ASSISTANT_TYPE_ALIASES.get(
                name.lower(), ""
            )
        )  # noqa: F821
        if data_assistant_cls is None:
            raise AttributeError(
                f'"{type(self).__name__}" object has no attribute "{name}".'
            )

        data_assistant_name: str = data_assistant_cls.data_assistant_type
        data_assistant_runner: Optional[
            DataAssistantRunner
        ] = self._data_assistant_runner_cache.get(data_assistant_name)
        if data_assistant_runner is None:
            data_assistant_runner = DataAssistantRunner(
                data_assistant_cls=data_assistant_cls,
                data_context=self._data_context,
            )
            self._data_assistant_runner_cache[
                data_assistant_name
            ] = data_assistant_runner

        return data_assistant_runner
