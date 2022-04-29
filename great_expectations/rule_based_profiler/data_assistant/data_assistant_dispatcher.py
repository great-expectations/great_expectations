import logging
from typing import Dict, Optional, Type

from great_expectations.rule_based_profiler.data_assistant import DataAssistant
from great_expectations.rule_based_profiler.data_assistant.data_assistant_runner import (
    DataAssistantRunner,
)

logger = logging.getLogger(__name__)


class DataAssistantDispatcher:
    """
    DataAssistantDispatcher intercepts requests for "DataAssistant" classes by their registered names and manages their
    associated "DataAssistantRunner" objects, which process invocations of calls to "DataAssistant" "run()" methods.
    """

    _registered_data_assistants: Dict[str, Type[DataAssistant]] = {}

    def __init__(self, data_context: "BaseDataContext") -> None:  # noqa: F821
        """
        Args:
            data_context: BaseDataContext associated with DataAssistantDispatcher
        """
        self._data_context = data_context

        self._data_assistant_runner_cache = {}

    def __getattr__(self, name: str) -> DataAssistantRunner:
        # Both, registered data_assistant_type and alias name are supported for invocation.

        # Attempt to utilize data_assistant_type name for invocation first.
        data_assistant_cls: Optional[
            Type["DataAssistant"]
        ] = DataAssistantDispatcher.get_data_assistant_impl(data_assistant_type=name)

        # If no registered data_assistant_type exists, try aliases for invocation.
        if data_assistant_cls is None:
            for type_ in DataAssistantDispatcher._registered_data_assistants.values():
                if type_.__alias__ == name:
                    data_assistant_cls = type_
                    break

        # If "DataAssistant" is not registered, then raise "AttributeError", which is appropriate for "__getattr__()".
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

    @staticmethod
    def register_data_assistant(
        data_assistant: Type[DataAssistant],  # noqa: F821
    ) -> None:
        """
        This method executes "run()" of effective "RuleBasedProfiler" and fills "DataAssistantResult" object with outputs.

        Args:
            data_assistant: "DataAssistant" class to be registered
        """
        data_assistant_type = data_assistant.data_assistant_type
        registered_data_assistants: Dict[
            str, Type[DataAssistant]
        ] = DataAssistantDispatcher._registered_data_assistants

        if data_assistant_type in registered_data_assistants:
            if registered_data_assistants[data_assistant_type] == data_assistant:
                logger.info(
                    f'Multiple declarations of DataAssistant "{data_assistant_type}" found.'
                )
                return
            else:
                logger.warning(
                    f'Overwriting the declaration of DataAssistant "{data_assistant_type}" took place.'
                )

        logger.debug(
            f'Registering the declaration of DataAssistant "{data_assistant_type}" took place.'
        )
        registered_data_assistants[data_assistant_type] = data_assistant

    @staticmethod
    def get_data_assistant_impl(
        data_assistant_type: str,
    ) -> Optional[Type[DataAssistant]]:  # noqa: F821
        """
        This method obtains (previously registered) "DataAssistant" class from DataAssistant Registry.

        Note that it will clean the input string before checking against registered assistants.

        Args:
            data_assistant_type: String representing "snake case" version of "DataAssistant" class type

        Returns:
            Class inheriting "DataAssistant" if found; otherwise, None
        """
        data_assistant_type = data_assistant_type.lower()
        return DataAssistantDispatcher._registered_data_assistants.get(
            data_assistant_type
        )
