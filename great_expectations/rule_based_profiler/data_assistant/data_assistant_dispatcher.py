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

        # _registered_data_assistants has both aliases and full names
        data_assistant_cls: Optional[
            Type[DataAssistant]
        ] = DataAssistantDispatcher.get_data_assistant_impl(name=name)

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

    @classmethod
    def register_data_assistant(
        cls,
        data_assistant: Type[DataAssistant],  # noqa: F821
    ) -> None:
        """
        This method executes "run()" of effective "RuleBasedProfiler" and fills "DataAssistantResult" object with outputs.

        Args:
            data_assistant: "DataAssistant" class to be registered
        """
        data_assistant_type = data_assistant.data_assistant_type
        cls._register(data_assistant_type, data_assistant)

        alias: Optional[str] = data_assistant.__alias__
        if alias is not None:
            cls._register(alias, data_assistant)

    @classmethod
    def _register(cls, name: str, data_assistant: Type[DataAssistant]) -> None:
        registered_data_assistants = cls._registered_data_assistants

        if name in registered_data_assistants:
            raise ValueError(f'Existing declarations of DataAssistant "{name}" found.')

        logger.debug(
            f'Registering the declaration of DataAssistant "{name}" took place.'
        )
        registered_data_assistants[name] = data_assistant

    @classmethod
    def get_data_assistant_impl(
        cls,
        name: Optional[str],
    ) -> Optional[Type[DataAssistant]]:  # noqa: F821
        """
        This method obtains (previously registered) "DataAssistant" class from DataAssistant Registry.

        Note that it will clean the input string before checking against registered assistants.

        Args:
            data_assistant_type: String representing "snake case" version of "DataAssistant" class type

        Returns:
            Class inheriting "DataAssistant" if found; otherwise, None
        """
        if name is None:
            return None
        name = name.lower()
        return cls._registered_data_assistants.get(name)
