from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Dict, List, Set, Type

from great_expectations.rule_based_profiler.data_assistant import (
    DataAssistant,  # noqa: TCH001
)
from great_expectations.rule_based_profiler.data_assistant.data_assistant_runner import (
    DataAssistantRunner,
)

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )

logger = logging.getLogger(__name__)


class DataAssistantDispatcher:
    """
    DataAssistantDispatcher intercepts requests for "DataAssistant" classes by their registered names and manages their
    associated "DataAssistantRunner" objects, which process invocations of calls to "DataAssistant" "run()" methods.
    """

    _registered_data_assistants: Dict[str, Type[DataAssistant]] = {}

    def __init__(self, data_context: AbstractDataContext) -> None:
        """
        Args:
            data_context: AbstractDataContext associated with DataAssistantDispatcher
        """
        self._data_context = data_context

        self._data_assistant_runner_cache: Dict[str, DataAssistantRunner] = {}

    def __getattr__(self, name: str) -> DataAssistantRunner:
        # Both, registered data_assistant_type and alias name are supported for invocation.

        # _registered_data_assistants has both aliases and full names
        data_assistant_cls: Type[
            DataAssistant
        ] | None = DataAssistantDispatcher._get_data_assistant_impl(name=name)

        # If "DataAssistant" is not registered, then raise "AttributeError", which is appropriate for "__getattr__()".
        if data_assistant_cls is None:
            raise AttributeError(
                f'"{type(self).__name__}" object has no attribute "{name}".'
            )

        data_assistant_name: str = data_assistant_cls.data_assistant_type
        data_assistant_runner: DataAssistantRunner | None = (
            self._data_assistant_runner_cache.get(data_assistant_name)
        )
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
    def _register_data_assistant(
        cls,
        data_assistant: Type[DataAssistant],
    ) -> None:
        """
        This method registers "DataAssistant" subclass for future instantiation and execution of its "run()" method.

        Args:
            data_assistant: "DataAssistant" class to be registered
        """
        # TODO: <Alex>6/21/2022: Only alias is to be registered (leave formal type registration in as commented).</Alex>
        # data_assistant_type = data_assistant.data_assistant_type
        # cls._register(data_assistant_type, data_assistant)

        alias: str | None = data_assistant.__alias__
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
    def _get_data_assistant_impl(
        cls,
        name: str | None,
    ) -> Type[DataAssistant] | None:
        """
        This method obtains (previously registered) "DataAssistant" class from DataAssistant Registry.

        Note that it will clean the input string before checking against registered assistants.

        Args:
            name: String representing "snake case" version of "DataAssistant" class type

        Returns:
            Class inheriting "DataAssistant" if found; otherwise, None
        """
        if name is None:
            return None

        name = name.lower()

        return cls._registered_data_assistants.get(name)

    def __dir__(self) -> List[str]:
        """
        This custom magic method is used to enable tab completion on "DataAssistantDispatcher" objects.
        """
        data_assistant_dispatcher_attrs: Set[str] = set(super().__dir__())
        data_assistant_registered_names: Set[
            str
        ] = get_registered_data_assistant_names()
        combined_dir_attrs: Set[str] = (
            data_assistant_dispatcher_attrs | data_assistant_registered_names
        )
        return list(combined_dir_attrs)


def get_registered_data_assistant_names() -> Set[str]:
    """
    This method returns names (registered data_assistant_type and alias name) of registered "DataAssistant" classes.
    """
    return set(DataAssistantDispatcher._registered_data_assistants.keys())
