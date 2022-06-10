from abc import ABC, abstractmethod

from great_expectations.data_context.types.data_context_variables import (
    DataContextVariables,
)


class AbstractDataContext(ABC):
    """
    Base class for all DataContexts that contain all context-agnostic data context operations.

    The class encapsulates most store / core components and convenience methods used to access them, meaning the
    majority of DataContext functionality lives here.

    TODO: eventually the dependency on ConfigPeer will be removed and this will become a pure ABC.
    """

    @abstractmethod
    def _init_variables(self) -> None:
        raise NotImplementedError

    @property
    def variables(self) -> DataContextVariables:
        return self._variables
