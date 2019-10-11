import logging
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class DataContextKey(ABC):
    """DataContextKey objects are used to uniquely identify resources used by the DataContext.

    A DataContextKey is designed to support clear naming with multiple representations including a hashable
    version making it suitable for use as the key in a dictionary.
    """
    @abstractmethod
    def to_tuple(self):
        pass

    @classmethod
    def from_tuple(cls, tuple_):
        return cls(*tuple_)

    def __eq__(self, other):
        return self.to_tuple() == other.to_tuple()

    def __hash__(self):
        return hash(self.to_tuple())
