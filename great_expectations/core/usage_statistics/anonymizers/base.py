from abc import ABC, abstractmethod


class AbstractAnonymizer(ABC):
    @abstractmethod
    def anonymize(self, obj: object):
        pass

    @abstractmethod
    def can_handle(self, obj: object) -> bool:
        pass
