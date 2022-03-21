from abc import ABC, abstractmethod, abstractstaticmethod


class BaseAnonymizer(ABC):
    @abstractmethod
    def anonymize(obj: object) -> dict:
        raise NotImplementedError

    @abstractstaticmethod
    def can_handle(obj: object) -> bool:
        raise NotImplementedError
