import json

from great_expectations.core import IDDict
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.types.base import SerializableDotDict


class ExceptionInfo(SerializableDotDict):
    def __init__(
        self,
        exception_traceback: str,
        exception_message: str,
        raised_exception: bool = True,
    ) -> None:
        super().__init__(
            exception_traceback=exception_traceback,
            exception_message=exception_message,
            raised_exception=raised_exception,
        )

    def to_json_dict(self) -> dict:
        fields_dict: dict = {
            "exception_traceback": self.exception_traceback,
            "exception_message": self.exception_message,
            "raised_exception": self.raised_exception,
        }
        return convert_to_json_serializable(fields_dict)

    @property
    def exception_traceback(self) -> str:
        return self["exception_traceback"]

    @property
    def exception_message(self) -> str:
        return self["exception_message"]

    @property
    def raised_exception(self) -> bool:
        return self["raised_exception"]

    def __repr__(self) -> str:
        fields_dict: dict = {
            "exception_traceback": self.exception_traceback,
            "exception_message": self.exception_message,
            "raised_exception": self.raised_exception,
        }
        return str(fields_dict)

    @property
    def id(self) -> str:
        return IDDict(self.to_json_dict()).to_id()

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            # Delegate comparison to the other instance's __eq__.
            return NotImplemented
        return self.id == other.id

    def __ne__(self, other):
        return not self.__eq__(other=other)

    def __str__(self):
        return json.dumps(self.to_json_dict(), indent=2)

    def __hash__(self) -> int:  # type: ignore[override] # standard Python dicts are unhashable because of mutability
        """Overrides the default implementation"""
        _result_hash: int = hash(self.id)
        return _result_hash
