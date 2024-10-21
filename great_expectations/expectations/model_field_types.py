from enum import Enum
from numbers import Number
from typing import Any, Callable, Dict, Generator, Iterable, Union

from great_expectations.compatibility.pydantic import fields
from great_expectations.compatibility.typing_extensions import Annotated, override
from great_expectations.expectations.model_field_descriptions import (
    MOSTLY_DESCRIPTION,
    VALUE_SET_DESCRIPTION,
)


class _Mostly(Number):
    """Mostly is a custom float type that constrains the input between 0.0 and 1.0.
    The multipleOf field should be set in the schemas for GX Cloud component control,
    but multipleOf should not be validated on input."""

    @override
    def __hash__(self: Number):
        return hash(self)

    @classmethod
    def __get_validators__(cls) -> Generator[Callable, None, None]:
        yield cls.validate

    @classmethod
    def validate(cls, v: float) -> float:
        if v is None:
            msg = "Mostly cannot be None"
            raise TypeError(msg)
        if not isinstance(v, Number):
            msg = "Mostly is not a valid float."
            raise TypeError(msg)
        if v < 0.0:
            msg = "Mostly must be greater than or equal to 0."
            raise TypeError(msg)
        if v > 1.0:
            msg = "Mostly must be less than or equal to 1."
            raise TypeError(msg)
        return v

    @classmethod
    def __modify_schema__(cls, field_schema: Dict[str, Any], field: Union[fields.ModelField, None]):
        if field:
            field_schema["description"] = MOSTLY_DESCRIPTION
            field_schema["type"] = "number"
            field_schema["minimum"] = 0.0
            field_schema["maximum"] = 1.0
            field_schema["multipleOf"] = 0.01


Mostly = Annotated[_Mostly, float]


class _ValueSet(Iterable):
    """ValueSet is a custom Iterable type."""

    @override
    def __iter__(self):
        return self

    @classmethod
    def __get_validators__(cls) -> Generator[Callable, None, None]:
        yield cls.validate

    @classmethod
    def validate(cls, v: Union[list, set]) -> Union[list, set]:
        if not isinstance(v, (list, set)):
            msg = "ValueSet is not a valid type."
            raise TypeError(msg)
        return v

    @classmethod
    def __modify_schema__(cls, field_schema: Dict[str, Any], field: Union[fields.ModelField, None]):
        if field:
            field_schema["title"] = "Value Set"
            field_schema["description"] = VALUE_SET_DESCRIPTION
            field_schema["oneOf"] = [
                {
                    "title": "Text",
                    "type": "array",
                    "items": {
                        "type": "string",
                        "minLength": 1,
                    },
                    "minItems": 1,
                    "examples": [
                        ["a", "b", "c", "d", "e"],
                        [
                            "2024-01-01",
                            "2024-01-02",
                            "2024-01-03",
                            "2024-01-04",
                            "2024-01-05",
                        ],
                    ],
                },
                {
                    "title": "Numbers",
                    "type": "array",
                    "items": {
                        "type": "number",
                    },
                    "minItems": 1,
                    "examples": [
                        [1, 2, 3, 4, 5],
                        [1.1, 2.2, 3.3, 4.4, 5.5],
                        [1, 2.2, 3, 4.4, 5],
                    ],
                },
            ]


ValueSet = Annotated[_ValueSet, Union[list, set]]


class ConditionParser(str, Enum):
    """Type of parser to be used to interpret a Row Condition."""

    GX = "great_expectations"
    GX_DEPRECATED = "great_expectations__experimental__"
    PANDAS = "pandas"
    SPARK = "spark"
