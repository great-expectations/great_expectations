from collections.abc import Callable, Generator
from enum import Enum
from typing import Any

from great_expectations.compatibility import pydantic


class Mostly(pydantic.ConstrainedFloat):
    ge = 0.0
    le = 1.0


class Column(pydantic.ConstrainedStr):
    min_length = 1


ColumnList = pydantic.conlist(item_type=Column, min_items=1)


class ColumnType(pydantic.ConstrainedStr):
    min_length = 1


class ValueSet:
    # A custom type to modify the schema for FE JSON form limitations
    # https://docs.pydantic.dev/1.10/usage/types/#custom-data-types
    @classmethod
    def __get_validators__(cls) -> Generator[Callable, None, None]:
        yield cls.validate

    @classmethod
    def validate(cls, v: list[Any]) -> list[Any]:
        # Ensure list has at least one item
        if not v:
            raise TypeError("Value Set must contain at least one item.")  # noqa: TRY003  # this is not a long message
        if any(isinstance(value, str) and value.strip() == "" for value in v):
            raise TypeError("Value Set cannot contain empty items.")  # noqa: TRY003  # this is not a long message
        return v

    @classmethod
    def __modify_schema__(
        cls, field_schema: dict[str, Any], field: pydantic.fields.ModelField | None
    ) -> None:
        # We need to override the schema, because the JSON form for Expectation input requires
        # that the input be either all strings or numbers. We do not validate that this is
        # actually the case because using the FE JSON Editor (or Python API) users can have
        # mixed types in their value_set.
        if field:
            field_schema["title"] = "Value Set"
            field_schema["oneOf"] = (
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
            )


class ConditionParser(str, Enum):
    PANDAS = "pandas"
    EXPERIMENTAL = "great_expectations__experimental__"
