from typing import Any, Dict, List, Union

from typing_extensions import Annotated

from great_expectations.compatibility.pydantic import Field, StrictStr, confloat, conlist, fields
from great_expectations.core.suite_parameters import SuiteParameterDict
from great_expectations.expectations.model_field_descriptions import (
    MOSTLY_DESCRIPTION,
    VALUE_SET_DESCRIPTION,
)


class Mostly(confloat(ge=0.0, le=1.0), float):
    """Mostly is a custom float type that constrains the input between 0.0 and 1.0.
    The multipleOf field should be set in the schemas for GX Cloud component control,
    but multipleOf should not be validated on input."""

    @classmethod
    def __modify_schema__(cls, field_schema: Dict[str, Any], field: Union[fields.ModelField, None]):
        if field:
            field_schema["description"] = MOSTLY_DESCRIPTION
            field_schema["minimum"] = 0.0
            field_schema["maximum"] = 1.0
            field_schema["multipleOf"] = 0.01


ColumnList = Annotated[List[StrictStr], conlist(item_type=StrictStr, min_items=1)]


ValueSet = Annotated[
    Union[SuiteParameterDict, list, set, None],
    Field(
        title="Value Set",
        description=VALUE_SET_DESCRIPTION,
        oneOf=[
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
        ],
    ),
]
