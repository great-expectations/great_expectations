from typing import List, Union

from typing_extensions import Annotated

from great_expectations.compatibility.pydantic import Field, StrictStr, conlist
from great_expectations.core.suite_parameters import SuiteParameterDict
from great_expectations.expectations.model_field_descriptions import (
    MOSTLY_DESCRIPTION,
    VALUE_SET_DESCRIPTION,
)

Mostly = Annotated[
    float,
    Field(ge=0.0, le=1.0, description=MOSTLY_DESCRIPTION),
]


ColumnList = Annotated[List[StrictStr], conlist(item_type=StrictStr, min_items=1)]


ValueSet = Annotated[
    Union[None, SuiteParameterDict, list, set],
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
