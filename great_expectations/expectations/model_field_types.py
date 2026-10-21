from typing import List, Literal, Union, get_args

from great_expectations.compatibility.pydantic import Field
from great_expectations.compatibility.typing_extensions import Annotated
from great_expectations.core.suite_parameters import (
    SuiteParameterDict,  # used in pydantic validation
)
from great_expectations.expectations.model_field_descriptions import (
    LIKE_PATTERN_ESCAPE_DESCRIPTION,
    MOSTLY_DESCRIPTION,
    VALUE_SET_DESCRIPTION,
)

MostlyField = Annotated[
    Union[float, SuiteParameterDict],
    Field(
        description=MOSTLY_DESCRIPTION,
        ge=0.0,
        le=1.0,
        # This is just for the schema, it should not be validated on input
        schema_overrides={"multipleOf": 0.01},
    ),
]

ValueSetField = Annotated[
    Union[List, SuiteParameterDict],
    Field(
        title="Value Set",
        description=VALUE_SET_DESCRIPTION,
        schema_overrides={
            "anyOf": [
                {
                    "title": "Value Set",
                    "description": VALUE_SET_DESCRIPTION,
                    "oneOf": [
                        {
                            "title": "Text",
                            "type": "array",
                            "items": {"type": "string", "minLength": 1},
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
                            "items": {"type": "number"},
                            "minItems": 1,
                            "examples": [
                                [1, 2, 3, 4, 5],
                                [1.1, 2.2, 3.3, 4.4, 5.5],
                                [1, 2.2, 3, 4.4, 5],
                            ],
                        },
                    ],
                },
                {"type": "object"},
            ]
        },
    ),
]


LikePatternEscapeField = Annotated[
    Union[str, SuiteParameterDict, None],
    Field(
        description=LIKE_PATTERN_ESCAPE_DESCRIPTION,
        # Schema-only, deliberately. Declaring the length on the string branch itself --
        # via constr or Field(min_length=...) -- changes runtime behavior for the worse:
        # an empty string then fails that branch, falls through to SuiteParameterDict, and
        # dict("") silently yields {}, so escape="" is accepted as an empty suite
        # parameter instead of being rejected. `validate_like_pattern_escape` enforces the
        # same rule at validation time, with a message that names the actual problem.
        schema_overrides={
            "anyOf": [
                {"type": "string", "minLength": 1, "maxLength": 1},
                {"type": "object"},
            ]
        },
    ),
]


def validate_like_pattern_escape(
    escape: Union[str, SuiteParameterDict, None],
) -> Union[str, SuiteParameterDict, None]:
    """Reject an escape that is not exactly one character.

    SQL permits a single escape character; a longer string renders an ESCAPE clause the
    database rejects, and an empty one renders an invalid clause. Failing here names the
    problem rather than surfacing it as a syntax error from the backend.

    Shared by the four LIKE pattern Expectations, which attach it with
    `validator("escape", allow_reuse=True)`.
    """
    if isinstance(escape, str) and len(escape) != 1:
        raise ValueError("escape must be a single character.")  # noqa: TRY003 # FIXME CoP

    return escape


# If you re-order these strings you must reorder the variable names from the `get_args`
# call below as well or the variables will reference the wrong string
ConditionParser = Literal[
    "great_expectations", "great_expectations__experimental__", "pandas", "spark"
]

(
    CONDITION_PARSER_GREAT_EXPECTATIONS,
    CONDITION_PARSER_GREAT_EXPECTATIONS_DEPRECATED,
    CONDITION_PARSER_PANDAS,
    CONDITION_PARSER_SPARK,
) = get_args(ConditionParser)
