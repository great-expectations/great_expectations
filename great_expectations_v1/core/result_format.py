from __future__ import annotations

import enum
from typing import Final, Literal, Union


class ResultFormat(str, enum.Enum):
    BOOLEAN_ONLY = "BOOLEAN_ONLY"
    BASIC = "BASIC"
    COMPLETE = "COMPLETE"
    SUMMARY = "SUMMARY"


ResultFormatUnion = Union[
    ResultFormat, dict, Literal["BOOLEAN_ONLY", "BASIC", "SUMMARY", "COMPLETE"]
]

DEFAULT_RESULT_FORMAT: Final = ResultFormat.SUMMARY
