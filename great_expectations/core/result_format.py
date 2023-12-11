from __future__ import annotations

import enum

from typing_extensions import TypedDict


class ResultFormat(str, enum.Enum):
    BOOLEAN_ONLY = "BOOLEAN_ONLY"
    BASIC = "BASIC"
    COMPLETE = "COMPLETE"
    SUMMARY = "SUMMARY"


class ResultFormatDict(TypedDict):
    result_format: ResultFormat
