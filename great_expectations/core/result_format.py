from __future__ import annotations

import enum


class ResultFormat(str, enum.Enum):
    BOOLEAN_ONLY = "BOOLEAN_ONLY"
    BASIC = "BASIC"
    COMPLETE = "COMPLETE"
    SUMMARY = "SUMMARY"
