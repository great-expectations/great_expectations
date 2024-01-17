from __future__ import annotations

from typing import ClassVar, List

from great_expectations.analytics.actions import (
    DATA_CONTEXT_INITIALIZED,
    EXPECTATION_CREATED,
    EXPECTATION_DELETED,
    EXPECTATION_SUITE_CREATED,
    EXPECTATION_SUITE_DELETED,
    EXPECTATION_SUITE_UPDATED,
    EXPECTATION_UPDATED,
)
from great_expectations.analytics.base_event import Action, Event
from great_expectations.compatibility.typing_extensions import override


class DataContextInitializedEvent(Event):
    _allowed_actions: ClassVar[List[Action]] = [DATA_CONTEXT_INITIALIZED]

    def __init__(self):
        super().__init__(action=DATA_CONTEXT_INITIALIZED)


class ExpectationEvent(Event):
    expectation_id: str | None = None
    expectation_suite_id: str | None = None
    expectation_type: str = "UNKNOWN"
    custom_exp_type: bool = False

    _allowed_actions: ClassVar[List[Action]] = [
        EXPECTATION_CREATED,
        EXPECTATION_UPDATED,
        EXPECTATION_DELETED,
    ]

    @override
    def _properties(self) -> dict:
        return {
            "expectation_id": self.expectation_id,
            "expectation_suite_id": self.expectation_suite_id,
            "expectation_type": self.expectation_type,
            "custom_epx_type": self.custom_exp_type,
        }


class ExpectationSuiteEvent(Event):
    expectation_suite_id: str | None = None
    expectation_suite_name: str | None = None
    expectation_ids: List[str] | None = None

    _allowed_actions: ClassVar[List[Action]] = [
        EXPECTATION_SUITE_CREATED,
        EXPECTATION_SUITE_UPDATED,
        EXPECTATION_SUITE_DELETED,
    ]

    @override
    def _properties(self) -> dict:
        return {
            "expectation_suite_id": self.expectation_suite_id,
            "expectation_suite_name": self.expectation_suite_name,
            "expectation_ids": self.expectation_ids,
        }
