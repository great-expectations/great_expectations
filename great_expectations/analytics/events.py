from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar, List

from great_expectations.analytics.actions import (
    DATA_CONTEXT_INITIALIZED,
    EXPECTATION_SUITE_CREATED,
    EXPECTATION_SUITE_DELETED,
    EXPECTATION_SUITE_EXPECTATION_CREATED,
    EXPECTATION_SUITE_EXPECTATION_DELETED,
    EXPECTATION_SUITE_EXPECTATION_UPDATED,
)
from great_expectations.analytics.base_event import Action, Event
from great_expectations.compatibility.typing_extensions import override


@dataclass
class DataContextInitializedEvent(Event):
    _allowed_actions: ClassVar[List[Action]] = [DATA_CONTEXT_INITIALIZED]

    def __init__(self):
        super().__init__(action=DATA_CONTEXT_INITIALIZED)


@dataclass
class _ExpectationSuiteExpectationEvent(Event):
    expectation_id: str | None = None
    expectation_suite_id: str | None = None

    @override
    def _properties(self) -> dict:
        return {
            "expectation_id": self.expectation_id,
            "expectation_suite_id": self.expectation_suite_id,
        }


@dataclass
class ExpectationSuiteExpectationCreatedEvent(_ExpectationSuiteExpectationEvent):
    expectation_type: str = "UNKNOWN"
    custom_exp_type: bool = False

    _allowed_actions: ClassVar[List[Action]] = [
        EXPECTATION_SUITE_EXPECTATION_CREATED,
    ]

    def __init__(
        self,
        expectation_id: str | None = None,
        expectation_suite_id: str | None = None,
        expectation_type: str = "UNKNOWN",
        custom_exp_type: bool = False,
    ):
        super().__init__(
            action=EXPECTATION_SUITE_EXPECTATION_CREATED,
            expectation_id=expectation_id,
            expectation_suite_id=expectation_suite_id,
        )
        self.expectation_type = expectation_type
        self.custom_exp_type = custom_exp_type

    @override
    def _properties(self) -> dict:
        return {
            **super()._properties(),
            "expectation_type": self.expectation_type,
            "custom_exp_type": self.custom_exp_type,
        }


@dataclass
class ExpectationSuiteExpectationUpdatedEvent(_ExpectationSuiteExpectationEvent):
    _allowed_actions: ClassVar[List[Action]] = [
        EXPECTATION_SUITE_EXPECTATION_UPDATED,
    ]

    def __init__(
        self,
        expectation_id: str | None = None,
        expectation_suite_id: str | None = None,
    ):
        super().__init__(
            action=EXPECTATION_SUITE_EXPECTATION_UPDATED,
            expectation_id=expectation_id,
            expectation_suite_id=expectation_suite_id,
        )


@dataclass
class ExpectationSuiteExpectationDeletedEvent(_ExpectationSuiteExpectationEvent):
    _allowed_actions: ClassVar[List[Action]] = [
        EXPECTATION_SUITE_EXPECTATION_DELETED,
    ]

    def __init__(
        self,
        expectation_id: str | None = None,
        expectation_suite_id: str | None = None,
    ):
        super().__init__(
            action=EXPECTATION_SUITE_EXPECTATION_DELETED,
            expectation_id=expectation_id,
            expectation_suite_id=expectation_suite_id,
        )


@dataclass
class _ExpectationSuiteEvent(Event):
    expectation_suite_id: str | None = None

    @override
    def _properties(self) -> dict:
        return {
            "expectation_suite_id": self.expectation_suite_id,
        }


@dataclass
class ExpectationSuiteCreatedEvent(_ExpectationSuiteEvent):
    _allowed_actions: ClassVar[List[Action]] = [EXPECTATION_SUITE_CREATED]

    def __init__(self, expectation_suite_id: str | None = None):
        super().__init__(
            action=EXPECTATION_SUITE_CREATED,
            expectation_suite_id=expectation_suite_id,
        )


@dataclass
class ExpectationSuiteDeletedEvent(_ExpectationSuiteEvent):
    _allowed_actions: ClassVar[List[Action]] = [EXPECTATION_SUITE_DELETED]

    def __init__(self, expectation_suite_id: str | None = None):
        super().__init__(
            action=EXPECTATION_SUITE_DELETED,
            expectation_suite_id=expectation_suite_id,
        )
