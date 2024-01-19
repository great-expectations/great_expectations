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


class DataContextInitializedEvent(Event):
    """
    Emitted when a DataContext is initialized.
      - AbstractDataContext.__init__
    """

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
    """
    TBD
    """

    expectation_type: str = "UNKNOWN"
    custom_exp_type: bool = False

    _allowed_actions: ClassVar[List[Action]] = [
        EXPECTATION_SUITE_EXPECTATION_CREATED,
    ]

    @override
    def _properties(self) -> dict:
        return {
            **super()._properties(),
            "expectation_type": self.expectation_type,
            "custom_exp_type": self.custom_exp_type,
        }


@dataclass
class ExpectationSuiteExpectationUpdatedEvent(_ExpectationSuiteExpectationEvent):
    """
    TBD
    """

    _allowed_actions: ClassVar[List[Action]] = [
        EXPECTATION_SUITE_EXPECTATION_UPDATED,
    ]


@dataclass
class ExpectationSuiteExpectationDeletedEvent(_ExpectationSuiteExpectationEvent):
    """
    TBD
    """

    _allowed_actions: ClassVar[List[Action]] = [
        EXPECTATION_SUITE_EXPECTATION_DELETED,
    ]


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
    """
    Emitted when an ExpectationSuite is created.
      - SuiteFactory.add
    """

    _allowed_actions: ClassVar[List[Action]] = [EXPECTATION_SUITE_CREATED]


@dataclass
class ExpectationSuiteDeletedEvent(_ExpectationSuiteEvent):
    """
    Emitted when an ExpectationSuite is deleted.
      - SuiteFactory.delete
    """

    _allowed_actions: ClassVar[List[Action]] = [EXPECTATION_SUITE_DELETED]
