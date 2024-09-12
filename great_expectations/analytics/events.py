from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar, List

from great_expectations.analytics.actions import (
    CHECKPOINT_CREATED,
    CHECKPOINT_DELETED,
    CHECKPOINT_RAN,
    DATA_CONTEXT_INITIALIZED,
    DOMAIN_OBJECT_ALL_DESERIALIZE_ERROR,
    EXPECTATION_SUITE_CREATED,
    EXPECTATION_SUITE_DELETED,
    EXPECTATION_SUITE_EXPECTATION_CREATED,
    EXPECTATION_SUITE_EXPECTATION_DELETED,
    EXPECTATION_SUITE_EXPECTATION_UPDATED,
    VALIDATION_DEFINITION_CREATED,
    VALIDATION_DEFINITION_DELETED,
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


@dataclass
class _CheckpointEvent(Event):
    checkpoint_id: str | None = None

    @override
    def _properties(self) -> dict:
        return {
            "checkpoint_id": self.checkpoint_id,
        }


@dataclass
class CheckpointCreatedEvent(_CheckpointEvent):
    _allowed_actions: ClassVar[List[Action]] = [CHECKPOINT_CREATED]

    def __init__(
        self,
        checkpoint_id: str | None = None,
        validation_definition_ids: list[str | None] | None = None,
    ):
        self.validation_definition_ids = validation_definition_ids
        super().__init__(
            action=CHECKPOINT_CREATED,
            checkpoint_id=checkpoint_id,
        )

    @override
    def _properties(self) -> dict:
        return {
            "validation_definition_ids": self.validation_definition_ids,
            **super()._properties(),
        }


@dataclass
class CheckpointDeletedEvent(_CheckpointEvent):
    _allowed_actions: ClassVar[List[Action]] = [CHECKPOINT_DELETED]

    def __init__(self, checkpoint_id: str | None = None):
        super().__init__(
            action=CHECKPOINT_DELETED,
            checkpoint_id=checkpoint_id,
        )


@dataclass
class CheckpointRanEvent(_CheckpointEvent):
    _allowed_actions: ClassVar[List[Action]] = [CHECKPOINT_RAN]

    def __init__(
        self,
        checkpoint_id: str | None = None,
        validation_definition_ids: list[str | None] | None = None,
    ):
        self.validation_definition_ids = validation_definition_ids
        super().__init__(
            action=CHECKPOINT_RAN,
            checkpoint_id=checkpoint_id,
        )

    @override
    def _properties(self) -> dict:
        return {
            "validation_definition_ids": self.validation_definition_ids,
            **super()._properties(),
        }


@dataclass
class _ValidationDefinitionEvent(Event):
    validation_definition_id: str | None = None

    @override
    def _properties(self) -> dict:
        return {
            "validation_definition_id": self.validation_definition_id,
        }


@dataclass
class ValidationDefinitionCreatedEvent(_ValidationDefinitionEvent):
    _allowed_actions: ClassVar[List[Action]] = [VALIDATION_DEFINITION_CREATED]

    def __init__(self, validation_definition_id: str | None = None):
        super().__init__(
            action=VALIDATION_DEFINITION_CREATED,
            validation_definition_id=validation_definition_id,
        )


@dataclass
class ValidationDefinitionDeletedEvent(_ValidationDefinitionEvent):
    _allowed_actions: ClassVar[List[Action]] = [VALIDATION_DEFINITION_DELETED]

    def __init__(self, validation_definition_id: str | None = None):
        super().__init__(
            action=VALIDATION_DEFINITION_DELETED,
            validation_definition_id=validation_definition_id,
        )


@dataclass
class DomainObjectAllDeserializationEvent(Event):
    _allowed_actions: ClassVar[List[Action]] = [DOMAIN_OBJECT_ALL_DESERIALIZE_ERROR]

    store_name: str
    error_type: str

    def __init__(self, store_name: str, error_type: str):
        super().__init__(action=DOMAIN_OBJECT_ALL_DESERIALIZE_ERROR)
        self.error_type = error_type
        self.store_name = store_name

    @override
    def _properties(self) -> dict:
        return {
            "error_type": self.error_type,
            "store_name": self.store_name,
        }
