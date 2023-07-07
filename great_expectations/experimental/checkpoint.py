from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from great_expectations.checkpoint import Checkpoint as LegacyCheckpoint
from great_expectations.data_context.types.base import CheckpointValidationConfig

if TYPE_CHECKING:
    from great_expectations.checkpoint.configurator import ActionDict
    from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )


# TODO: to be renamed once we're good to move this out of experimental
class ExperimentalCheckpoint:
    def __init__(
        self,
        name: str,
        data_context: AbstractDataContext,
        action_list: Sequence[ActionDict] | None = None,
        validations: list[dict] | list[CheckpointValidationConfig] | None = None,
    ) -> None:
        self._checkpoint = LegacyCheckpoint(
            name=name,
            data_context=data_context,
            action_list=action_list,
            validations=validations,
        )

    @property
    def name(self) -> str:
        return self._checkpoint.name

    @property
    def action_list(self) -> Sequence[ActionDict]:
        return self._checkpoint.action_list

    @action_list.setter
    def action_list(self, actions: Sequence[ActionDict]) -> None:
        self._checkpoint.config.action_list = actions

    @property
    def validations(self) -> list[CheckpointValidationConfig]:
        return self._checkpoint.validations

    @validations.setter
    def validations(
        self, validations_: list[dict] | list[CheckpointValidationConfig]
    ) -> None:
        validations = [
            CheckpointValidationConfig(**v) if isinstance(v, dict) else v
            for v in validations_
        ]
        self._checkpoint.config.validations = validations

    def add_action(self, action: ActionDict) -> None:
        if action in self.action_list:
            raise ValueError(
                "The provided action already exists in this Checkpoint's action list"
            )
        self.action_list.append(action)

    def add_validation(self, validation: dict | CheckpointValidationConfig) -> None:
        if isinstance(validation, dict):
            validation = CheckpointValidationConfig(**validation)

        if validation in self.validations:
            raise ValueError(
                "The provided validation already exists in this Checkpoint's validations list"
            )
        self.validations.append(validation)

    def run(self) -> CheckpointResult:
        # TODO: expand with more args?
        return self._checkpoint.run()
