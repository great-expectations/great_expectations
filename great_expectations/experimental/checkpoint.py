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
        existing_actions = self._checkpoint.action_list
        if action in existing_actions:
            raise ValueError(
                "The provided action already exists in this Checkpoint's action list"
            )

        updated_actions = existing_actions.append(action)
        self.action_list = updated_actions

    def add_validation(self, validation: dict | CheckpointValidationConfig) -> None:
        if isinstance(validation, dict):
            validation = CheckpointValidationConfig(**validation)

        existing_validations = self._checkpoint.validations
        if validation in existing_validations:
            raise ValueError(
                "The provided validation already exists in this Checkpoint's validations list"
            )

        updated_validations = existing_validations.append(validation)
        self.validations = updated_validations

    def run(self) -> CheckpointResult:
        # TODO: expand with more args?
        return self._checkpoint.run()
