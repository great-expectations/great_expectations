
from great_expectations.checkpoint.configurator import ActionDict
from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.types.base import CheckpointValidationConfig


class ExperimentalCheckpoint:
    def __init__(
        self,
        name: str,
        data_context: AbstractDataContext,
        action_list: list[ActionDict],
        validations: list[CheckpointValidationConfig],
    ) -> None:
        self._name = name
        self._data_context = data_context
        self._action_list = action_list
        self._validations = validations


    def add_action(self, action: ActionDict) -> None:
        pass

    def add_validation(self, validation: CheckpointValidationConfig) -> None:
        pass
