from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Union

from great_expectations import project_manager
from great_expectations._docs_decorators import public_api
from great_expectations.checkpoint.actions import ValidationAction  # noqa: TCH001
from great_expectations.compatibility.pydantic import BaseModel, validator
from great_expectations.core.serdes import _IdentifierBundle
from great_expectations.core.validation_config import ValidationConfig
from great_expectations.render.renderer.renderer import Renderer

if TYPE_CHECKING:
    from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult


def _encode_validation_config(validation: ValidationConfig) -> dict:
    if not validation.id:
        validation_config_store = project_manager.get_validation_config_store()
        key = validation_config_store.get_key(name=validation.name, id=None)
        validation_config_store.add(key=key, value=validation)

    return _IdentifierBundle(name=validation.name, id=validation.id)


class Checkpoint(BaseModel):
    """
    A Checkpoint is the primary means for validating data in a production deployment of Great Expectations.

    Checkpoints provide a convenient abstraction for running a number of validations and triggering a set of actions
    to be taken after the validation step.

    Args:
        validations: List of validation configs to be run.
        actions: List of actions to be taken after the validations are run.

    """

    name: str
    validations: List[ValidationConfig]
    actions: List[ValidationAction]
    id: Union[str, None] = None

    class Config:
        arbitrary_types_allowed = (
            True  # Necessary for compatibility with ValidationAction's Marshmallow dep
        )
        """
        When serialized, the validations field should be encoded as a set of identifiers.
        These will be used as foreign keys to retrieve the actual objects from the appropriate stores.

        Example:
        {
            "name": "my_checkpoint",
            "validations": [
                {
                    "name": "my_first_validation",
                    "id": "a758816-64c8-46cb-8f7e-03c12cea1d67"
                },
                {
                    "name": "my_second_validation",
                    "id": "1339js16-64c8-46cb-8f7e-03c12cea1d67"
                },
            ],
            "actions": [
                {
                    "name": "my_slack_action",
                    "slack_webhook": "https://hooks.slack.com/services/ABC123/DEF456/XYZ789",
                    "notify_on": "all",
                    "notify_with": ["my_data_docs_site"],
                    "renderer": {
                        "class_name": "SlackRenderer",
                    }
                }
        """
        json_encoders = {
            ValidationConfig: lambda v: _encode_validation_config(v),
            Renderer: lambda r: r.serialize(),
        }

    @validator("validations")
    def _validate_validations(
        cls, validations: list[ValidationConfig]
    ) -> list[ValidationConfig]:
        if len(validations) == 0:
            raise ValueError("Checkpoint must contain at least one validation")

        return validations

    @public_api
    def run(
        self,
        batch_params: Dict[str, Any] | None = None,
        suite_params: Dict[str, Any] | None = None,
    ) -> CheckpointResult:
        raise NotImplementedError

    @public_api
    def save(self) -> None:
        raise NotImplementedError
