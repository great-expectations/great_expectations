from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Union

import great_expectations.exceptions as gx_exceptions
from great_expectations import project_manager
from great_expectations._docs_decorators import public_api
from great_expectations.checkpoint.actions import ValidationAction  # noqa: TCH001
from great_expectations.compatibility.pydantic import BaseModel, validator
from great_expectations.core.serdes import _IdentifierBundle
from great_expectations.core.validation_config import ValidationConfig
from great_expectations.render.renderer.renderer import Renderer

if TYPE_CHECKING:
    from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
    from great_expectations.data_context.store.validation_config_store import (
        ValidationConfigStore,
    )


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
            ValidationConfig: lambda v: v.serialize(),
            Renderer: lambda r: r.serialize(),
            # ExpectationSuite: lambda e: e.serialize(),
            # BatchConfig: lambda b: b.serialize(),
        }

    @validator("validations", pre=True)
    def _validate_validations(
        cls, validations: list[ValidationConfig] | list[dict]
    ) -> list[ValidationConfig]:
        if len(validations) == 0:
            raise ValueError("Checkpoint must contain at least one validation")

        if isinstance(validations[0], dict):
            validation_config_store = project_manager.get_validation_config_store()
            identifier_bundles = [_IdentifierBundle(**v) for v in validations]
            return cls._deserialize_identifier_bundles_to_validation_configs(
                identifier_bundles=identifier_bundles, store=validation_config_store
            )

        return validations

    @classmethod
    def _deserialize_identifier_bundles_to_validation_configs(
        cls, identifier_bundles: list[_IdentifierBundle], store: ValidationConfigStore
    ) -> list[ValidationConfig]:
        validations: list[ValidationConfig] = []
        for id_bundle in identifier_bundles:
            key = store.get_key(name=id_bundle.name, id=id_bundle.id)

            try:
                validation_config = store.get(key=key)
            except (KeyError, gx_exceptions.InvalidKeyError):
                raise ValueError(
                    f"Unable to retrieve validation config {id_bundle} from store"
                )

            validations.append(validation_config)

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
