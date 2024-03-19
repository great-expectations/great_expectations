from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Union, cast

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

    Checkpoints provide a convenient abstraction for running a number of validation definnitions and triggering a set of actions
    to be taken after the validation step.

    Args:
        validation_definitions: List of validation definitions to be run.
        actions: List of actions to be taken after the validation definitions are run.

    """

    name: str
    validation_definitions: List[ValidationConfig]
    actions: List[ValidationAction]
    id: Union[str, None] = None

    class Config:
        """
        When serialized, the validation_definitions field will be encoded as a set of identifiers.
        These will be used as foreign keys to retrieve the actual objects from the appropriate stores.

        Example:
        {
            "name": "my_checkpoint",
            "validation_definitions": [
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

        arbitrary_types_allowed = (
            True  # Necessary for compatibility with ValidationAction's Marshmallow dep
        )
        json_encoders = {
            ValidationConfig: lambda v: v.identifier_bundle(),
            Renderer: lambda r: r.serialize(),
        }

    @validator("validation_definitions", pre=True)
    def _validate_validation_definitions(
        cls, validation_definitions: list[ValidationConfig] | list[dict]
    ) -> list[ValidationConfig]:
        if len(validation_definitions) == 0:
            raise ValueError(
                "Checkpoint must contain at least one validation definition"
            )

        if isinstance(validation_definitions[0], dict):
            validation_config_store = project_manager.get_validation_config_store()
            identifier_bundles = [
                _IdentifierBundle(**v)  # type: ignore[arg-type] # All validation configs are dicts if the first one is
                for v in validation_definitions
            ]
            return cls._deserialize_identifier_bundles_to_validation_configs(
                identifier_bundles=identifier_bundles, store=validation_config_store
            )

        return cast(List[ValidationConfig], validation_definitions)

    @classmethod
    def _deserialize_identifier_bundles_to_validation_configs(
        cls, identifier_bundles: list[_IdentifierBundle], store: ValidationConfigStore
    ) -> list[ValidationConfig]:
        validation_definitions: list[ValidationConfig] = []
        for id_bundle in identifier_bundles:
            key = store.get_key(name=id_bundle.name, id=id_bundle.id)

            try:
                validation_definition = store.get(key=key)
            except (KeyError, gx_exceptions.InvalidKeyError):
                raise ValueError(
                    f"Unable to retrieve validation definition {id_bundle} from store"
                )

            if not validation_definition:
                raise ValueError(
                    "ValidationDefinitionStore did not retrieve a validation definition"
                )
            validation_definitions.append(validation_definition)

        return validation_definitions

    @public_api
    def run(
        self,
        batch_parameters: Dict[str, Any] | None = None,
        expectation_parameters: Dict[str, Any] | None = None,
    ) -> CheckpointResult:
        raise NotImplementedError

    @public_api
    def save(self) -> None:
        raise NotImplementedError
