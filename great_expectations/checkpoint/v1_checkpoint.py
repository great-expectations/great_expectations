from __future__ import annotations

import datetime as dt
import json
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union, cast

import great_expectations.exceptions as gx_exceptions
from great_expectations import project_manager
from great_expectations._docs_decorators import public_api
from great_expectations.checkpoint.actions import ValidationAction  # noqa: TCH001
from great_expectations.compatibility.pydantic import BaseModel, root_validator, validator
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,  # noqa: TCH001
)
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.core.serdes import _IdentifierBundle
from great_expectations.core.validation_config import ValidationConfig
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.render.renderer.renderer import Renderer

if TYPE_CHECKING:
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

    """  # noqa: E501

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
        """  # noqa: E501

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
            raise ValueError("Checkpoint must contain at least one validation definition")

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
                raise ValueError(f"Unable to retrieve validation definition {id_bundle} from store")

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
        run_id = RunIdentifier(run_time=dt.datetime.now(dt.timezone.utc))
        run_results = self._run_validation_definitions(
            batch_parameters=batch_parameters,
            expectation_parameters=expectation_parameters,
            run_id=run_id,
        )
        self._run_actions(run_results=run_results)

        return CheckpointResult(
            run_id=run_id,
            run_results=run_results,
            checkpoint_config=self,
            validation_result_url=None,  # Need to plumb from actions
        )

    def _run_validation_definitions(
        self,
        batch_parameters: Dict[str, Any] | None,
        expectation_parameters: Dict[str, Any] | None,
        run_id: RunIdentifier,
    ) -> Dict[ValidationResultIdentifier, ExpectationSuiteValidationResult]:
        run_results: Dict[ValidationResultIdentifier, ExpectationSuiteValidationResult] = {}
        for validation_definition in self.validation_definitions:
            key = self._build_result_key(validation_definition=validation_definition, run_id=run_id)
            validation_result = validation_definition.run(
                batch_definition_options=batch_parameters,
                evaluation_parameters=expectation_parameters,
            )
            run_results[key] = validation_result

        return run_results

    def _build_result_key(
        self, validation_definition: ValidationConfig, run_id: RunIdentifier
    ) -> ValidationResultIdentifier:
        return ValidationResultIdentifier(
            expectation_suite_identifier=ExpectationSuiteIdentifier(
                name=validation_definition.suite.name
            ),
            run_id=run_id,
            batch_identifier=validation_definition.active_batch_id,
        )

    def _run_actions(
        self, run_results: Dict[ValidationResultIdentifier, ExpectationSuiteValidationResult]
    ) -> None:
        # TODO: Open to implement once actions are updated to run on aggregated results
        pass

    @public_api
    def save(self) -> None:
        raise NotImplementedError


class CheckpointResult(BaseModel):
    run_id: RunIdentifier
    run_results: Dict[ValidationResultIdentifier, ExpectationSuiteValidationResult]
    checkpoint_config: Checkpoint
    validation_result_url: Optional[str] = None
    success: Optional[bool] = None

    class Config:
        arbitrary_types_allowed = True

    @root_validator
    def _root_validate_success(cls, values: dict) -> dict:
        run_results = values["run_results"]
        if len(run_results) == 0:
            raise ValueError("CheckpointResult must contain at least one run result")

        values["success"] = all(result.success for result in run_results.values())
        return values

    @property
    def name(self) -> str:
        return self.checkpoint_config.name

    def describe_dict(self) -> dict:
        success_count = sum(1 for r in self.run_results.values() if r.success)
        run_result_descriptions = [r.describe_dict() for r in self.run_results.values()]

        return {
            "success": success_count == len(run_result_descriptions),
            "success_percent": success_count / len(run_result_descriptions) * 100,
            "validation_results": run_result_descriptions,
        }

    @public_api
    def describe(self) -> str:
        """JSON string description of this CheckpointResult"""
        return json.dumps(self.describe_dict(), indent=4)


CheckpointResult.update_forward_refs()
