from __future__ import annotations

import datetime as dt
import json
from typing import TYPE_CHECKING, Any, Dict, List, Optional, TypedDict, Union, cast

import great_expectations.exceptions as gx_exceptions
from great_expectations._docs_decorators import public_api
from great_expectations.checkpoint.actions import (
    ActionContext,
    EmailAction,
    MicrosoftTeamsNotificationAction,
    OpsgenieAlertAction,
    PagerdutyAlertAction,
    SlackNotificationAction,
    SNSNotificationAction,
    StoreValidationResultAction,
    UpdateDataDocsAction,
)
from great_expectations.compatibility.pydantic import BaseModel, Field, root_validator, validator
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,  # noqa: TCH001
)
from great_expectations.core.result_format import ResultFormat
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.core.serdes import _IdentifierBundle
from great_expectations.core.validation_definition import ValidationDefinition
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.render.renderer.renderer import Renderer

if TYPE_CHECKING:
    from typing_extensions import TypeAlias

    from great_expectations.data_context.store.validation_definition_store import (
        ValidationDefinitionStore,
    )

CheckpointAction: TypeAlias = Union[
    MicrosoftTeamsNotificationAction,
    OpsgenieAlertAction,
    PagerdutyAlertAction,
    SlackNotificationAction,
    SNSNotificationAction,
    StoreValidationResultAction,
    UpdateDataDocsAction,
    # NOTE: Currently placed last as the root validator emits warnings
    #       that aren't relevant to end users (unless they are using the action).
    EmailAction,
]


class Checkpoint(BaseModel):
    """
    A Checkpoint is the primary means for validating data in a production deployment of Great Expectations.

    Checkpoints provide a convenient abstraction for running a number of validation definnitions and triggering a set of actions
    to be taken after the validation step.

    Args:
        name: The name of the checkpoint.
        validation_definitions: List of validation definitions to be run.
        actions: List of actions to be taken after the validation definitions are run.
        result_format: The format in which to return the results of the validation definitions. Default is ResultFormat.SUMMARY.
        id: An optional unique identifier for the checkpoint.

    """  # noqa: E501

    name: str
    validation_definitions: List[ValidationDefinition]
    actions: List[CheckpointAction] = Field(default_factory=list)
    result_format: ResultFormat = ResultFormat.SUMMARY
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
                    "id": "a58816-64c8-46cb-8f7e-03c12cea1d67"
                },
                {
                    "name": "my_second_validation",
                    "id": "139ab16-64c8-46cb-8f7e-03c12cea1d67"
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
            ],
            "result_format": "SUMMARY",
            "id": "b758816-64c8-46cb-8f7e-03c12cea1d67"
        }
        """  # noqa: E501

        arbitrary_types_allowed = (
            True  # Necessary for compatibility with ValidationAction's Marshmallow dep
        )
        json_encoders = {
            ValidationDefinition: lambda v: v.identifier_bundle(),
            Renderer: lambda r: r.serialize(),
        }

    @validator("validation_definitions", pre=True)
    def _validate_validation_definitions(
        cls, validation_definitions: list[ValidationDefinition] | list[dict]
    ) -> list[ValidationDefinition]:
        from great_expectations import project_manager

        if len(validation_definitions) == 0:
            raise ValueError("Checkpoint must contain at least one validation definition")  # noqa: TRY003

        if isinstance(validation_definitions[0], dict):
            validation_definition_store = project_manager.get_validation_definition_store()
            identifier_bundles = [
                _IdentifierBundle(**v)  # type: ignore[arg-type] # All validation configs are dicts if the first one is
                for v in validation_definitions
            ]
            return cls._deserialize_identifier_bundles_to_validation_definitions(
                identifier_bundles=identifier_bundles, store=validation_definition_store
            )

        return cast(List[ValidationDefinition], validation_definitions)

    @classmethod
    def _deserialize_identifier_bundles_to_validation_definitions(
        cls, identifier_bundles: list[_IdentifierBundle], store: ValidationDefinitionStore
    ) -> list[ValidationDefinition]:
        validation_definitions: list[ValidationDefinition] = []
        for id_bundle in identifier_bundles:
            key = store.get_key(name=id_bundle.name, id=id_bundle.id)

            try:
                validation_definition = store.get(key=key)
            except (KeyError, gx_exceptions.InvalidKeyError):
                raise ValueError(f"Unable to retrieve validation definition {id_bundle} from store")  # noqa: TRY003

            if not validation_definition:
                raise ValueError(  # noqa: TRY003
                    "ValidationDefinitionStore did not retrieve a validation definition"
                )
            validation_definitions.append(validation_definition)

        return validation_definitions

    @public_api
    def run(
        self,
        batch_parameters: Dict[str, Any] | None = None,
        expectation_parameters: Dict[str, Any] | None = None,
        run_id: RunIdentifier | None = None,
    ) -> CheckpointResult:
        run_id = run_id or RunIdentifier(run_time=dt.datetime.now(dt.timezone.utc))
        run_results = self._run_validation_definitions(
            batch_parameters=batch_parameters,
            expectation_parameters=expectation_parameters,
            result_format=self.result_format,
            run_id=run_id,
        )

        checkpoint_result = CheckpointResult(
            run_id=run_id,
            run_results=run_results,
            checkpoint_config=self,
        )
        self._run_actions(checkpoint_result=checkpoint_result)

        return checkpoint_result

    def _run_validation_definitions(
        self,
        batch_parameters: Dict[str, Any] | None,
        expectation_parameters: Dict[str, Any] | None,
        result_format: ResultFormat,
        run_id: RunIdentifier,
    ) -> Dict[ValidationResultIdentifier, ExpectationSuiteValidationResult]:
        run_results: Dict[ValidationResultIdentifier, ExpectationSuiteValidationResult] = {}
        for validation_definition in self.validation_definitions:
            validation_result = validation_definition.run(
                batch_parameters=batch_parameters,
                suite_parameters=expectation_parameters,
                result_format=result_format,
                run_id=run_id,
            )
            key = self._build_result_key(
                validation_definition=validation_definition,
                run_id=run_id,
                batch_identifier=validation_result.batch_id,
            )
            run_results[key] = validation_result

        return run_results

    def _build_result_key(
        self,
        validation_definition: ValidationDefinition,
        run_id: RunIdentifier,
        batch_identifier: Optional[str] = None,
    ) -> ValidationResultIdentifier:
        return ValidationResultIdentifier(
            expectation_suite_identifier=ExpectationSuiteIdentifier(
                name=validation_definition.suite.name
            ),
            run_id=run_id,
            batch_identifier=batch_identifier,
        )

    def _run_actions(
        self,
        checkpoint_result: CheckpointResult,
    ) -> None:
        action_context = ActionContext()
        sorted_actions = self._sort_actions()
        for action in sorted_actions:
            action_result = action.v1_run(
                checkpoint_result=checkpoint_result,
                action_context=action_context,
            )
            action_context.update(action=action, action_result=action_result)

    def _sort_actions(self) -> List[CheckpointAction]:
        """
        UpdateDataDocsActions are prioritized to run first, followed by all other actions.

        This is due to the fact that certain actions reference data docs sites,
        which must be updated first.
        """
        priority_actions: List[CheckpointAction] = []
        secondary_actions: List[CheckpointAction] = []
        for action in self.actions:
            if isinstance(action, UpdateDataDocsAction):
                priority_actions.append(action)
            else:
                secondary_actions.append(action)

        return priority_actions + secondary_actions

    @public_api
    def save(self) -> None:
        from great_expectations import project_manager

        store = project_manager.get_checkpoints_store()
        key = store.get_key(name=self.name, id=self.id)

        store.update(key=key, value=self)


class CheckpointResult(BaseModel):
    run_id: RunIdentifier
    run_results: Dict[ValidationResultIdentifier, ExpectationSuiteValidationResult]
    checkpoint_config: Checkpoint
    success: Optional[bool] = None

    class Config:
        arbitrary_types_allowed = True

    @root_validator
    def _root_validate_result(cls, values: dict) -> dict:
        run_results = values["run_results"]
        if len(run_results) == 0:
            raise ValueError("CheckpointResult must contain at least one run result")  # noqa: TRY003

        if values["success"] is None:
            values["success"] = all(result.success for result in run_results.values())
        return values

    @property
    def name(self) -> str:
        return self.checkpoint_config.name

    def describe_dict(self) -> CheckpointDescriptionDict:
        success_count = sum(1 for r in self.run_results.values() if r.success)
        run_result_descriptions = [r.describe_dict() for r in self.run_results.values()]
        num_results = len(run_result_descriptions)

        return {
            "success": success_count == num_results,
            "statistics": {
                "evaluated_validations": num_results,
                "success_percent": success_count / num_results * 100,
                "successful_validations": success_count,
                "unsuccessful_validations": num_results - success_count,
            },
            "validation_results": run_result_descriptions,
        }

    @public_api
    def describe(self) -> str:
        """JSON string description of this CheckpointResult"""
        return json.dumps(self.describe_dict(), indent=4)


# Necessary due to cyclic dependencies between Checkpoint and CheckpointResult
CheckpointResult.update_forward_refs()


class CheckpointDescriptionDict(TypedDict):
    success: bool
    statistics: CheckpointDescriptionStatistics
    validation_results: List[Dict[str, Any]]


class CheckpointDescriptionStatistics(TypedDict):
    evaluated_validations: int
    success_percent: float
    successful_validations: int
    unsuccessful_validations: int
