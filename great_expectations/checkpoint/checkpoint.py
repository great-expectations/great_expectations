from __future__ import annotations

import datetime as dt
import json
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    TypedDict,
    Union,
    cast,
)

import great_expectations.exceptions as gx_exceptions
from great_expectations._docs_decorators import public_api
from great_expectations.checkpoint.actions import (
    _VALIDATION_ACTION_REGISTRY,
    ActionContext,
    UpdateDataDocsAction,
    ValidationAction,
)
from great_expectations.compatibility.pydantic import (
    BaseModel,
    Extra,
    Field,
    root_validator,
    validator,
)
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
)
from great_expectations.core.freshness_diagnostics import CheckpointFreshnessDiagnostics
from great_expectations.core.owner_resolution import (
    ResolvedContext,
    owner_from_batch_definition,
    resolve_context,
    unbound_resolution_note,
)
from great_expectations.core.result_format import DEFAULT_RESULT_FORMAT, ResultFormatUnion
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.core.serdes import _IdentifierBundle
from great_expectations.core.suite_parameters import SuiteParameterDict
from great_expectations.core.validation_definition import ValidationDefinition, _ambient_note
from great_expectations.data_context.data_context.context_factory import project_manager
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.exceptions import (
    CheckpointNotAddedError,
    CheckpointNotFreshError,
    CheckpointRunWithoutValidationDefinitionError,
)
from great_expectations.exceptions.exceptions import (
    CheckpointNotFoundError,
    InvalidKeyError,
    StoreBackendError,
)
from great_expectations.exceptions.resource_freshness import ResourceFreshnessAggregateError
from great_expectations.render.renderer.renderer import Renderer

if TYPE_CHECKING:
    from great_expectations.data_context.store.validation_definition_store import (
        ValidationDefinitionStore,
    )


@public_api
class Checkpoint(BaseModel):
    """
    A Checkpoint is the primary means for validating data in a production deployment of Great Expectations.

    Checkpoints provide a convenient abstraction for running a number of validation definitions and triggering a set of actions
    to be taken after the validation step.

    Args:
        name: The name of the checkpoint.
        validation_definitions: List of validation definitions to be run.
        actions: List of actions to be taken after the validation definitions are run.
        result_format: The format in which to return the results of the validation definitions. Default is ResultFormat.SUMMARY.
        id: An optional unique identifier for the checkpoint.

    """  # noqa: E501 # FIXME CoP

    name: str
    validation_definitions: List[ValidationDefinition]
    actions: List[ValidationAction] = Field(default_factory=list)
    result_format: ResultFormatUnion = DEFAULT_RESULT_FORMAT
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
        """  # noqa: E501 # FIXME CoP

        extra = Extra.forbid
        arbitrary_types_allowed = (
            True  # Necessary for compatibility with ValidationAction's Marshmallow dep
        )
        json_encoders = {
            ValidationDefinition: lambda v: v.identifier_bundle(),
            Renderer: lambda r: r.serialize(),
        }

    @validator("actions", pre=True)
    @classmethod
    def validate_actions(
        cls, action_list: list[ValidationAction] | list[dict]
    ) -> list[ValidationAction]:
        validated_actions: list[ValidationAction] = []
        for action in action_list:
            if isinstance(action, ValidationAction):
                validated_actions.append(action)
            else:
                action_type: str | None = action.get("type")
                action_cls = _VALIDATION_ACTION_REGISTRY.get(action_type)
                validated_action = action_cls(**action)
                validated_actions.append(validated_action)

        return validated_actions

    @override
    def json(  # noqa: PLR0913 # FIXME CoP
        self,
        *,
        include: AbstractSet[int | str] | Mapping[int | str, Any] | None = None,
        exclude: AbstractSet[int | str] | Mapping[int | str, Any] | None = None,
        by_alias: bool = False,
        skip_defaults: bool | None = None,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        encoder: Callable[[Any], Any] | None = None,
        models_as_dict: bool = True,
        **dumps_kwargs: Any,
    ) -> str:
        """
        Override the default json method to enable proper diagnostics around validation_definitions.

        NOTE: This should be removed in favor of a field/model serializer when we upgrade to
              Pydantic 2.
        """
        json_data = super().json(
            include=include,
            exclude=self._determine_exclude(exclude=exclude),
            by_alias=by_alias,
            skip_defaults=skip_defaults,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
            encoder=encoder,
            models_as_dict=models_as_dict,
        )

        data = json.loads(json_data)  # Parse back to dict to add validation_definitions
        data_with_validation_definitions = self._serialize_validation_definitions(data)
        return json.dumps(data_with_validation_definitions, **dumps_kwargs)

    @override
    def dict(  # noqa: PLR0913 # FIXME CoP
        self,
        *,
        include: AbstractSet[int | str] | Mapping[int | str, Any] | None = None,
        exclude: AbstractSet[int | str] | Mapping[int | str, Any] | None = None,
        by_alias: bool = False,
        skip_defaults: bool | None = None,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
    ) -> Dict[str, Any]:
        """
        Override the default dict method to enable proper diagnostics around validation_definitions.

        NOTE: This should be removed in favor of a field/model serializer when we upgrade to
              Pydantic 2.
        """
        data = super().dict(
            include=include,
            exclude=self._determine_exclude(exclude=exclude),
            by_alias=by_alias,
            skip_defaults=skip_defaults,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
        )

        return self._serialize_validation_definitions(data=data)

    def _determine_exclude(
        self, exclude: AbstractSet[int | str] | Mapping[int | str, Any] | None
    ) -> AbstractSet[int | str] | Mapping[int | str, Any] | None:
        if not exclude:
            exclude = set()

        if isinstance(exclude, set):
            exclude.add("validation_definitions")
        else:
            exclude["__all__"] = "validation_definitions"  # type: ignore[index] # FIXME

        return exclude

    def _serialize_validation_definitions(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Manually serialize the validation_definitions field to avoid Pydantic's default
        serialization.

        We want to aggregate all errors from the validation_definitions and raise them as
        a single error.
        """
        data["validation_definitions"] = []

        diagnostics = CheckpointFreshnessDiagnostics(errors=[])
        for validation_definition in self.validation_definitions:
            try:
                identifier_bundle = validation_definition.identifier_bundle()
                data["validation_definitions"].append(identifier_bundle.dict())
            except ResourceFreshnessAggregateError as e:
                diagnostics.errors.extend(e.errors)

        diagnostics.raise_for_error()
        return data

    @validator("validation_definitions", pre=True)
    def _validate_validation_definitions(
        cls, validation_definitions: list[ValidationDefinition] | list[Dict[str, Any]]
    ) -> list[ValidationDefinition]:
        if validation_definitions and isinstance(validation_definitions[0], Dict):
            validation_definition_store = project_manager.get_validation_definition_store()
            identifier_bundles = [
                _IdentifierBundle(**v)  # type: ignore[arg-type] # All validation configs are dicts if the first one is
                for v in validation_definitions
            ]
            return cls._deserialize_identifier_bundles_to_validation_definitions(
                identifier_bundles=identifier_bundles,
                store=validation_definition_store,
                ambient=True,
            )

        return cast("List[ValidationDefinition]", validation_definitions)

    @classmethod
    def _deserialize_identifier_bundles_to_validation_definitions(
        cls,
        identifier_bundles: list[_IdentifierBundle],
        store: ValidationDefinitionStore,
        *,
        ambient: bool = False,
    ) -> list[ValidationDefinition]:
        # ``ambient`` is True only when ``store`` was read from the current Data Context; a miss
        # then names that context. A store handed in by a caller keeps the plain message.
        validation_definitions: list[ValidationDefinition] = []
        for id_bundle in identifier_bundles:
            key = store.get_key(name=id_bundle.name, id=id_bundle.id)

            try:
                validation_definition = store.get(key=key)
            except (KeyError, gx_exceptions.InvalidKeyError):
                raise ValueError(  # noqa: TRY003 # FIXME CoP
                    f"Unable to retrieve validation definition {id_bundle} from store"
                    f"{_ambient_note(ends_sentence=False) if ambient else ''}"
                )

            if not validation_definition:
                raise ValueError(  # noqa: TRY003 # FIXME CoP
                    "ValidationDefinitionStore did not retrieve a validation definition"
                )
            validation_definitions.append(validation_definition)

        return validation_definitions

    @public_api
    def run(
        self,
        batch_parameters: Dict[str, Any] | None = None,
        expectation_parameters: SuiteParameterDict | None = None,
        run_id: RunIdentifier | None = None,
    ) -> CheckpointResult:
        """
        Runs the Checkpoint's underlying Validation Definitions and Actions.

        Args:
            batch_parameters: Parameters to be used when loading the Batch.
            expectation_parameters: Parameters to be used when validating the Batch.
            run_id: An optional unique identifier for the run.

        Returns:
            A CheckpointResult object containing the results of the run.

        Raises:
            CheckpointRunWithoutValidationDefinitionError: If the Checkpoint is run without any
                                                           Validation Definitions.
            CheckpointNotAddedError: If the Checkpoint has not been added to the Store.
            CheckpointNotFreshError: If the Checkpoint has been modified since it was last added
                                     to the Store.
        """
        if not self.validation_definitions:
            raise CheckpointRunWithoutValidationDefinitionError()

        diagnostics = self.is_fresh()
        if not diagnostics.success:
            # The checkpoint itself is not added but all children are - we can add it for the user
            if not diagnostics.parent_added and diagnostics.children_added:
                self._add_to_store()
            else:
                diagnostics.raise_for_error()

        if batch_parameters is None:
            batch_parameters = {}
        if expectation_parameters is None:
            expectation_parameters = SuiteParameterDict()
        run_id = run_id or RunIdentifier(run_time=dt.datetime.now(dt.timezone.utc))
        self._prepare_checkpoint_run_for_context(batch_parameters, expectation_parameters)
        run_results = self._run_validation_definitions(
            batch_parameters=batch_parameters,
            expectation_parameters=expectation_parameters,
            result_format=self.result_format,
            run_id=run_id,
        )

        checkpoint_result = self._construct_result(run_id=run_id, run_results=run_results)
        self._run_actions(checkpoint_result=checkpoint_result)

        return checkpoint_result

    def _resolve_context(self) -> ResolvedContext:
        owner = None
        if self.validation_definitions:
            try:
                data = self.validation_definitions[0].data
            except AttributeError:  # e.g. Mock(spec=ValidationDefinition) has no `data`
                data = None
            owner = owner_from_batch_definition(data)
        return resolve_context(owner)

    def _prepare_checkpoint_run_for_context(
        self,
        batch_parameters: Dict[str, Any],
        expectation_parameters: SuiteParameterDict,
    ) -> None:
        context = self._resolve_context().context
        context.prepare_checkpoint_run(self, batch_parameters, expectation_parameters)

    def _run_validation_definitions(
        self,
        batch_parameters: Dict[str, Any],
        expectation_parameters: SuiteParameterDict,
        result_format: ResultFormatUnion,
        run_id: RunIdentifier,
    ) -> Dict[ValidationResultIdentifier, ExpectationSuiteValidationResult]:
        run_results: Dict[ValidationResultIdentifier, ExpectationSuiteValidationResult] = {}
        for validation_definition in self.validation_definitions:
            validation_result = validation_definition.run(
                checkpoint_id=self.id,
                batch_parameters=batch_parameters,
                expectation_parameters=expectation_parameters,
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

    def _construct_result(
        self,
        run_id: RunIdentifier,
        run_results: Dict[ValidationResultIdentifier, ExpectationSuiteValidationResult],
    ) -> CheckpointResult:
        for result in run_results.values():
            result.meta["checkpoint_id"] = self.id

        return CheckpointResult(
            run_id=run_id,
            run_results=run_results,
            checkpoint_config=self,
        )

    def _run_actions(
        self,
        checkpoint_result: CheckpointResult,
    ) -> None:
        action_context = ActionContext()
        sorted_actions = self._sort_actions()
        for action in sorted_actions:
            # An action runs against the Data Context this Checkpoint resolves through.
            action._data_context = self._resolve_context().context
            action_result = action.run(
                checkpoint_result=checkpoint_result,
                action_context=action_context,
            )
            action_context.update(action=action, action_result=action_result)

    def _sort_actions(self) -> List[ValidationAction]:
        """
        UpdateDataDocsActions are prioritized to run first, followed by all other actions.

        This is due to the fact that certain actions reference data docs sites,
        which must be updated first.
        """
        priority_actions: List[ValidationAction] = []
        secondary_actions: List[ValidationAction] = []
        for action in self.actions:
            if isinstance(action, UpdateDataDocsAction):
                priority_actions.append(action)
            else:
                secondary_actions.append(action)

        return priority_actions + secondary_actions

    def is_fresh(self) -> CheckpointFreshnessDiagnostics:
        checkpoint_diagnostics = CheckpointFreshnessDiagnostics(
            errors=[] if self.id else [CheckpointNotAddedError(name=self.name)]
        )
        validation_definition_diagnostics = [vd.is_fresh() for vd in self.validation_definitions]
        checkpoint_diagnostics.update_with_children(*validation_definition_diagnostics)

        if not checkpoint_diagnostics.success:
            return checkpoint_diagnostics

        resolved = self._resolve_context()
        store = resolved.context.checkpoint_store
        key = store.get_key(name=self.name, id=self.id)

        try:
            checkpoint = store.get(key=key)
        except (
            StoreBackendError,  # Generic error from stores
            InvalidKeyError,  # Ephemeral context error
        ):
            note = None if resolved.bound else unbound_resolution_note(resolved.context)
            return CheckpointFreshnessDiagnostics(
                errors=[CheckpointNotFoundError(name=self.name, note=note)]
            )

        return CheckpointFreshnessDiagnostics(
            errors=[] if checkpoint == self else [CheckpointNotFreshError(name=self.name)]
        )

    @public_api
    def save(self) -> None:
        """Save the current state of this Checkpoint."""
        store = self._resolve_context().context.checkpoint_store
        key = store.get_key(name=self.name, id=self.id)

        store.update(key=key, value=self)

    def _add_to_store(self) -> None:
        """This is used to persist a checkpoint before we run it.

        We need to persist a checkpoint before it can be run. If user calls runs but hasn't
        persisted it we add it for them.
        """
        store = self._resolve_context().context.checkpoint_store
        key = store.get_key(name=self.name, id=self.id)

        store.add(key=key, value=self)


@public_api
class CheckpointResult(BaseModel):
    """
    The result of running a Checkpoint.

    Contains information about Expectation successes and failures from running
    each Validation Definition in the Checkpoint.
    """

    run_id: RunIdentifier
    run_results: Dict[ValidationResultIdentifier, ExpectationSuiteValidationResult]
    checkpoint_config: Checkpoint
    success: Optional[bool] = None

    class Config:
        extra = Extra.forbid
        arbitrary_types_allowed = True

    @root_validator
    def _root_validate_result(cls, values: dict) -> dict:
        run_results = values["run_results"]
        if len(run_results) == 0:
            raise ValueError("CheckpointResult must contain at least one run result")  # noqa: TRY003 # FIXME CoP

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
