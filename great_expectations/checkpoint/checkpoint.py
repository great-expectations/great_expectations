from __future__ import annotations

import datetime
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Literal,
    Optional,
    Sequence,
    Union,
)

import great_expectations.exceptions as gx_exceptions
from great_expectations._docs_decorators import (
    public_api,
)
from great_expectations.checkpoint.configurator import (
    ActionDicts,
)
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.checkpoint.util import (
    convert_validations_list_to_checkpoint_validation_definitions,
    does_batch_request_in_validations_contain_batch_data,
    get_substituted_validation_dict,
    get_validations_with_batch_request_as_dict,
    substitute_runtime_config,
    validate_validation_dict,
)
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core import RunIdentifier
from great_expectations.core.batch import (
    BatchRequest,
    BatchRequestBase,
    RuntimeBatchRequest,
    batch_request_contains_batch_data,
    get_batch_request_as_dict,
)
from great_expectations.core.config_peer import ConfigOutputModes, ConfigPeer
from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.types.base import (
    CheckpointConfig,
    CheckpointValidationDefinition,
)
from great_expectations.data_context.types.resource_identifiers import GXCloudIdentifier
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.util import (
    deep_filter_properties_iterable,
    filter_properties_dict,
    load_class,
)
from great_expectations.validation_operators import ActionListValidationOperator

if TYPE_CHECKING:
    from great_expectations.checkpoint.configurator import ActionDict
    from great_expectations.core.config_provider import _ConfigurationProvider
    from great_expectations.data_context import AbstractDataContext
    from great_expectations.datasource.fluent.interfaces import (
        BatchRequest as FluentBatchRequest,
    )
    from great_expectations.validation_operators.types.validation_operator_result import (
        ValidationOperatorResult,
    )
    from great_expectations.validator.validator import Validator


logger = logging.getLogger(__name__)


def _does_validation_contain_batch_request(
    validations: list[CheckpointValidationDefinition],
) -> bool:
    return any(val.batch_request is not None for val in validations)


def _does_validation_contain_expectation_suite_name(
    validations: list[CheckpointValidationDefinition],
) -> bool:
    return any(val.expectation_suite_name is not None for val in validations)


class BaseCheckpoint(ConfigPeer):
    """
    BaseCheckpoint class is initialized from CheckpointConfig typed object and contains all functionality
    in the form of interface methods (which can be overwritten by subclasses) and their reference implementation.

    While not technically categorized as abstract class, "BaseCheckpoint" serves as parent class; it must never be
    instantiated directly (only its descendants, such as "Checkpoint", should be instantiated).
    """  # noqa: E501

    DEFAULT_ACTION_LIST: ClassVar[Sequence[ActionDict]] = ActionDicts.DEFAULT_ACTION_LIST

    def __init__(
        self,
        checkpoint_config: CheckpointConfig,
        data_context: AbstractDataContext | None = None,
    ) -> None:
        self._data_context = data_context
        self._checkpoint_config = checkpoint_config
        self._validator: Validator | None = None

    @public_api
    def run(  # noqa: C901, PLR0913, PLR0912, PLR0915
        self,
        expectation_suite_name: str | None = None,
        batch_request: BatchRequestBase | FluentBatchRequest | dict | None = None,
        validator: Validator | None = None,
        action_list: Sequence[ActionDict] | None = None,
        suite_parameters: dict | None = None,
        runtime_configuration: dict | None = None,
        validations: list[dict] | list[CheckpointValidationDefinition] | None = None,
        run_id: str | RunIdentifier | None = None,
        run_name: str | None = None,
        run_time: datetime.datetime | None = None,
        result_format: str | dict | None = None,
        expectation_suite_id: str | None = None,
    ) -> CheckpointResult:
        """Validate against current Checkpoint.

        Arguments allow for override of the current Checkpoint configuration.

        Args:
            expectation_suite_name: Expectation suite associated with checkpoint.
            batch_request: Batch request describing the batch of data to validate.
            validator: Validator objects, loaded with Batch data samples, can be supplied (in lieu of  "batch_request")
            action_list: A list of actions to perform after each batch is validated.
            suite_parameters: Suite parameters to use in generating this checkpoint.
            runtime_configuration: Runtime configuration to pass into the validator's runtime configuration
                (e.g. `result_format`).
            validations: Validations to be executed as part of checkpoint.
            run_id: The run_id for the validation; if None, a default value will be used.
            run_name: The run_name for the validation; if None, a default value will be used.
            run_time: The date/time of the run.
            result_format: One of several supported formatting directives for expectation validation results
            expectation_suite_id: Great Expectations Cloud id for the expectation suite

        Raises:
            InvalidCheckpointConfigError: If `run_id` is provided with `run_name` or `run_time`.
            InvalidCheckpointConfigError: If `result_format` is not an expected type.
            CheckpointError: If Checkpoint does not contain a `batch_request` or validations.

        Returns:
            CheckpointResult
        """  # noqa: E501
        context = self.data_context
        if context is None:
            raise ValueError(  # noqa: TRY003
                "Must associate Checkpoint with a DataContext before running; please add using context.checkpoints.add"  # noqa: E501
            )

        validations = convert_validations_list_to_checkpoint_validation_definitions(validations)

        if sum(bool(x) for x in [self._validator is not None, validator is not None]) > 1:
            raise gx_exceptions.CheckpointError(  # noqa: TRY003
                f'Checkpoint "{self.name}" has already been created with a validator and overriding it through run() is not allowed.'  # noqa: E501
            )

        if validator:
            self._validator = validator

        if self._validator:
            if batch_request or _does_validation_contain_batch_request(validations=validations):
                raise gx_exceptions.CheckpointError(  # noqa: TRY003
                    f'Checkpoint "{self.name}" has already been created with a validator and overriding it by supplying a batch_request and/or validations with a batch_request to run() is not allowed.'  # noqa: E501
                )

            if expectation_suite_name or _does_validation_contain_expectation_suite_name(
                validations=validations
            ):
                raise gx_exceptions.CheckpointError(  # noqa: TRY003
                    f'Checkpoint "{self.name}" has already been created with a validator and overriding its expectation_suite_name by supplying an expectation_suite_name and/or validations with an expectation_suite_name to run() is not allowed.'  # noqa: E501
                )

        if (run_id and run_name) or (run_id and run_time):
            raise gx_exceptions.InvalidCheckpointConfigError(  # noqa: TRY003
                "Please provide either a run_id or run_name and/or run_time"
            )

        # If no validations are provided, the combination of expectation_suite_name, batch_request,
        # and action_list are considered the "default" validation.
        using_default_validation = not self.validations and not validations

        run_time = run_time or datetime.datetime.now(tz=datetime.timezone.utc)
        runtime_configuration = runtime_configuration or {}
        result_format = result_format or runtime_configuration.get("result_format")

        _result_format_types = (type(None), str, dict)
        if not isinstance(result_format, _result_format_types):
            raise gx_exceptions.InvalidCheckpointConfigError(  # noqa: TRY003
                f"result_format should be of type - {' '.join(str(t) for t in _result_format_types)}"  # noqa: E501
            )

        batch_request = get_batch_request_as_dict(batch_request=batch_request)
        validations = get_validations_with_batch_request_as_dict(validations=validations)

        runtime_kwargs: dict = {
            "expectation_suite_name": expectation_suite_name,
            "batch_request": batch_request or {},
            "action_list": action_list or [],
            "suite_parameters": suite_parameters or {},
            "runtime_configuration": runtime_configuration or {},
            "validations": validations or [],
            "expectation_suite_id": expectation_suite_id,
        }

        substituted_runtime_config: dict = self._get_substituted_config(
            config_provider=context.config_provider, runtime_kwargs=runtime_kwargs
        )

        batch_request = substituted_runtime_config.get("batch_request")
        validations = convert_validations_list_to_checkpoint_validation_definitions(
            substituted_runtime_config.get("validations") or []
        )

        run_id = run_id or RunIdentifier(run_name=run_name, run_time=run_time)

        if len(validations) == 0 and not (batch_request or self._validator):
            raise gx_exceptions.CheckpointError(  # noqa: TRY003
                f'Checkpoint "{self.name}" must be called with a validator or contain either a batch_request or validations.'  # noqa: E501
            )

        # Ensure that validations dicts have the most specific id available
        # (default to Checkpoint's default_validation_id if no validations were passed in the signature)  # noqa: E501
        if using_default_validation:
            for validation in validations:
                validation.id = self.config.default_validation_id

        validation_operator_results: list[ValidationOperatorResult] = []
        if len(validations) > 0:
            for idx, validation_dict in enumerate(validations):
                result = self._run_validation(
                    substituted_runtime_config=substituted_runtime_config,
                    result_format=result_format,
                    run_id=run_id,
                    idx=idx,
                    validation_dict=validation_dict,
                    context=context,
                )
                validation_operator_results.append(result)
        else:
            result = self._run_validation(
                substituted_runtime_config=substituted_runtime_config,
                result_format=result_format,
                run_id=run_id,
                context=context,
            )
            validation_operator_results.append(result)

        checkpoint_run_results: dict = {}
        for validation_operator_result in validation_operator_results:
            run_results = validation_operator_result.run_results

            validation_result_url: str | None = None
            for run_result in run_results.values():
                validation_result = run_result.get("validation_result")
                if validation_result:
                    meta = validation_result["meta"]
                    id = self.id
                    meta["checkpoint_id"] = id
                # TODO: We only currently support 1 validation_result_url per checkpoint and use the first one we  # noqa: E501
                #       encounter. Since checkpoints can have more than 1 validation result, we will need to update  # noqa: E501
                #       this and the consumers.
                if not validation_result_url:
                    if (
                        "actions_results" in run_result
                        and "store_validation_result" in run_result["actions_results"]
                        and "validation_result_url"
                        in run_result["actions_results"]["store_validation_result"]
                    ):
                        validation_result_url = run_result["actions_results"][
                            "store_validation_result"
                        ]["validation_result_url"]

            checkpoint_run_results.update(run_results)

        return CheckpointResult(
            validation_result_url=validation_result_url,
            run_id=run_id,  # type: ignore[arg-type] # could be str
            run_results=checkpoint_run_results,
            checkpoint_config=self.config,
        )

    def _get_substituted_config(
        self,
        config_provider: _ConfigurationProvider,
        runtime_kwargs: dict | None = None,
    ) -> dict:
        if runtime_kwargs is None:
            runtime_kwargs = {}

        config_kwargs: dict = self.get_config(mode=ConfigOutputModes.JSON_DICT)  # type: ignore[assignment] # always returns a dict

        substituted_config: dict = substitute_runtime_config(
            source_config=config_kwargs, runtime_kwargs=runtime_kwargs
        )

        if self._using_cloud_context:
            return substituted_config

        return config_provider.substitute_config(substituted_config)

    def _run_validation(  # noqa: PLR0913
        self,
        substituted_runtime_config: dict,
        result_format: dict | str | None,
        run_id: str | RunIdentifier | None,
        context: AbstractDataContext,
        idx: int | None = 0,
        validation_dict: CheckpointValidationDefinition | None = None,
    ) -> ValidationOperatorResult:
        if validation_dict is None:
            validation_dict = CheckpointValidationDefinition(
                id=substituted_runtime_config.get("default_validation_id")
            )

        try:
            substituted_validation_dict: dict = get_substituted_validation_dict(
                substituted_runtime_config=substituted_runtime_config,
                validation_dict=validation_dict,
            )
            validate_validation_dict(
                validation_dict=substituted_validation_dict,
                batch_request_required=(not self._validator),
            )

            batch_request: Union[BatchRequest, FluentBatchRequest, RuntimeBatchRequest, None] = (
                substituted_validation_dict.get("batch_request")
            )
            expectation_suite_name: str | None = substituted_validation_dict.get(
                "expectation_suite_name"
            )
            expectation_suite_id: str | None = substituted_validation_dict.get(
                "expectation_suite_id"
            )
            include_rendered_content: bool | None = substituted_validation_dict.get(
                "include_rendered_content"
            )
            if include_rendered_content is None:
                include_rendered_content = (
                    context._determine_if_expectation_validation_result_include_rendered_content()
                )

            validator: Validator = self._validator or context.get_validator(
                batch_request=batch_request,
                expectation_suite_name=expectation_suite_name
                if not self._using_cloud_context
                else None,
                expectation_suite_id=(expectation_suite_id if self._using_cloud_context else None),
                include_rendered_content=include_rendered_content,
            )

            action_list: Sequence[ActionDict] | None = substituted_validation_dict.get(
                "action_list"
            )
            runtime_configuration_validation = substituted_validation_dict.get(
                "runtime_configuration", {}
            )
            catch_exceptions_validation = runtime_configuration_validation.get("catch_exceptions")
            result_format_validation = runtime_configuration_validation.get("result_format")
            result_format = result_format or result_format_validation

            if result_format is None:
                result_format = {"result_format": "SUMMARY"}

            action_list_validation_operator: ActionListValidationOperator = (
                ActionListValidationOperator(
                    data_context=context,
                    action_list=action_list,
                    result_format=result_format,
                    name=f"{self.name}-checkpoint-validation[{idx}]",
                )
            )
            checkpoint_identifier = None
            if self._using_cloud_context:
                checkpoint_identifier = GXCloudIdentifier(
                    resource_type=GXCloudRESTResource.CHECKPOINT,
                    id=self.id,
                )

            operator_run_kwargs = {}

            if catch_exceptions_validation is not None:
                operator_run_kwargs["catch_exceptions"] = catch_exceptions_validation

            validation_id: str | None = substituted_validation_dict.get("id")

            return action_list_validation_operator.run(
                assets_to_validate=[validator],
                run_id=run_id,
                suite_parameters=substituted_validation_dict.get("suite_parameters"),
                result_format=result_format,
                checkpoint_identifier=checkpoint_identifier,
                checkpoint_name=self.name,
                validation_id=validation_id,
                **operator_run_kwargs,
            )
        except (
            gx_exceptions.CheckpointError,
            gx_exceptions.ExecutionEngineError,
            gx_exceptions.MetricError,
        ) as e:
            raise gx_exceptions.CheckpointError(  # noqa: TRY003
                f"Exception occurred while running validation[{idx}] of Checkpoint '{self.name}': {e.message}."  # noqa: E501
            ) from e

    @property
    @override
    def config(self) -> CheckpointConfig:
        return self._checkpoint_config

    @property
    def name(self) -> str | None:
        return self.config.name

    @property
    def action_list(self) -> Sequence[ActionDict]:
        try:
            return self.config.action_list
        except AttributeError:
            return []

    @property
    def validations(
        self,
    ) -> list[CheckpointValidationDefinition]:
        try:
            return self.config.validations
        except AttributeError:
            return []

    @property
    def id(self) -> str | None:
        try:
            return self.config.id
        except AttributeError:
            return None

    @property
    def data_context(self) -> AbstractDataContext | None:
        return self._data_context

    @property
    def _using_cloud_context(self) -> bool:
        # Chetan - 20221216 - This is a temporary property to encapsulate any Cloud leakage
        # Upon refactoring this class to decouple Cloud-specific branches, this should be removed
        from great_expectations.data_context.data_context.cloud_data_context import (
            CloudDataContext,
        )

        return isinstance(self.data_context, CloudDataContext)

    @override
    def __repr__(self) -> str:
        return str(self.get_config())


@public_api
class Checkpoint(BaseCheckpoint):
    """A checkpoint is the primary means for validating data in a production deployment of Great Expectations.

    Checkpoints provide a convenient abstraction for bundling the Validation of a Batch (or Batches) of data against
    an Expectation Suite (or several), as well as the Actions that should be taken after the validation.

    A Checkpoint uses a Validator to run one or more Expectation Suites against one or more Batches provided by one
    or more Batch Requests. Running a Checkpoint produces Validation Results and will result in optional Actions
    being performed if they are configured to do so.

    Args:
        name: User-selected checkpoint name (e.g. "staging_tables").
        data_context: Data context that is associated with the current checkpoint.
        expectation_suite_name: Expectation suite associated with checkpoint.
        batch_request: Batch request describing the batch of data to validate.
        action_list: A list of actions to perform after each batch is validated.
        suite_parameters: Suite parameters to use in generating this checkpoint.
        runtime_configuration: Runtime configuration to pass into the validator's runtime configuration
            (e.g. `result_format`).
        validations: Validations to be executed as part of checkpoint.
        id: Great Expectations Cloud id for this Checkpoint.
        expectation_suite_id: Great Expectations Cloud id associated with Expectation Suite.
        default_validation_id:  Default value used by Checkpoint if no Validations are configured.

    Raises:
        ValueError: If BatchRequest contains batch_data, since only primitive types are allowed in the constructor.
        ValueError: If Validations contains batch_data, since only primitive types are allowed in the constructor.
    """  # noqa: E501

    """
    --ge-feature-maturity-info--

        id: checkpoint
        title: Newstyle Class-based Checkpoints
        short_description: Run a configured checkpoint from a notebook.
        description: Run a configured checkpoint from a notebook.
        how_to_guide_url: https://docs.greatexpectations.io/en/latest/guides/how_to_guides/validation/how_to_create_a_new_checkpoint.html
        maturity: Beta
        maturity_details:
            api_stability: Mostly stable (transitioning ValidationOperators to Checkpoints)
            implementation_completeness: Complete
            unit_test_coverage: Partial ("golden path"-focused tests; error checking tests need to be improved)
            integration_infrastructure_test_coverage: N/A
            documentation_completeness: Complete
            bug_risk: Medium

    --ge-feature-maturity-info--
    """  # noqa: E501

    def __init__(  # noqa: PLR0913
        self,
        name: str,
        data_context: AbstractDataContext | None = None,
        expectation_suite_name: str | None = None,
        batch_request: BatchRequestBase | FluentBatchRequest | dict | None = None,
        validator: Validator | None = None,
        action_list: Sequence[ActionDict] | None = None,
        suite_parameters: dict | None = None,
        runtime_configuration: dict | None = None,
        validations: list[dict] | list[CheckpointValidationDefinition] | None = None,
        id: str | None = None,
        expectation_suite_id: str | None = None,
        default_validation_id: str | None = None,
    ) -> None:
        validations = convert_validations_list_to_checkpoint_validation_definitions(validations)

        if validator:
            if batch_request or _does_validation_contain_batch_request(validations=validations):
                raise gx_exceptions.CheckpointError(  # noqa: TRY003
                    f'Checkpoint "{name}" cannot be called with a validator and contain a batch_request and/or a batch_request in validations.'  # noqa: E501
                )

            if expectation_suite_name or _does_validation_contain_expectation_suite_name(
                validations=validations
            ):
                raise gx_exceptions.CheckpointError(  # noqa: TRY003
                    f'Checkpoint "{name}" cannot be called with a validator and contain an expectation_suite_name and/or an expectation_suite_name in validations.'  # noqa: E501
                )

            expectation_suite_name = validator.expectation_suite_name

        # Only primitive types are allowed as constructor arguments; data frames are supplied to "run()" as arguments.  # noqa: E501
        if batch_request_contains_batch_data(batch_request=batch_request):
            raise ValueError(  # noqa: TRY003
                """Error: batch_data found in batch_request -- only primitive types are allowed as Checkpoint \
constructor arguments.
"""  # noqa: E501
            )

        if does_batch_request_in_validations_contain_batch_data(validations=validations):
            raise ValueError(  # noqa: TRY003
                """Error: batch_data found in batch_request -- only primitive types are allowed as Checkpoint \
constructor arguments.
"""  # noqa: E501
            )

        checkpoint_config = CheckpointConfig(
            name=name,
            expectation_suite_name=expectation_suite_name,
            batch_request=batch_request,  # type: ignore[arg-type] # FluentBatchRequest is not a dict
            # TODO: check if `pydantic.BaseModel` and call `batch_request.dict()`??
            action_list=action_list,
            suite_parameters=suite_parameters,
            runtime_configuration=runtime_configuration,
            validations=validations,
            id=id,
            expectation_suite_id=expectation_suite_id,
            default_validation_id=default_validation_id,
        )
        super().__init__(
            checkpoint_config=checkpoint_config,
            data_context=data_context,
        )

        self._validator = validator

    @classmethod
    def construct_from_config_args(  # noqa: PLR0913
        cls,
        data_context: AbstractDataContext,
        checkpoint_store_name: str,
        name: str,
        module_name: str = "great_expectations.checkpoint",
        class_name: Literal["Checkpoint"] = "Checkpoint",
        expectation_suite_name: Optional[str] = None,
        batch_request: Optional[dict] = None,
        action_list: Optional[Sequence[ActionDict]] = None,
        suite_parameters: Optional[dict] = None,
        runtime_configuration: Optional[dict] = None,
        validations: Optional[Union[list[dict], list[CheckpointValidationDefinition]]] = None,
        id: Optional[str] = None,
        expectation_suite_id: Optional[str] = None,
        default_validation_id: Optional[str] = None,
        validator: Validator | None = None,
    ) -> Checkpoint:
        checkpoint_config: Union[CheckpointConfig, dict]

        validations = cls._reconcile_validations(validations=validations, validator=validator)

        # These checks protect against typed objects (BatchRequest and/or RuntimeBatchRequest) encountered in arguments.  # noqa: E501
        batch_request = get_batch_request_as_dict(batch_request=batch_request)
        validations = get_validations_with_batch_request_as_dict(validations=validations)

        # DataFrames shouldn't be saved to CheckpointStore
        if batch_request_contains_batch_data(batch_request=batch_request):
            raise gx_exceptions.InvalidConfigError(  # noqa: TRY003
                f'batch_data found in batch_request cannot be saved to CheckpointStore "{checkpoint_store_name}"'  # noqa: E501
            )

        if does_batch_request_in_validations_contain_batch_data(validations=validations):
            raise gx_exceptions.InvalidConfigError(  # noqa: TRY003
                f'batch_data found in validations cannot be saved to CheckpointStore "{checkpoint_store_name}"'  # noqa: E501
            )

        checkpoint_config = {
            "name": name,
            "module_name": module_name,
            "class_name": class_name,
            "expectation_suite_name": expectation_suite_name,
            "batch_request": batch_request,
            "action_list": action_list,
            "suite_parameters": suite_parameters,
            "runtime_configuration": runtime_configuration,
            "validations": validations,
            "id": id,
            "expectation_suite_id": expectation_suite_id,
            "default_validation_id": default_validation_id,
        }

        default_checkpoints_module_name = "great_expectations.checkpoint"

        klass = load_class(
            class_name=class_name,
            module_name=module_name or default_checkpoints_module_name,
        )
        if not issubclass(klass, Checkpoint):
            raise gx_exceptions.InvalidCheckpointConfigError(  # noqa: TRY003
                f'Custom class "{klass.__name__}" must extend "Checkpoint" (exclusively).'
            )

        checkpoint_config = deep_filter_properties_iterable(
            properties=checkpoint_config,
            clean_falsy=True,
        )

        new_checkpoint: Checkpoint = instantiate_class_from_config(
            config=checkpoint_config,
            runtime_environment={
                "data_context": data_context,
            },
            config_defaults={
                "module_name": default_checkpoints_module_name,
            },
        )

        return new_checkpoint

    @staticmethod
    def _reconcile_validations(
        validations: list[dict] | list[CheckpointValidationDefinition] | None = None,
        validator: Validator | None = None,
    ) -> list[CheckpointValidationDefinition]:
        """
        Helper to help resolve logic between validator and validations input
        arguments to `construct_from_config_args`.
        """
        if validator:
            if validations:
                raise ValueError(  # noqa: TRY003
                    "Please provide either a validator or validations list (but not both)."
                )
            return validator.convert_to_checkpoint_validations_list()

        return convert_validations_list_to_checkpoint_validation_definitions(validations)

    @staticmethod
    def instantiate_from_config_with_runtime_args(
        checkpoint_config: CheckpointConfig,
        data_context: AbstractDataContext,
        **runtime_kwargs,
    ) -> Checkpoint:
        config: dict = checkpoint_config.to_json_dict()

        key: str
        value: Any
        for key, value in runtime_kwargs.items():
            if value is not None:
                config[key] = value

        config = filter_properties_dict(  # type: ignore[assignment] # filter could return None
            properties=config,
            clean_falsy=True,
        )

        return Checkpoint(**config, data_context=data_context)
