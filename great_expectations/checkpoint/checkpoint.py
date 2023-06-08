from __future__ import annotations

import copy
import datetime
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    List,
    Literal,
    Optional,
    Sequence,
    Type,
    Union,
    cast,
)

import great_expectations.exceptions as gx_exceptions
from great_expectations.checkpoint.configurator import (
    ActionDicts,
    SimpleCheckpointConfigurator,
)
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.checkpoint.util import (
    does_batch_request_in_validations_contain_batch_data,
    get_substituted_validation_dict,
    get_validations_with_batch_request_as_dict,
    substitute_runtime_config,
    substitute_template_config,
    validate_validation_dict,
)
from great_expectations.core import RunIdentifier
from great_expectations.core._docs_decorators import (
    deprecated_argument,
    new_argument,
    public_api,
)
from great_expectations.core.async_executor import AsyncExecutor, AsyncResult
from great_expectations.core.batch import (
    BatchRequest,
    BatchRequestBase,
    RuntimeBatchRequest,
    batch_request_contains_batch_data,
    get_batch_request_as_dict,
)
from great_expectations.core.config_peer import ConfigOutputModes, ConfigPeer
from great_expectations.core.usage_statistics.events import UsageStatsEvents
from great_expectations.core.usage_statistics.usage_statistics import (
    get_checkpoint_run_usage_statistics,
    usage_statistics_enabled_method,
)
from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.types.base import (
    CheckpointConfig,
    CheckpointValidationConfig,
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
    from great_expectations.core.expectation_validation_result import (
        ExpectationSuiteValidationResult,
        ExpectationSuiteValidationResultMeta,
    )
    from great_expectations.data_context import AbstractDataContext
    from great_expectations.datasource.fluent.interfaces import (
        BatchRequest as FluentBatchRequest,
    )
    from great_expectations.validation_operators.types.validation_operator_result import (
        ValidationOperatorResult,
    )
    from great_expectations.validator.validator import Validator


logger = logging.getLogger(__name__)


def _get_validator_class() -> Type[Validator]:
    """Using this function helps work around circular import dependncies."""
    module_name = "great_expectations.validator.validator"
    class_name = "Validator"
    return load_class(class_name=class_name, module_name=module_name)


def _does_validation_contain_batch_request(
    validations: Optional[List[dict]] = None,
) -> bool:
    if validations is not None:
        for val in validations:
            if val.get("batch_request") is not None:
                return True

    return False


def _does_validation_contain_expectation_suite_name(
    validations: Optional[List[dict]] = None,
) -> bool:
    if validations is not None:
        for val in validations:
            if val.get("expectation_suite_name") is not None:
                return True

    return False


class BaseCheckpoint(ConfigPeer):
    """
    BaseCheckpoint class is initialized from CheckpointConfig typed object and contains all functionality
    in the form of interface methods (which can be overwritten by subclasses) and their reference implementation.

    While not technically categorized as abstract class, "BaseCheckpoint" serves as parent class; it must never be
    instantiated directly (only its descendants, such as "Checkpoint" and "SimpleCheckpoint", should be instantiated).
    """

    DEFAULT_ACTION_LIST: ClassVar[
        Sequence[ActionDict]
    ] = ActionDicts.DEFAULT_ACTION_LIST

    def __init__(
        self,
        checkpoint_config: CheckpointConfig,
        data_context: AbstractDataContext,
    ) -> None:
        self._usage_statistics_handler = data_context._usage_statistics_handler

        self._data_context = data_context

        self._checkpoint_config = checkpoint_config

        self._validator: Optional[Validator] = None

    # TODO: Add eval param processing using new TBD parser syntax and updated EvaluationParameterParser and
    #  parse_evaluation_parameters function (e.g. datetime substitution or specifying relative datetimes like "most
    #  recent"). Currently, environment variable substitution is the only processing applied to evaluation parameters,
    #  while run_name_template also undergoes strftime datetime substitution
    @public_api
    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.CHECKPOINT_RUN,
        args_payload_fn=get_checkpoint_run_usage_statistics,
    )
    @new_argument(
        argument_name="expectation_suite_ge_cloud_id",
        version="0.13.33",
        message="Used in cloud deployments.",
    )
    def run(  # noqa: C901, PLR0913, PLR0915
        self,
        template_name: Optional[str] = None,
        run_name_template: Optional[str] = None,
        expectation_suite_name: Optional[str] = None,
        batch_request: Optional[
            Union[BatchRequestBase, FluentBatchRequest, dict]
        ] = None,
        validator: Optional[Validator] = None,
        action_list: Optional[Sequence[ActionDict]] = None,
        evaluation_parameters: Optional[dict] = None,
        runtime_configuration: Optional[dict] = None,
        validations: Optional[List[dict]] = None,
        profilers: Optional[List[dict]] = None,
        run_id: Optional[Union[str, RunIdentifier]] = None,
        run_name: Optional[str] = None,
        run_time: Optional[datetime.datetime] = None,
        result_format: Optional[Union[str, dict]] = None,
        expectation_suite_ge_cloud_id: Optional[str] = None,
    ) -> CheckpointResult:
        """Validate against current Checkpoint.

        Arguments allow for override of the current Checkpoint configuration.

        Args:
            template_name: The name of another checkpoint to use as a base template.
            run_name_template: A template to create run names, using environment
                variables and datetime-template syntax (e.g. "%Y-%M-staging-$MY_ENV_VAR").
            expectation_suite_name: Expectation suite associated with checkpoint.
            batch_request: Batch request describing the batch of data to validate.
            validator: Validator objects, loaded with Batch data samples, can be supplied (in lieu of  "batch_request")
            action_list: A list of actions to perform after each batch is validated.
            evaluation_parameters: Evaluation parameters to use in generating this checkpoint.
            runtime_configuration: Runtime configuration to pass into the validator's runtime configuration
                (e.g. `result_format`).
            validations: Validations to be executed as part of checkpoint.
            profilers: Profilers to use in generating this checkpoint.
            run_id: The run_id for the validation; if None, a default value will be used.
            run_name: The run_name for the validation; if None, a default value will be used.
            run_time: The date/time of the run.
            result_format: One of several supported formatting directives for expectation validation results
            expectation_suite_ge_cloud_id: Great Expectations Cloud id for the expectation suite

        Raises:
            InvalidCheckpointConfigError: If `run_id` is provided with `run_name` or `run_time`.
            InvalidCheckpointConfigError: If `result_format` is not an expected type.
            CheckpointError: If Checkpoint does not contain a `batch_request` or validations.

        Returns:
            CheckpointResult
        """
        if (
            sum(bool(x) for x in [self._validator is not None, validator is not None])
            > 1
        ):
            raise gx_exceptions.CheckpointError(
                f'Checkpoint "{self.name}" has already been created with a validator and overriding it through run() is not allowed.'
            )

        if validator:
            self._validator = validator

        if self._validator:
            if batch_request or _does_validation_contain_batch_request(
                validations=validations
            ):
                raise gx_exceptions.CheckpointError(
                    f'Checkpoint "{self.name}" has already been created with a validator and overriding it by supplying a batch_request and/or validations with a batch_request to run() is not allowed.'
                )

            if (
                expectation_suite_name
                or _does_validation_contain_expectation_suite_name(
                    validations=validations
                )
            ):
                raise gx_exceptions.CheckpointError(
                    f'Checkpoint "{self.name}" has already been created with a validator and overriding its expectation_suite_name by supplying an expectation_suite_name and/or validations with an expectation_suite_name to run() is not allowed.'
                )

        if (run_id and run_name) or (run_id and run_time):
            raise gx_exceptions.InvalidCheckpointConfigError(
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
            raise gx_exceptions.InvalidCheckpointConfigError(
                f"result_format should be of type - {' '.join(str(t) for t in _result_format_types)}"
            )

        batch_request = get_batch_request_as_dict(batch_request=batch_request)
        validations = get_validations_with_batch_request_as_dict(
            validations=validations
        )

        runtime_kwargs: dict = {
            "template_name": template_name,
            "run_name_template": run_name_template,
            "expectation_suite_name": expectation_suite_name,
            "batch_request": batch_request or {},
            "action_list": action_list or [],
            "evaluation_parameters": evaluation_parameters or {},
            "runtime_configuration": runtime_configuration or {},
            "validations": validations or [],
            "profilers": profilers or [],
            "expectation_suite_ge_cloud_id": expectation_suite_ge_cloud_id,
        }

        substituted_runtime_config: dict = self.get_substituted_config(
            runtime_kwargs=runtime_kwargs
        )

        run_name_template = substituted_runtime_config.get("run_name_template")

        batch_request = substituted_runtime_config.get("batch_request")
        validations = cast(list, substituted_runtime_config.get("validations") or [])

        if run_name is None and run_name_template is not None:
            run_name = run_time.strftime(run_name_template)

        run_id = run_id or RunIdentifier(run_name=run_name, run_time=run_time)

        if len(validations) == 0 and not (batch_request or self._validator):
            raise gx_exceptions.CheckpointError(
                f'Checkpoint "{self.name}" must be called with a validator or contain either a batch_request or validations.'
            )

        # Ensure that validations dicts have the most specific id available
        # (default to Checkpoint's default_validation_id if no validations were passed in the signature)
        if using_default_validation:
            for validation in validations:
                validation["id"] = self.config.default_validation_id

        # Use AsyncExecutor to speed up I/O bound validations by running them in parallel with multithreading (if
        # concurrency is enabled in the data context configuration) -- please see the below arguments used to initialize
        # AsyncExecutor and the corresponding AsyncExecutor docstring for more details on when multiple threads are
        # used.
        with AsyncExecutor(
            self.data_context.concurrency, max_workers=len(validations)
        ) as async_executor:
            # noinspection PyUnresolvedReferences
            async_validation_operator_results: List[
                AsyncResult[ValidationOperatorResult]
            ] = []
            if len(validations) > 0:
                for idx, validation_dict in enumerate(validations):
                    self._run_validation(
                        substituted_runtime_config=substituted_runtime_config,
                        async_validation_operator_results=async_validation_operator_results,
                        async_executor=async_executor,
                        result_format=result_format,
                        run_id=run_id,
                        idx=idx,
                        validation_dict=validation_dict,
                    )
            else:
                self._run_validation(
                    substituted_runtime_config=substituted_runtime_config,
                    async_validation_operator_results=async_validation_operator_results,
                    async_executor=async_executor,
                    result_format=result_format,
                    run_id=run_id,
                )

            checkpoint_run_results: dict = {}
            async_validation_operator_result: AsyncResult
            for async_validation_operator_result in async_validation_operator_results:
                async_result = async_validation_operator_result.result()
                run_results = async_result.run_results

                run_result: dict
                validation_result: Optional[ExpectationSuiteValidationResult]
                meta: ExpectationSuiteValidationResultMeta
                validation_result_url: str | None = None
                for run_result in run_results.values():
                    validation_result = run_result.get("validation_result")  # type: ignore[assignment] # could be dict
                    if validation_result:
                        meta = validation_result.meta  # type: ignore[assignment] # could be dict
                        id = self.ge_cloud_id
                        meta["checkpoint_id"] = id
                    # TODO: We only currently support 1 validation_result_url per checkpoint and use the first one we
                    #       encounter. Since checkpoints can have more than 1 validation result, we will need to update
                    #       this and the consumers.
                    if not validation_result_url:
                        if (
                            "actions_results" in run_result
                            and "store_validation_result"
                            in run_result["actions_results"]
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

    def get_substituted_config(
        self,
        runtime_kwargs: Optional[dict] = None,
    ) -> dict:
        if runtime_kwargs is None:
            runtime_kwargs = {}

        config_kwargs: dict = self.get_config(mode=ConfigOutputModes.JSON_DICT)  # type: ignore[assignment] # always returns a dict

        template_name: Optional[str] = runtime_kwargs.get("template_name")
        if template_name:
            config_kwargs["template_name"] = template_name

        substituted_runtime_config: dict = self._get_substituted_template(
            source_config=config_kwargs
        )
        substituted_runtime_config = self._get_substituted_runtime_kwargs(
            source_config=substituted_runtime_config, runtime_kwargs=runtime_kwargs
        )

        return substituted_runtime_config

    def _get_substituted_template(
        self,
        source_config: dict,
    ) -> dict:
        substituted_config: dict

        template_name = source_config.get("template_name")
        if template_name:
            checkpoint: Checkpoint = self.data_context.get_checkpoint(
                name=template_name
            )
            template_config: dict = checkpoint.config.to_json_dict()

            if template_config["config_version"] != source_config["config_version"]:
                raise gx_exceptions.CheckpointError(
                    f"Invalid template '{template_name}' (ver. {template_config['config_version']}) for Checkpoint "
                    f"'{source_config}' (ver. {source_config['config_version']}. Checkpoints can only use templates with the same config_version."
                )

            substituted_template_config: dict = self._get_substituted_template(
                source_config=template_config
            )
            substituted_config = substitute_template_config(
                source_config=source_config, template_config=substituted_template_config
            )
        else:
            substituted_config = copy.deepcopy(source_config)

        if self._using_cloud_context:
            return substituted_config

        return self._substitute_config_variables(config=substituted_config)

    def _get_substituted_runtime_kwargs(
        self,
        source_config: dict,
        runtime_kwargs: Optional[dict] = None,
    ) -> dict:
        if runtime_kwargs is None:
            runtime_kwargs = {}

        substituted_config: dict = substitute_runtime_config(
            source_config=source_config, runtime_kwargs=runtime_kwargs
        )

        if self._using_cloud_context:
            return substituted_config

        return self._substitute_config_variables(config=substituted_config)

    def _substitute_config_variables(self, config: dict) -> dict:
        return self.data_context.config_provider.substitute_config(config)

    def _run_validation(  # noqa: PLR0913
        self,
        substituted_runtime_config: dict,
        async_validation_operator_results: List[AsyncResult],
        async_executor: AsyncExecutor,
        result_format: Union[dict, str, None],
        run_id: Optional[Union[str, RunIdentifier]],
        idx: Optional[int] = 0,
        validation_dict: Optional[dict] = None,
    ) -> None:
        if validation_dict is None:
            validation_dict = {
                "id": substituted_runtime_config.get("default_validation_id")
            }

        try:
            substituted_validation_dict: dict = get_substituted_validation_dict(
                substituted_runtime_config=substituted_runtime_config,
                validation_dict=validation_dict,
            )
            validate_validation_dict(
                validation_dict=substituted_validation_dict,
                batch_request_required=(not self._validator),
            )

            batch_request: Union[
                BatchRequest, FluentBatchRequest, RuntimeBatchRequest, None
            ] = substituted_validation_dict.get("batch_request")
            expectation_suite_name: str | None = substituted_validation_dict.get(
                "expectation_suite_name"
            )
            expectation_suite_ge_cloud_id: str | None = substituted_validation_dict.get(
                "expectation_suite_ge_cloud_id"
            )
            include_rendered_content: Optional[bool] = substituted_validation_dict.get(
                "include_rendered_content"
            )
            if include_rendered_content is None:
                include_rendered_content = (
                    self._data_context._determine_if_expectation_validation_result_include_rendered_content()
                )

            validator: Validator = self._validator or self.data_context.get_validator(
                batch_request=batch_request,
                expectation_suite_name=expectation_suite_name
                if not self._using_cloud_context
                else None,
                expectation_suite_ge_cloud_id=(
                    expectation_suite_ge_cloud_id if self._using_cloud_context else None
                ),
                include_rendered_content=include_rendered_content,
            )

            action_list: Sequence[ActionDict] | None = substituted_validation_dict.get(
                "action_list"
            )
            runtime_configuration_validation = substituted_validation_dict.get(
                "runtime_configuration", {}
            )
            catch_exceptions_validation = runtime_configuration_validation.get(
                "catch_exceptions"
            )
            result_format_validation = runtime_configuration_validation.get(
                "result_format"
            )
            result_format = result_format or result_format_validation

            if result_format is None:
                result_format = {"result_format": "SUMMARY"}

            action_list_validation_operator: ActionListValidationOperator = (
                ActionListValidationOperator(
                    data_context=self.data_context,
                    action_list=action_list,
                    result_format=result_format,
                    name=f"{self.name}-checkpoint-validation[{idx}]",
                )
            )
            checkpoint_identifier = None
            if self._using_cloud_context:
                checkpoint_identifier = GXCloudIdentifier(
                    resource_type=GXCloudRESTResource.CHECKPOINT,
                    id=self.ge_cloud_id,
                )

            operator_run_kwargs = {}

            if catch_exceptions_validation is not None:
                operator_run_kwargs["catch_exceptions"] = catch_exceptions_validation

            validation_id: Optional[str] = substituted_validation_dict.get("id")

            async_validation_operator_result = async_executor.submit(
                action_list_validation_operator.run,
                assets_to_validate=[validator],
                run_id=run_id,
                evaluation_parameters=substituted_validation_dict.get(
                    "evaluation_parameters"
                ),
                result_format=result_format,
                checkpoint_identifier=checkpoint_identifier,
                checkpoint_name=self.name,
                validation_id=validation_id,
                **operator_run_kwargs,
            )
            async_validation_operator_results.append(async_validation_operator_result)
        except (
            gx_exceptions.CheckpointError,
            gx_exceptions.ExecutionEngineError,
            gx_exceptions.MetricError,
        ) as e:
            raise gx_exceptions.CheckpointError(
                f"Exception occurred while running validation[{idx}] of Checkpoint '{self.name}': {e.message}."
            ) from e

    def self_check(self, pretty_print: bool = True) -> dict:
        """Method that is intended to provide visibility into parameters that Checkpoint was instantiated with.

        If used as part of the test_yaml_config() workflow, the user will be able to know if the Checkpoint is
        configured with all necessary parameters.

        When run with self_check()::

            yaml_config: str = # [a checkpoint yaml configuration]
            config: CommentedMap = yaml.load(yaml_config)
            checkpoint_config: CheckpointConfig = CheckpointConfig(**config)
            checkpoint: Checkpoint = Checkpoint(
                data_context=context,
                checkpoint_config.to_json_dict()
            )
            checkpoint.self_check()

        When run with test_yaml_config::

            checkpoint: Checkpoint = context.test_yaml_config(
                yaml_config=yaml_config,
                name="my_checkpoint"
                )

        Args:
            pretty_print (bool): If True, then additional messages if Checkpoint configuration is missing
                a "validations" or "action_list" attribute.

        Returns:
            Dictionary containing Checkpoint configuration converted into json dictionary.

        """
        report_object: dict = {"config": self.config.to_json_dict()}

        if pretty_print:
            print(f"\nCheckpoint class name: {self.__class__.__name__}")

        validations_present: bool = bool(
            self.validations
            and isinstance(self.validations, list)
            and len(self.validations) > 0
        )
        action_list: Optional[Sequence[ActionDict]] = self.action_list
        action_list_present: bool = (
            action_list is not None
            and isinstance(action_list, list)
            and len(action_list) > 0
        ) or (
            validations_present
            and all(
                (
                    validation.get("action_list")
                    and isinstance(validation["action_list"], list)
                    and len(validation["action_list"]) > 0
                )
                for validation in self.validations
            )
        )
        if pretty_print:
            if not validations_present:
                print(
                    """Your current Checkpoint configuration has an empty or missing "validations" attribute.  This
means you must either update your Checkpoint configuration or provide an appropriate validations
list programmatically (i.e., when your Checkpoint is run).
                    """
                )
            if not action_list_present:
                print(
                    """Your current Checkpoint configuration has an empty or missing "action_list" attribute.  This
means you must provide an appropriate validations list programmatically (i.e., when your Checkpoint
is run), with each validation having its own defined "action_list" attribute.
                    """
                )

        return report_object

    @property
    def config(self) -> CheckpointConfig:
        return self._checkpoint_config

    @property
    def name(self) -> Optional[str]:
        try:
            return self.config.name
        except AttributeError:
            return None

    @property
    def config_version(self) -> Optional[float]:
        try:
            return self.config.config_version
        except AttributeError:
            return None

    @property
    def action_list(self) -> Sequence[ActionDict]:
        try:
            return self.config.action_list
        except AttributeError:
            return []

    @property
    def validations(
        self,
    ) -> list[CheckpointValidationConfig] | list[dict]:  # always a list[dict]
        try:
            return self.config.validations
        except AttributeError:
            result: list[dict] = []
            return result

    @property
    def ge_cloud_id(self) -> Optional[str]:
        try:
            return self.config.ge_cloud_id
        except AttributeError:
            return None

    @property
    def data_context(self) -> AbstractDataContext:
        return self._data_context

    @property
    def _using_cloud_context(self) -> bool:
        # Chetan - 20221216 - This is a temporary property to encapsulate any Cloud leakage
        # Upon refactoring this class to decouple Cloud-specific branches, this should be removed
        from great_expectations.data_context.data_context.cloud_data_context import (
            CloudDataContext,
        )

        return isinstance(self.data_context, CloudDataContext)

    def __repr__(self) -> str:
        return str(self.get_config())


@public_api
@deprecated_argument(argument_name="validation_operator_name", version="0.14.0")
@deprecated_argument(argument_name="batches", version="0.14.0")
@new_argument(
    argument_name="ge_cloud_id", version="0.13.33", message="Used in cloud deployments."
)
@new_argument(
    argument_name="expectation_suite_ge_cloud_id",
    version="0.13.33",
    message="Used in cloud deployments.",
)
class Checkpoint(BaseCheckpoint):
    """A checkpoint is the primary means for validating data in a production deployment of Great Expectations.

    Checkpoints provide a convenient abstraction for bundling the Validation of a Batch (or Batches) of data against
    an Expectation Suite (or several), as well as the Actions that should be taken after the validation.

    A Checkpoint uses a Validator to run one or more Expectation Suites against one or more Batches provided by one
    or more Batch Requests. Running a Checkpoint produces Validation Results and will result in optional Actions
    being performed if they are configured to do so.

    Args:
        name: User-selected cCheckpoint name (e.g. "staging_tables").
        data_context: Data context that is associated with the current checkpoint.
        config_version: Version number of the checkpoint configuration.
        template_name: The name of another checkpoint to use as a base template.
        run_name_template: A template to create run names, using environment
            variables and datetime-template syntax (e.g. "%Y-%M-staging-$MY_ENV_VAR").
        expectation_suite_name: Expectation suite associated with checkpoint.
        batch_request: Batch request describing the batch of data to validate.
        action_list: A list of actions to perform after each batch is validated.
        evaluation_parameters: Evaluation parameters to use in generating this checkpoint.
        runtime_configuration: Runtime configuration to pass into the validator's runtime configuration
            (e.g. `result_format`).
        validations: Validations to be executed as part of checkpoint.
        profilers: Profilers to use in generating this checkpoint.
        validation_operator_name: List of validation Operators configured by the Checkpoint.
        batches: List of Batches for validation by Checkpoint.
        ge_cloud_id: Great Expectations Cloud id for this Checkpoint.
        expectation_suite_ge_cloud_id: Great Expectations Cloud id associated with Expectation Suite.
        default_validation_id:  Default value used by Checkpoint if no Validations are configured.

    Raises:
        ValueError: If BatchRequest contains batch_data, since only primitive types are allowed in the constructor.
        ValueError: If Validations contains batch_data, since only primitive types are allowed in the constructor.
    """

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
    """

    def __init__(  # noqa: PLR0913
        self,
        name: str,
        data_context: AbstractDataContext,
        config_version: Union[int, float] = 1.0,
        template_name: Optional[str] = None,
        run_name_template: Optional[str] = None,
        expectation_suite_name: Optional[str] = None,
        batch_request: Optional[
            Union[BatchRequestBase, FluentBatchRequest, dict]
        ] = None,
        validator: Optional[Validator] = None,
        action_list: Optional[Sequence[ActionDict]] = None,
        evaluation_parameters: Optional[dict] = None,
        runtime_configuration: Optional[dict] = None,
        validations: Optional[List[dict]] = None,
        profilers: Optional[List[dict]] = None,
        ge_cloud_id: Optional[str] = None,
        expectation_suite_ge_cloud_id: Optional[str] = None,
        default_validation_id: Optional[str] = None,
    ) -> None:
        if validator:
            if batch_request or _does_validation_contain_batch_request(
                validations=validations
            ):
                raise gx_exceptions.CheckpointError(
                    f'Checkpoint "{name}" cannot be called with a validator and contain a batch_request and/or a batch_request in validations.'
                )

            if (
                expectation_suite_name
                or _does_validation_contain_expectation_suite_name(
                    validations=validations
                )
            ):
                raise gx_exceptions.CheckpointError(
                    f'Checkpoint "{name}" cannot be called with a validator and contain an expectation_suite_name and/or an expectation_suite_name in validations.'
                )

            expectation_suite_name = validator.expectation_suite_name

        # Only primitive types are allowed as constructor arguments; data frames are supplied to "run()" as arguments.
        if batch_request_contains_batch_data(batch_request=batch_request):
            raise ValueError(
                """Error: batch_data found in batch_request -- only primitive types are allowed as Checkpoint \
constructor arguments.
"""
            )

        if does_batch_request_in_validations_contain_batch_data(
            validations=validations
        ):
            raise ValueError(
                """Error: batch_data found in batch_request -- only primitive types are allowed as Checkpoint \
constructor arguments.
"""
            )

        checkpoint_config = CheckpointConfig(
            name=name,
            config_version=config_version,
            class_name=self.__class__.__name__,
            template_name=template_name,
            run_name_template=run_name_template,
            expectation_suite_name=expectation_suite_name,
            batch_request=batch_request,  # type: ignore[arg-type] # FluentBatchRequest is not a dict
            # TODO: check if `pydantic.BaseModel` and call `batch_request.dict()`??
            action_list=action_list,
            evaluation_parameters=evaluation_parameters,
            runtime_configuration=runtime_configuration,
            validations=validations,
            profilers=profilers,
            ge_cloud_id=ge_cloud_id,
            expectation_suite_ge_cloud_id=expectation_suite_ge_cloud_id,
            default_validation_id=default_validation_id,
        )
        super().__init__(
            checkpoint_config=checkpoint_config,
            data_context=data_context,
        )

        self._validator = validator

    def run_with_runtime_args(  # noqa: PLR0913
        self,
        template_name: Optional[str] = None,
        run_name_template: Optional[str] = None,
        expectation_suite_name: Optional[str] = None,
        batch_request: Optional[
            Union[BatchRequestBase, FluentBatchRequest, dict]
        ] = None,
        validator: Optional[Validator] = None,
        action_list: Optional[Sequence[ActionDict]] = None,
        evaluation_parameters: Optional[dict] = None,
        runtime_configuration: Optional[dict] = None,
        validations: Optional[List[dict]] = None,
        profilers: Optional[List[dict]] = None,
        run_id: Optional[Union[str, int, float]] = None,
        run_name: Optional[str] = None,
        run_time: Optional[datetime.datetime] = None,
        result_format: str | dict | None = None,  # TODO: type-dict?
        expectation_suite_ge_cloud_id: Optional[str] = None,
        **kwargs,
    ) -> CheckpointResult:
        checkpoint_config_from_store: CheckpointConfig = cast(
            CheckpointConfig, self.get_config()
        )

        if (
            "runtime_configuration" in checkpoint_config_from_store
            and checkpoint_config_from_store.runtime_configuration
            and "result_format" in checkpoint_config_from_store.runtime_configuration
        ):
            result_format = (
                result_format
                or checkpoint_config_from_store.runtime_configuration.get(
                    "result_format"
                )
            )

        if result_format is None:
            result_format = {"result_format": "SUMMARY"}

        batch_request = get_batch_request_as_dict(batch_request=batch_request)
        validations = get_validations_with_batch_request_as_dict(
            validations=validations
        )

        checkpoint_config_from_call_args: dict = {
            "template_name": template_name,
            "run_name_template": run_name_template,
            "expectation_suite_name": expectation_suite_name,
            "batch_request": batch_request,
            "validator": validator,
            "action_list": action_list,
            "evaluation_parameters": evaluation_parameters,
            "runtime_configuration": runtime_configuration,
            "validations": validations,
            "profilers": profilers,
            "run_id": run_id,
            "run_name": run_name,
            "run_time": run_time,
            "result_format": result_format,
            "expectation_suite_ge_cloud_id": expectation_suite_ge_cloud_id,
        }

        checkpoint_config: dict = {
            key: value
            for key, value in checkpoint_config_from_store.items()
            if key in checkpoint_config_from_call_args
        }
        checkpoint_config.update(checkpoint_config_from_call_args)

        checkpoint_run_arguments: dict = dict(**checkpoint_config, **kwargs)
        filter_properties_dict(
            properties=checkpoint_run_arguments,
            clean_falsy=True,
            inplace=True,
        )

        return self.run(**checkpoint_run_arguments)

    @classmethod
    def construct_from_config_args(  # noqa: PLR0913
        cls,
        data_context: AbstractDataContext,
        checkpoint_store_name: str,
        name: str,
        config_version: Optional[Union[int, float]] = 1.0,
        template_name: Optional[str] = None,
        module_name: str = "great_expectations.checkpoint",
        class_name: Literal["Checkpoint", "SimpleCheckpoint"] = "Checkpoint",
        run_name_template: Optional[str] = None,
        expectation_suite_name: Optional[str] = None,
        batch_request: Optional[dict] = None,
        action_list: Optional[Sequence[ActionDict]] = None,
        evaluation_parameters: Optional[dict] = None,
        runtime_configuration: Optional[dict] = None,
        validations: Optional[
            Union[List[dict], List[CheckpointValidationConfig]]
        ] = None,
        profilers: Optional[List[dict]] = None,
        # the following four arguments are used by SimpleCheckpoint
        site_names: Optional[Union[str, List[str]]] = None,
        slack_webhook: Optional[str] = None,
        notify_on: Optional[str] = None,
        notify_with: Optional[Union[str, List[str]]] = None,
        ge_cloud_id: Optional[str] = None,
        expectation_suite_ge_cloud_id: Optional[str] = None,
        default_validation_id: Optional[str] = None,
        validator: Validator | None = None,
    ) -> Checkpoint:
        checkpoint_config: Union[CheckpointConfig, dict]

        validations = cls._reconcile_validations(
            validations=validations, validator=validator
        )

        # These checks protect against typed objects (BatchRequest and/or RuntimeBatchRequest) encountered in arguments.
        batch_request = get_batch_request_as_dict(batch_request=batch_request)
        validations = get_validations_with_batch_request_as_dict(
            validations=validations
        )

        # DataFrames shouldn't be saved to CheckpointStore
        if batch_request_contains_batch_data(batch_request=batch_request):
            raise gx_exceptions.InvalidConfigError(
                f'batch_data found in batch_request cannot be saved to CheckpointStore "{checkpoint_store_name}"'
            )

        if does_batch_request_in_validations_contain_batch_data(
            validations=validations
        ):
            raise gx_exceptions.InvalidConfigError(
                f'batch_data found in validations cannot be saved to CheckpointStore "{checkpoint_store_name}"'
            )

        checkpoint_config = {
            "name": name,
            "config_version": config_version,
            "template_name": template_name,
            "module_name": module_name,
            "class_name": class_name,
            "run_name_template": run_name_template,
            "expectation_suite_name": expectation_suite_name,
            "batch_request": batch_request,
            "action_list": action_list,
            "evaluation_parameters": evaluation_parameters,
            "runtime_configuration": runtime_configuration,
            "validations": validations,
            "profilers": profilers,
            "ge_cloud_id": ge_cloud_id,
            "expectation_suite_ge_cloud_id": expectation_suite_ge_cloud_id,
            "default_validation_id": default_validation_id,
        }

        default_checkpoints_module_name = "great_expectations.checkpoint"

        klass = load_class(
            class_name=class_name,
            module_name=module_name or default_checkpoints_module_name,
        )
        if issubclass(klass, SimpleCheckpoint):
            checkpoint_config.update(
                {
                    # the following four keys are used by SimpleCheckpoint
                    "site_names": site_names,
                    "slack_webhook": slack_webhook,
                    "notify_on": notify_on,
                    "notify_with": notify_with,
                }
            )
        elif not issubclass(klass, Checkpoint):
            raise gx_exceptions.InvalidCheckpointConfigError(
                f'Custom class "{klass.__name__}" must extend either "Checkpoint" or "SimpleCheckpoint" (exclusively).'
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
        validations: list[dict] | list[CheckpointValidationConfig] | None = None,
        validator: Validator | None = None,
    ) -> list[dict] | None:
        """
        Helper to help resolve logic between validator and validations input
        arguments to `construct_from_config_args`.
        """
        if validator:
            if validations:
                raise ValueError(
                    "Please provide either a validator or validations list (but not both)."
                )
            validations = validator.convert_to_checkpoint_validations_list()

        if not validations:
            return None

        # A lot of downstream logic depends on validations being a dict instead of a rich config type
        # We should patch those instances so they can expect a CheckpointValidationConfig
        validations = [
            validation.to_dict()
            if isinstance(validation, CheckpointValidationConfig)
            else validation
            for validation in validations
        ]

        return validations

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

        checkpoint: Checkpoint = instantiate_class_from_config(
            config=config,
            runtime_environment={
                "data_context": data_context,
            },
            config_defaults={
                "module_name": "great_expectations.checkpoint",
            },
        )

        return checkpoint


@public_api
@deprecated_argument(argument_name="validation_operator_name", version="0.14.0")
@deprecated_argument(argument_name="batches", version="0.14.0")
@new_argument(
    argument_name="ge_cloud_id", version="0.13.33", message="Used in cloud deployments."
)
@new_argument(
    argument_name="expectation_suite_ge_cloud_id",
    version="0.13.33",
    message="Used in cloud deployments.",
)
class SimpleCheckpoint(Checkpoint):
    """A SimpleCheckpoint provides a means to simplify the process of specifying a Checkpoint configuration.

    It provides a basic set of actions - store Validation Result, store Evaluation Parameters, update Data Docs,
    and optionally, send a Slack notification - allowing you to omit an action_list from your configuration and at runtime.

    Args:
        name: user-selected Checkpoint name (e.g. `staging_tables`).
        data_context: Data context that is associated with the current checkpoint.
        config_version: version number of the checkpoint configuration.
        template_name: the name of another checkpoint to use as a base template.
        run_name_template: a template to create run names, using environment variables and datetime-template syntax (e.g. `%Y-%M-staging-$MY_ENV_VAR`).
        expectation_suite_name: expectation suite associated with checkpoint.
        batch_request: batch request describing the batch of data to validate.
        action_list: a list of actions to perform after each batch is validated.
        evaluation_parameters: evaluation parameters to use in generating this checkpoint.
        runtime_configuration: runtime configuration to pass into the validator's runtime configuration (e.g. `result_format`).
        validations: validations to be executed as part of checkpoint.
        profilers: profilers to use in generating this checkpoint.
        validation_operator_name: list of validation operators configured by the checkpoint.
        batches: list of batches for validation by checkpoint.
        ge_cloud_id: Great Expectations Cloud id for this checkpoint.
        site_names: a list of Data Docs site names to update as part of the update Data Docs action - defaults to `all`.
        slack_webhook:  if provided, an action will be added that sends a Slack notification to the provided webhook.
        notify_on: used to define when a notification is fired, according to validation result outcome - `all`, `failure`, or `success`. Defaults to `all`.
        notify_with: a list of Data Docs site names for which to include a URL in any notifications - defaults to `all`.
        expectation_suite_ge_cloud_id: Great Expectations Cloud id associated with expectation suite.
        kwargs: additional keyword arguments.
    """

    _configurator_class = SimpleCheckpointConfigurator
    name: str

    # noinspection PyUnusedLocal
    def __init__(  # noqa: PLR0913
        self,
        name: str,
        data_context,
        config_version: Optional[Union[int, float]] = 1.0,
        template_name: Optional[str] = None,
        run_name_template: Optional[str] = None,
        expectation_suite_name: Optional[str] = None,
        batch_request: Optional[
            Union[BatchRequestBase, FluentBatchRequest, dict]
        ] = None,
        validator: Optional[Validator] = None,
        action_list: Optional[Sequence[ActionDict]] = None,
        evaluation_parameters: Optional[dict] = None,
        runtime_configuration: Optional[dict] = None,
        validations: Optional[List[dict]] = None,
        profilers: Optional[List[dict]] = None,
        ge_cloud_id: Optional[str] = None,
        # the following four arguments are used by SimpleCheckpointConfigurator
        site_names: Union[str, List[str]] = "all",
        slack_webhook: Optional[str] = None,
        notify_on: str = "all",
        notify_with: Union[str, List[str]] = "all",
        expectation_suite_ge_cloud_id: Optional[str] = None,
        **kwargs,
    ) -> None:
        checkpoint_config: CheckpointConfig = self._configurator_class(
            name=name,
            data_context=data_context,
            config_version=config_version,
            template_name=template_name,
            run_name_template=run_name_template,
            expectation_suite_name=expectation_suite_name,
            batch_request=batch_request,
            action_list=action_list,
            evaluation_parameters=evaluation_parameters,
            runtime_configuration=runtime_configuration,
            validations=validations,
            profilers=profilers,
            site_names=site_names,
            slack_webhook=slack_webhook,
            notify_on=notify_on,
            notify_with=notify_with,
            ge_cloud_id=ge_cloud_id,
            expectation_suite_ge_cloud_id=expectation_suite_ge_cloud_id,
        ).build()

        super().__init__(
            name=checkpoint_config.name,
            data_context=data_context,
            config_version=checkpoint_config.config_version,
            template_name=checkpoint_config.template_name,
            run_name_template=checkpoint_config.run_name_template,
            expectation_suite_name=checkpoint_config.expectation_suite_name,
            batch_request=batch_request,
            validator=validator,
            action_list=checkpoint_config.action_list,
            evaluation_parameters=checkpoint_config.evaluation_parameters,
            runtime_configuration=checkpoint_config.runtime_configuration,
            validations=validations,
            profilers=checkpoint_config.profilers,
            ge_cloud_id=checkpoint_config.ge_cloud_id,
            expectation_suite_ge_cloud_id=checkpoint_config.expectation_suite_ge_cloud_id,
        )

    @public_api
    @new_argument(
        argument_name="expectation_suite_ge_cloud_id",
        version="0.13.33",
        message="Used in cloud deployments.",
    )
    def run(  # noqa: PLR0913
        self,
        template_name: Optional[str] = None,
        run_name_template: Optional[str] = None,
        expectation_suite_name: Optional[str] = None,
        batch_request: Optional[
            Union[BatchRequestBase, FluentBatchRequest, dict]
        ] = None,
        validator: Optional[Validator] = None,
        action_list: Optional[Sequence[ActionDict]] = None,
        evaluation_parameters: Optional[dict] = None,
        runtime_configuration: Optional[dict] = None,
        validations: Optional[List[dict]] = None,
        profilers: Optional[List[dict]] = None,
        run_id: Optional[Union[str, RunIdentifier]] = None,
        run_name: Optional[str] = None,
        run_time: Optional[Union[str, datetime.datetime]] = None,
        result_format: Optional[str] = None,
        # the following four arguments are specific to SimpleCheckpoint
        site_names: Union[str, List[str]] = "all",
        slack_webhook: Optional[str] = None,
        notify_on: str = "all",
        notify_with: Union[str, List[str]] = "all",
        expectation_suite_ge_cloud_id: Optional[str] = None,
    ) -> CheckpointResult:
        """Validate against the current SimpleCheckpoint.

        Arguments allow for override of the current SimpleCheckpoint configuration.

        Args:
            template_name: The name of another checkpoint to use as a base template.
            run_name_template: A template to create run names, using environment
                variables and datetime-template syntax (e.g. "%Y-%M-staging-$MY_ENV_VAR").
            expectation_suite_name: Expectation suite associated with checkpoint.
            batch_request: Batch request describing the batch of data to validate.
            validator: Validator objects, loaded with Batch data samples, can be supplied (in lieu of  "batch_request")
            action_list: A list of actions to perform after each batch is validated.
            evaluation_parameters: Evaluation parameters to use in generating this checkpoint.
            runtime_configuration: Runtime configuration to pass into the validator's runtime configuration
                (e.g. `result_format`).
            validations: Validations to be executed as part of checkpoint.
            profilers: Profilers to use in generating this checkpoint.
            run_id: The run_id for the validation; if None, a default value will be used.
            run_name: The run_name for the validation; if None, a default value will be used.
            run_time: The date/time of the run.
            result_format: One of several supported formatting directives for expectation validation results
            site_names: a list of Data Docs site names to update as part of the update Data Docs action - defaults to `all`.
            slack_webhook:  if provided, an action will be added that sends a Slack notification to the provided webhook.
            notify_on: used to define when a notification is fired, according to validation result outcome - `all`, `failure`, or `success`. Defaults to `all`.
            notify_with: a list of Data Docs site names for which to include a URL in any notifications - defaults to `all`.
            expectation_suite_ge_cloud_id: Great Expectations Cloud id for the expectation suite

        Returns:
            CheckpointResult
        """
        new_baseline_config = None

        # if any SimpleCheckpoint-specific kwargs are passed, generate a new baseline config using configurator,
        # passing only action_list, since this is the only config key that would be affected by the
        # SimpleCheckpoint-specific kwargs
        if any((site_names, slack_webhook, notify_on, notify_with)):
            new_baseline_config = self._configurator_class(
                name=self.name,
                data_context=self.data_context,
                action_list=action_list,
                site_names=site_names,
                slack_webhook=slack_webhook,
                notify_on=notify_on,
                notify_with=notify_with,
            ).build()

        return super().run(
            template_name=template_name,
            run_name_template=run_name_template,
            expectation_suite_name=expectation_suite_name,
            batch_request=batch_request,
            validator=validator,
            action_list=new_baseline_config.action_list
            if new_baseline_config
            else action_list,
            evaluation_parameters=evaluation_parameters,
            runtime_configuration=runtime_configuration,
            validations=validations,
            profilers=profilers,
            run_id=run_id,
            run_name=run_name,
            run_time=run_time,
            result_format=result_format,
            expectation_suite_ge_cloud_id=expectation_suite_ge_cloud_id,
        )
