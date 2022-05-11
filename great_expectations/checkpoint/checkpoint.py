import copy
import datetime
import json
import logging
import os
from typing import Any, Dict, List, Optional, Union, cast
from uuid import UUID

import great_expectations.exceptions as ge_exceptions
from great_expectations.checkpoint.configurator import SimpleCheckpointConfigurator
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.checkpoint.util import (
    batch_request_in_validations_contains_batch_data,
    get_substituted_validation_dict,
    get_validations_with_batch_request_as_dict,
    substitute_runtime_config,
    substitute_template_config,
)
from great_expectations.core import RunIdentifier
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
from great_expectations.core.util import get_datetime_string_from_strftime_format
from great_expectations.data_asset import DataAsset
from great_expectations.data_context.types.base import CheckpointConfig
from great_expectations.data_context.types.resource_identifiers import GeCloudIdentifier
from great_expectations.data_context.util import (
    instantiate_class_from_config,
    substitute_all_config_variables,
)
from great_expectations.util import (
    deep_filter_properties_iterable,
    filter_properties_dict,
)
from great_expectations.validation_operators import ActionListValidationOperator
from great_expectations.validation_operators.types.validation_operator_result import (
    ValidationOperatorResult,
)
from great_expectations.validator.validator import Validator

logger = logging.getLogger(__name__)


class BaseCheckpoint(ConfigPeer):
    "\n    BaseCheckpoint class is initialized from CheckpointConfig typed object and contains all functionality\n    in the form of interface methods (which can be overwritten by subclasses) and their reference implementation.\n"

    def __init__(
        self, checkpoint_config: CheckpointConfig, data_context: "DataContext"
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if "DataContext" not in str(type(data_context)):
            raise TypeError("A Checkpoint requires a valid DataContext")
        self._usage_statistics_handler = data_context._usage_statistics_handler
        self._data_context = data_context
        self._checkpoint_config = checkpoint_config

    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.CHECKPOINT_RUN.value,
        args_payload_fn=get_checkpoint_run_usage_statistics,
    )
    def run(
        self,
        template_name: Optional[str] = None,
        run_name_template: Optional[str] = None,
        expectation_suite_name: Optional[str] = None,
        batch_request: Optional[Union[(BatchRequestBase, dict)]] = None,
        action_list: Optional[List[dict]] = None,
        evaluation_parameters: Optional[dict] = None,
        runtime_configuration: Optional[dict] = None,
        validations: Optional[List[dict]] = None,
        profilers: Optional[List[dict]] = None,
        run_id: Optional[Union[(str, RunIdentifier)]] = None,
        run_name: Optional[str] = None,
        run_time: Optional[Union[(str, datetime.datetime)]] = None,
        result_format: Optional[Union[(str, dict)]] = None,
        expectation_suite_ge_cloud_id: Optional[str] = None,
    ) -> CheckpointResult:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        assert (not (run_id and run_name)) and (
            not (run_id and run_time)
        ), "Please provide either a run_id or run_name and/or run_time."
        run_time = run_time or datetime.datetime.now()
        runtime_configuration = runtime_configuration or {}
        result_format = result_format or runtime_configuration.get("result_format")
        batch_request = get_batch_request_as_dict(batch_request=batch_request)
        validations = get_validations_with_batch_request_as_dict(
            validations=validations
        )
        runtime_kwargs: dict = {
            "template_name": template_name,
            "run_name_template": run_name_template,
            "expectation_suite_name": expectation_suite_name,
            "batch_request": (batch_request or {}),
            "action_list": (action_list or []),
            "evaluation_parameters": (evaluation_parameters or {}),
            "runtime_configuration": (runtime_configuration or {}),
            "validations": (validations or []),
            "profilers": (profilers or []),
            "expectation_suite_ge_cloud_id": expectation_suite_ge_cloud_id,
        }
        substituted_runtime_config: dict = self.get_substituted_config(
            runtime_kwargs=runtime_kwargs
        )
        run_name_template = substituted_runtime_config.get("run_name_template")
        batch_request = substituted_runtime_config.get("batch_request")
        validations = substituted_runtime_config.get("validations") or []
        if (len(validations) == 0) and (not batch_request):
            raise ge_exceptions.CheckpointError(
                f'Checkpoint "{self.name}" must contain either a batch_request or validations.'
            )
        if (run_name is None) and (run_name_template is not None):
            run_name = get_datetime_string_from_strftime_format(
                format_str=run_name_template, datetime_obj=run_time
            )
        run_id = run_id or RunIdentifier(run_name=run_name, run_time=run_time)
        with AsyncExecutor(
            self.data_context.concurrency, max_workers=len(validations)
        ) as async_executor:
            async_validation_operator_results: List[
                AsyncResult[ValidationOperatorResult]
            ] = []
            if len(validations) > 0:
                for (idx, validation_dict) in enumerate(validations):
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
            run_results: dict = {}
            for async_validation_operator_result in async_validation_operator_results:
                run_results.update(
                    async_validation_operator_result.result().run_results
                )
        return CheckpointResult(
            run_id=run_id, run_results=run_results, checkpoint_config=self.config
        )

    def get_substituted_config(self, runtime_kwargs: Optional[dict] = None) -> dict:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if runtime_kwargs is None:
            runtime_kwargs = {}
        config_kwargs: dict = self.get_config(mode=ConfigOutputModes.JSON_DICT)
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

    def _get_substituted_template(self, source_config: dict) -> dict:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        substituted_config: dict
        template_name = source_config.get("template_name")
        if template_name:
            checkpoint: Checkpoint = self.data_context.get_checkpoint(
                name=template_name
            )
            template_config: dict = checkpoint.config.to_json_dict()
            if template_config["config_version"] != source_config["config_version"]:
                raise ge_exceptions.CheckpointError(
                    f"Invalid template '{template_name}' (ver. {template_config['config_version']}) for Checkpoint '{source_config}' (ver. {source_config['config_version']}. Checkpoints can only use templates with the same config_version."
                )
            substituted_template_config: dict = self._get_substituted_template(
                source_config=template_config
            )
            substituted_config = substitute_template_config(
                source_config=source_config, template_config=substituted_template_config
            )
        else:
            substituted_config = copy.deepcopy(source_config)
        if self.data_context.ge_cloud_mode:
            return substituted_config
        return self._substitute_config_variables(config=substituted_config)

    def _get_substituted_runtime_kwargs(
        self, source_config: dict, runtime_kwargs: Optional[dict] = None
    ) -> dict:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if runtime_kwargs is None:
            runtime_kwargs = {}
        substituted_config: dict = substitute_runtime_config(
            source_config=source_config, runtime_kwargs=runtime_kwargs
        )
        if self.data_context.ge_cloud_mode:
            return substituted_config
        return self._substitute_config_variables(config=substituted_config)

    def _substitute_config_variables(self, config: dict) -> dict:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        substituted_config_variables = substitute_all_config_variables(
            self.data_context.config_variables,
            dict(os.environ),
            self.data_context.DOLLAR_SIGN_ESCAPE_STRING,
        )
        substitutions = {
            **substituted_config_variables,
            **dict(os.environ),
            **self.data_context.runtime_environment,
        }
        return substitute_all_config_variables(
            data=config,
            replace_variables_dict=substitutions,
            dollar_sign_escape_string=self.data_context.DOLLAR_SIGN_ESCAPE_STRING,
        )

    def _run_validation(
        self,
        substituted_runtime_config: dict,
        async_validation_operator_results: List[AsyncResult],
        async_executor: AsyncExecutor,
        result_format: Optional[dict],
        run_id: Optional[Union[(str, RunIdentifier)]],
        idx: Optional[int] = 0,
        validation_dict: Optional[dict] = None,
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if validation_dict is None:
            validation_dict = {}
        try:
            substituted_validation_dict: dict = get_substituted_validation_dict(
                substituted_runtime_config=substituted_runtime_config,
                validation_dict=validation_dict,
            )
            batch_request: Union[
                (BatchRequest, RuntimeBatchRequest)
            ] = substituted_validation_dict.get("batch_request")
            expectation_suite_name: str = substituted_validation_dict.get(
                "expectation_suite_name"
            )
            expectation_suite_ge_cloud_id: str = substituted_validation_dict.get(
                "expectation_suite_ge_cloud_id"
            )
            validator: Validator = self.data_context.get_validator(
                batch_request=batch_request,
                expectation_suite_name=(
                    expectation_suite_name
                    if (not self.data_context.ge_cloud_mode)
                    else None
                ),
                expectation_suite_ge_cloud_id=(
                    expectation_suite_ge_cloud_id
                    if self.data_context.ge_cloud_mode
                    else None
                ),
            )
            action_list: list = substituted_validation_dict.get("action_list")
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
            if self.data_context.ge_cloud_mode:
                checkpoint_identifier = GeCloudIdentifier(
                    resource_type="contract", ge_cloud_id=str(self.ge_cloud_id)
                )
            operator_run_kwargs = {}
            if catch_exceptions_validation is not None:
                operator_run_kwargs["catch_exceptions"] = catch_exceptions_validation
            async_validation_operator_results.append(
                async_executor.submit(
                    action_list_validation_operator.run,
                    assets_to_validate=[validator],
                    run_id=run_id,
                    evaluation_parameters=substituted_validation_dict.get(
                        "evaluation_parameters"
                    ),
                    result_format=result_format,
                    checkpoint_identifier=checkpoint_identifier,
                    **operator_run_kwargs,
                )
            )
        except (
            ge_exceptions.CheckpointError,
            ge_exceptions.ExecutionEngineError,
            ge_exceptions.MetricError,
        ) as e:
            raise ge_exceptions.CheckpointError(
                f"Exception occurred while running validation[{idx}] of Checkpoint '{self.name}': {e.message}."
            )

    def self_check(self, pretty_print=True) -> dict:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        report_object: dict = {"config": self.config.to_json_dict()}
        if pretty_print:
            print(
                f"""
Checkpoint class name: {self.__class__.__name__}"""
            )
        validations_present: bool = (
            self.validations
            and isinstance(self.validations, list)
            and (len(self.validations) > 0)
        )
        action_list: Optional[list] = self.action_list
        action_list_present: bool = (
            (action_list is not None)
            and isinstance(action_list, list)
            and (len(action_list) > 0)
        ) or (
            validations_present
            and all(
                [
                    (
                        validation.get("action_list")
                        and isinstance(validation["action_list"], list)
                        and (len(validation["action_list"]) > 0)
                    )
                    for validation in self.validations
                ]
            )
        )
        if pretty_print:
            if not validations_present:
                print(
                    'Your current Checkpoint configuration has an empty or missing "validations" attribute.  This\nmeans you must either update your Checkpoint configuration or provide an appropriate validations\nlist programmatically (i.e., when your Checkpoint is run).\n                    '
                )
            if not action_list_present:
                print(
                    'Your current Checkpoint configuration has an empty or missing "action_list" attribute.  This\nmeans you must provide an appropriate validations list programmatically (i.e., when your Checkpoint\nis run), with each validation having its own defined "action_list" attribute.\n                    '
                )
        return report_object

    @property
    def config(self) -> CheckpointConfig:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._checkpoint_config

    @property
    def name(self) -> Optional[str]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        try:
            return self.config.name
        except AttributeError:
            return None

    @property
    def config_version(self) -> Optional[float]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        try:
            return self.config.config_version
        except AttributeError:
            return None

    @property
    def action_list(self) -> List[Dict]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        try:
            return self.config.action_list
        except AttributeError:
            return []

    @property
    def validations(self) -> List[Dict]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        try:
            return self.config.validations
        except AttributeError:
            return []

    @property
    def ge_cloud_id(self) -> Optional[UUID]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        try:
            return self.config.ge_cloud_id
        except AttributeError:
            return None

    @property
    def data_context(self) -> "DataContext":
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._data_context

    def __repr__(self) -> str:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return str(self.get_config())


class Checkpoint(BaseCheckpoint):
    '\n    --ge-feature-maturity-info--\n\n        id: checkpoint\n        title: Newstyle Class-based Checkpoints\n        short_description: Run a configured checkpoint from a notebook.\n        description: Run a configured checkpoint from a notebook.\n        how_to_guide_url: https://docs.greatexpectations.io/en/latest/guides/how_to_guides/validation/how_to_create_a_new_checkpoint.html\n        maturity: Beta\n        maturity_details:\n            api_stability: Mostly stable (transitioning ValidationOperators to Checkpoints)\n            implementation_completeness: Complete\n            unit_test_coverage: Partial ("golden path"-focused tests; error checking tests need to be improved)\n            integration_infrastructure_test_coverage: N/A\n            documentation_completeness: Complete\n            bug_risk: Medium\n\n    --ge-feature-maturity-info--\n'

    def __init__(
        self,
        name: str,
        data_context: "DataContext",
        config_version: Optional[Union[(int, float)]] = None,
        template_name: Optional[str] = None,
        run_name_template: Optional[str] = None,
        expectation_suite_name: Optional[str] = None,
        batch_request: Optional[Union[(BatchRequestBase, dict)]] = None,
        action_list: Optional[List[dict]] = None,
        evaluation_parameters: Optional[dict] = None,
        runtime_configuration: Optional[dict] = None,
        validations: Optional[List[dict]] = None,
        profilers: Optional[List[dict]] = None,
        validation_operator_name: Optional[str] = None,
        batches: Optional[List[dict]] = None,
        ge_cloud_id: Optional[UUID] = None,
        expectation_suite_ge_cloud_id: Optional[UUID] = None,
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if batch_request_contains_batch_data(batch_request=batch_request):
            raise ValueError(
                f"""Error: batch_data found in batch_request -- only primitive types are allowed as Checkpoint constructor arguments.
"""
            )
        if batch_request_in_validations_contains_batch_data(validations=validations):
            raise ValueError(
                f"""Error: batch_data found in batch_request -- only primitive types are allowed as Checkpoint constructor arguments.
"""
            )
        checkpoint_config: CheckpointConfig = CheckpointConfig(
            name=name,
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
            validation_operator_name=validation_operator_name,
            batches=batches,
            ge_cloud_id=ge_cloud_id,
            expectation_suite_ge_cloud_id=expectation_suite_ge_cloud_id,
        )
        super().__init__(checkpoint_config=checkpoint_config, data_context=data_context)

    def run_with_runtime_args(
        self,
        template_name: Optional[str] = None,
        run_name_template: Optional[str] = None,
        expectation_suite_name: Optional[str] = None,
        batch_request: Optional[Union[(BatchRequestBase, dict)]] = None,
        action_list: Optional[List[dict]] = None,
        evaluation_parameters: Optional[dict] = None,
        runtime_configuration: Optional[dict] = None,
        validations: Optional[List[dict]] = None,
        profilers: Optional[List[dict]] = None,
        run_id: Optional[Union[(str, int, float)]] = None,
        run_name: Optional[str] = None,
        run_time: Optional[datetime.datetime] = None,
        result_format: Optional[str] = None,
        expectation_suite_ge_cloud_id: Optional[str] = None,
        **kwargs,
    ) -> CheckpointResult:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        checkpoint_config_from_store: CheckpointConfig = cast(
            CheckpointConfig, self.get_config()
        )
        if (
            ("runtime_configuration" in checkpoint_config_from_store)
            and checkpoint_config_from_store.runtime_configuration
            and ("result_format" in checkpoint_config_from_store.runtime_configuration)
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
            for (key, value) in checkpoint_config_from_store.items()
            if (key in checkpoint_config_from_call_args)
        }
        checkpoint_config.update(checkpoint_config_from_call_args)
        checkpoint_run_arguments: dict = dict(**checkpoint_config, **kwargs)
        filter_properties_dict(
            properties=checkpoint_run_arguments, clean_falsy=True, inplace=True
        )
        return self.run(**checkpoint_run_arguments)

    @staticmethod
    def construct_from_config_args(
        data_context: "DataContext",
        checkpoint_store_name: str,
        name: str,
        config_version: Optional[Union[(int, float)]] = None,
        template_name: Optional[str] = None,
        module_name: Optional[str] = None,
        class_name: Optional[str] = None,
        run_name_template: Optional[str] = None,
        expectation_suite_name: Optional[str] = None,
        batch_request: Optional[dict] = None,
        action_list: Optional[List[dict]] = None,
        evaluation_parameters: Optional[dict] = None,
        runtime_configuration: Optional[dict] = None,
        validations: Optional[List[dict]] = None,
        profilers: Optional[List[dict]] = None,
        validation_operator_name: Optional[str] = None,
        batches: Optional[List[dict]] = None,
        site_names: Optional[Union[(str, List[str])]] = None,
        slack_webhook: Optional[str] = None,
        notify_on: Optional[str] = None,
        notify_with: Optional[Union[(str, List[str])]] = None,
        ge_cloud_id: Optional[str] = None,
        expectation_suite_ge_cloud_id: Optional[str] = None,
    ) -> "Checkpoint":
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        checkpoint_config: Union[(CheckpointConfig, dict)]
        batch_request = get_batch_request_as_dict(batch_request=batch_request)
        validations = get_validations_with_batch_request_as_dict(
            validations=validations
        )
        if batch_request_contains_batch_data(batch_request=batch_request):
            raise ge_exceptions.InvalidConfigError(
                f'batch_data found in batch_request cannot be saved to CheckpointStore "{checkpoint_store_name}"'
            )
        if batch_request_in_validations_contains_batch_data(validations=validations):
            raise ge_exceptions.InvalidConfigError(
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
            "validation_operator_name": validation_operator_name,
            "batches": batches,
            "site_names": site_names,
            "slack_webhook": slack_webhook,
            "notify_on": notify_on,
            "notify_with": notify_with,
            "ge_cloud_id": ge_cloud_id,
            "expectation_suite_ge_cloud_id": expectation_suite_ge_cloud_id,
        }
        checkpoint_config = deep_filter_properties_iterable(
            properties=checkpoint_config, clean_falsy=True
        )
        new_checkpoint: Checkpoint = instantiate_class_from_config(
            config=checkpoint_config,
            runtime_environment={"data_context": data_context},
            config_defaults={"module_name": "great_expectations.checkpoint"},
        )
        return new_checkpoint

    @staticmethod
    def instantiate_from_config_with_runtime_args(
        checkpoint_config: CheckpointConfig,
        data_context: "DataContext",
        **runtime_kwargs,
    ) -> "Checkpoint":
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        config: dict = checkpoint_config.to_json_dict()
        key: str
        value: Any
        for (key, value) in runtime_kwargs.items():
            if value is not None:
                config[key] = value
        config = filter_properties_dict(properties=config, clean_falsy=True)
        checkpoint: Checkpoint = instantiate_class_from_config(
            config=config,
            runtime_environment={"data_context": data_context},
            config_defaults={"module_name": "great_expectations.checkpoint"},
        )
        return checkpoint


class LegacyCheckpoint(Checkpoint):
    '\n    --ge-feature-maturity-info--\n\n        id: checkpoint_notebook\n        title: LegacyCheckpoint - Notebook\n        icon:\n        short_description: Run a configured Checkpoint from a notebook.\n        description: Run a configured Checkpoint from a notebook.\n        how_to_guide_url: https://docs.greatexpectations.io/en/latest/guides/how_to_guides/validation/how_to_run_a_checkpoint_in_python.html\n        maturity: Experimental (to-be-deprecated in favor of Checkpoint)\n        maturity_details:\n            api_stability: to-be-deprecated in favor of Checkpoint\n            implementation_completeness: Complete\n            unit_test_coverage: Partial ("golden path"-focused tests; error checking tests need to be improved)\n            integration_infrastructure_test_coverage: N/A\n            documentation_completeness: Complete\n            bug_risk: Low\n\n        id: checkpoint_command_line\n        title: LegacyCheckpoint - Command Line\n        icon:\n        short_description: Run a configured Checkpoint from a command line.\n        description: Run a configured checkpoint from a command line in a Terminal shell.\n        how_to_guide_url: https://docs.greatexpectations.io/en/latest/guides/how_to_guides/validation/how_to_run_a_checkpoint_in_terminal.html\n        maturity: Experimental (to-be-deprecated in favor of Checkpoint)\n        maturity_details:\n            api_stability: to-be-deprecated in favor of Checkpoint\n            implementation_completeness: Complete\n            unit_test_coverage: Complete\n            integration_infrastructure_test_coverage: N/A\n            documentation_completeness: Complete\n            bug_risk: Low\n\n        id: checkpoint_cron_job\n        title: LegacyCheckpoint - Cron\n        icon:\n        short_description: Deploy a configured Checkpoint as a scheduled task with cron.\n        description: Use the Unix crontab command to edit the cron file and add a line that will run Checkpoint as a scheduled task.\n        how_to_guide_url: https://docs.greatexpectations.io/en/latest/guides/how_to_guides/validation/how_to_deploy_a_scheduled_checkpoint_with_cron.html\n        maturity: Experimental (to-be-deprecated in favor of Checkpoint)\n        maturity_details:\n            api_stability: to-be-deprecated in favor of Checkpoint\n            implementation_completeness: Complete\n            unit_test_coverage: Complete\n            integration_infrastructure_test_coverage: N/A\n            documentation_completeness: Complete\n            bug_risk: Low\n\n        id: checkpoint_airflow_dag\n        title: LegacyCheckpoint - Airflow DAG\n        icon:\n        short_description: Run a configured Checkpoint in Apache Airflow\n        description: Running a configured Checkpoint in Apache Airflow enables the triggering of data validation using an Expectation Suite directly within an Airflow DAG.\n        how_to_guide_url: https://docs.greatexpectations.io/en/latest/guides/how_to_guides/validation/how_to_run_a_checkpoint_in_airflow.html\n        maturity: Beta (to-be-deprecated in favor of Checkpoint)\n        maturity_details:\n            api_stability: to-be-deprecated in favor of Checkpoint\n            implementation_completeness: Partial (no operator, but probably don\'t need one)\n            unit_test_coverage: N/A\n            integration_infrastructure_test_coverage: Minimal\n            documentation_completeness: Complete (pending how-to)\n            bug_risk: Low\n\n        id: checkpoint_kedro\n        title: LegacyCheckpoint - Kedro\n        icon:\n        short_description:\n        description:\n        how_to_guide_url:\n        maturity: Experimental (to-be-deprecated in favor of Checkpoint)\n        maturity_details:\n            api_stability: to-be-deprecated in favor of Checkpoint\n            implementation_completeness: Unknown\n            unit_test_coverage: Unknown\n            integration_infrastructure_test_coverage: Unknown\n            documentation_completeness:  Minimal (none)\n            bug_risk: Unknown\n\n        id: checkpoint_prefect\n        title: LegacyCheckpoint - Prefect\n        icon:\n        short_description:\n        description:\n        how_to_guide_url:\n        maturity: Experimental (to-be-deprecated in favor of Checkpoint)\n        maturity_details:\n            api_stability: to-be-deprecated in favor of Checkpoint\n            implementation_completeness: Unknown\n            unit_test_coverage: Unknown\n            integration_infrastructure_test_coverage: Unknown\n            documentation_completeness: Minimal (none)\n            bug_risk: Unknown\n\n        id: checkpoint_dbt\n        title: LegacyCheckpoint - DBT\n        icon:\n        short_description:\n        description:\n        how_to_guide_url:\n        maturity: Beta (to-be-deprecated in favor of Checkpoint)\n        maturity_details:\n            api_stability: to-be-deprecated in favor of Checkpoint\n            implementation_completeness: Minimal\n            unit_test_coverage: Minimal (none)\n            integration_infrastructure_test_coverage: Minimal (none)\n            documentation_completeness: Minimal (none)\n            bug_risk: Low\n\n    --ge-feature-maturity-info--\n'

    def __init__(
        self,
        name: str,
        data_context,
        validation_operator_name: Optional[str] = None,
        batches: Optional[List[dict]] = None,
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        super().__init__(
            name=name,
            data_context=data_context,
            validation_operator_name=validation_operator_name,
            batches=batches,
        )
        self._validation_operator_name = validation_operator_name
        self._batches = batches

    @property
    def validation_operator_name(self) -> Optional[str]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._validation_operator_name

    @property
    def batches(self) -> Optional[List[dict]]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._batches

    def _run_default_validation_operator(
        self,
        assets_to_validate: List,
        run_id: Optional[Union[(str, RunIdentifier)]] = None,
        evaluation_parameters: Optional[dict] = None,
        run_name: Optional[str] = None,
        run_time: Optional[Union[(str, datetime.datetime)]] = None,
        result_format: Optional[Union[(str, dict)]] = None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        result_format = result_format or {"result_format": "SUMMARY"}
        if not assets_to_validate:
            raise ge_exceptions.DataContextError(
                "No batches of data were passed in. These are required"
            )
        for batch in assets_to_validate:
            if not isinstance(batch, (tuple, DataAsset, Validator)):
                raise ge_exceptions.DataContextError(
                    "Batches are required to be of type DataAsset or Validator"
                )
        if (run_id is None) and (run_name is None):
            run_name = datetime.datetime.now(datetime.timezone.utc).strftime(
                "%Y%m%dT%H%M%S.%fZ"
            )
            logger.info(f"Setting run_name to: {run_name}")
        default_validation_operator = ActionListValidationOperator(
            data_context=self.data_context,
            action_list=[
                {
                    "name": "store_validation_result",
                    "action": {"class_name": "StoreValidationResultAction"},
                },
                {
                    "name": "store_evaluation_params",
                    "action": {"class_name": "StoreEvaluationParametersAction"},
                },
                {
                    "name": "update_data_docs",
                    "action": {"class_name": "UpdateDataDocsAction", "site_names": []},
                },
            ],
            result_format=result_format,
            name="default-action-list-validation-operator",
        )
        if evaluation_parameters is None:
            return default_validation_operator.run(
                assets_to_validate=assets_to_validate,
                run_id=run_id,
                run_name=run_name,
                run_time=run_time,
                result_format=result_format,
            )
        else:
            return default_validation_operator.run(
                assets_to_validate=assets_to_validate,
                run_id=run_id,
                evaluation_parameters=evaluation_parameters,
                run_name=run_name,
                run_time=run_time,
                result_format=result_format,
            )

    def run(
        self,
        run_id=None,
        evaluation_parameters=None,
        run_name=None,
        run_time=None,
        result_format=None,
        **kwargs,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        batches_to_validate = self._get_batches_to_validate(self.batches)
        if (
            self.validation_operator_name
            and self.data_context.validation_operators.get(
                self.validation_operator_name
            )
        ):
            results = self.data_context.run_validation_operator(
                self.validation_operator_name,
                assets_to_validate=batches_to_validate,
                run_id=run_id,
                evaluation_parameters=evaluation_parameters,
                run_name=run_name,
                run_time=run_time,
                result_format=result_format,
                **kwargs,
            )
        else:
            if self.validation_operator_name:
                logger.warning(
                    f'Could not find Validation Operator "{self.validation_operator_name}" when running Checkpoint "{self.name}". Using default action_list_operator.'
                )
            results = self._run_default_validation_operator(
                assets_to_validate=batches_to_validate,
                run_id=run_id,
                evaluation_parameters=evaluation_parameters,
                run_name=run_name,
                run_time=run_time,
                result_format=result_format,
            )
        return results

    def _get_batches_to_validate(self, batches):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        batches_to_validate = []
        for batch in batches:
            batch_kwargs = batch["batch_kwargs"]
            suites = batch["expectation_suite_names"]
            if not suites:
                raise Exception(
                    f"""A batch has no suites associated with it. At least one suite is required.
    - Batch: {json.dumps(batch_kwargs)}
    - Please add at least one suite to Checkpoint {self.name}
"""
                )
            for suite_name in batch["expectation_suite_names"]:
                suite = self.data_context.get_expectation_suite(suite_name)
                batch = self.data_context.get_batch(batch_kwargs, suite)
                batches_to_validate.append(batch)
        return batches_to_validate


class SimpleCheckpoint(Checkpoint):
    _configurator_class = SimpleCheckpointConfigurator

    def __init__(
        self,
        name: str,
        data_context,
        config_version: Optional[Union[(int, float)]] = None,
        template_name: Optional[str] = None,
        run_name_template: Optional[str] = None,
        expectation_suite_name: Optional[str] = None,
        batch_request: Optional[Union[(BatchRequestBase, dict)]] = None,
        action_list: Optional[List[dict]] = None,
        evaluation_parameters: Optional[dict] = None,
        runtime_configuration: Optional[dict] = None,
        validations: Optional[List[dict]] = None,
        profilers: Optional[List[dict]] = None,
        ge_cloud_id: Optional[UUID] = None,
        site_names: Union[(str, List[str])] = "all",
        slack_webhook: Optional[str] = None,
        notify_on: str = "all",
        notify_with: Union[(str, List[str])] = "all",
        expectation_suite_ge_cloud_id: Optional[str] = None,
        **kwargs,
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
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
            action_list=checkpoint_config.action_list,
            evaluation_parameters=checkpoint_config.evaluation_parameters,
            runtime_configuration=checkpoint_config.runtime_configuration,
            validations=validations,
            profilers=checkpoint_config.profilers,
            ge_cloud_id=checkpoint_config.ge_cloud_id,
            expectation_suite_ge_cloud_id=checkpoint_config.expectation_suite_ge_cloud_id,
        )

    def run(
        self,
        template_name: Optional[str] = None,
        run_name_template: Optional[str] = None,
        expectation_suite_name: Optional[str] = None,
        batch_request: Optional[Union[(BatchRequestBase, dict)]] = None,
        action_list: Optional[List[dict]] = None,
        evaluation_parameters: Optional[dict] = None,
        runtime_configuration: Optional[dict] = None,
        validations: Optional[List[dict]] = None,
        profilers: Optional[List[dict]] = None,
        run_id: Optional[Union[(str, RunIdentifier)]] = None,
        run_name: Optional[str] = None,
        run_time: Optional[Union[(str, datetime.datetime)]] = None,
        result_format: Optional[str] = None,
        site_names: Union[(str, List[str])] = "all",
        slack_webhook: Optional[str] = None,
        notify_on: str = "all",
        notify_with: Union[(str, List[str])] = "all",
        expectation_suite_ge_cloud_id: Optional[str] = None,
    ) -> CheckpointResult:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        new_baseline_config = None
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
            action_list=(
                new_baseline_config.action_list if new_baseline_config else action_list
            ),
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
