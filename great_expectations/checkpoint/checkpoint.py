import copy
import json
import logging
import os
from copy import deepcopy
from datetime import datetime
from typing import Dict, List, Optional, Union, Any

from great_expectations.checkpoint.configurator import SimpleCheckpointConfigurator
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.core import RunIdentifier
from great_expectations.core.batch import BatchRequest
from great_expectations.core.util import (
    get_datetime_string_from_strftime_format,
    nested_update,
)
from great_expectations.data_context.types.base import CheckpointConfig
from great_expectations.data_context.util import (
    instantiate_class_from_config,
    substitute_all_config_variables,
)
from great_expectations.exceptions import CheckpointError
from great_expectations.validation_operators import ActionListValidationOperator
from great_expectations.validation_operators.types.validation_operator_result import (
    ValidationOperatorResult,
)
from great_expectations.validator.validator import Validator

logger = logging.getLogger(__name__)


class Checkpoint:
    def __init__(
        self,
        name: str,
        data_context,
        config_version: Optional[Union[int, float]] = None,
        template_name: Optional[str] = None,
        module_name: Optional[str] = None,
        class_name: Optional[str] = None,
        configurator: Optional[Any] = None,
        run_name_template: Optional[str] = None,
        expectation_suite_name: Optional[str] = None,
        batch_request: Optional[Union[BatchRequest, dict]] = None,
        action_list: Optional[List[dict]] = None,
        evaluation_parameters: Optional[dict] = None,
        runtime_configuration: Optional[dict] = None,
        validations: Optional[List[dict]] = None,
        profilers: Optional[List[dict]] = None,
        validation_operator_name: Optional[str] = None,
        batches: Optional[List[dict]] = None,
        **kwargs
    ):
        self._name = name
        # Note the gross typechecking to avoid a circular import
        if "DataContext" not in str(type(data_context)):
            raise TypeError("A checkpoint requires a valid DataContext")
        self._data_context = data_context

        if configurator:
            configurator_obj = configurator(
                name=name,
                data_context=data_context,
                config_version=config_version,
                template_name=template_name,
                class_name=class_name,
                module_name=module_name,
                run_name_template=run_name_template,
                expectation_suite_name=expectation_suite_name,
                batch_request=batch_request,
                action_list=action_list,
                evaluation_parameters=evaluation_parameters,
                runtime_configuration=runtime_configuration,
                validations=validations,
                profilers=profilers,
                # Next two fields are for LegacyCheckpoint configuration
                validation_operator_name=validation_operator_name,
                batches=batches,
                **kwargs
            )
            checkpoint_config: CheckpointConfig = configurator_obj.build()
        else:
            assert len(kwargs) == 0, f"Invalid arguments: {list(kwargs.keys())}"
            checkpoint_config: CheckpointConfig = CheckpointConfig(
                **{
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
                    # Next two fields are for LegacyCheckpoint configuration
                    "validation_operator_name": validation_operator_name,
                    "batches": batches,
                }
            )
        self._config = checkpoint_config

        self._substituted_config = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def data_context(self):
        return self._data_context

    @property
    def config(self) -> CheckpointConfig:
        return self._config

    @property
    def action_list(self) -> List[Dict]:
        return self._config.action_list

    # TODO: (Rob) should we type the big validation dicts for better validation/prevent duplication
    def get_substituted_config(
        self,
        config: Optional[Union[CheckpointConfig, dict]] = None,
        runtime_kwargs: Optional[dict] = None,
    ) -> CheckpointConfig:
        runtime_kwargs = runtime_kwargs or {}
        if config is None:
            config = self.config
        if isinstance(config, dict):
            config = CheckpointConfig(**config)

        if (
            self._substituted_config is not None
            and not runtime_kwargs.get("template_name")
            and not config.template_name
        ):
            substituted_config = deepcopy(self._substituted_config)
            if any(runtime_kwargs.values()):
                substituted_config.update(runtime_kwargs=runtime_kwargs)
        else:
            template_name = runtime_kwargs.get("template_name") or config.template_name

            if not template_name:
                substituted_config = copy.deepcopy(config)
                if any(runtime_kwargs.values()):
                    substituted_config.update(runtime_kwargs=runtime_kwargs)

                self._substituted_config = substituted_config
            else:
                checkpoint = self.data_context.get_checkpoint(name=template_name)
                template_config = checkpoint.config

                if template_config.config_version != config.config_version:
                    raise CheckpointError(
                        f"Invalid template '{template_name}' (ver. {template_config.config_version}) for Checkpoint "
                        f"'{config}' (ver. {config.config_version}. Checkpoints can only use templates with the same config_version."
                    )

                if template_config.template_name is not None:
                    substituted_config = self.get_substituted_config(
                        config=template_config
                    )
                else:
                    substituted_config = template_config

                # merge template with config
                substituted_config.update(
                    other_config=config, runtime_kwargs=runtime_kwargs
                )

                # don't replace _substituted_config if already exists
                if self._substituted_config is None:
                    self._substituted_config = substituted_config
        return self._substitute_config_variables(config=substituted_config)

    def _substitute_config_variables(
        self, config: CheckpointConfig
    ) -> CheckpointConfig:
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

        return CheckpointConfig(
            **substitute_all_config_variables(
                config, substitutions, self.data_context.DOLLAR_SIGN_ESCAPE_STRING
            )
        )

    # TODO: <Alex>ALEX/Rob -- since this method is static, should we move it to a "util" type of a module?</Alex>
    @staticmethod
    def _get_runtime_batch_request(
        substituted_runtime_config: CheckpointConfig,
        validation_batch_request: Optional[dict] = None,
    ) -> BatchRequest:
        if substituted_runtime_config.batch_request is None:
            return (
                validation_batch_request
                if validation_batch_request is None
                else BatchRequest(**validation_batch_request)
            )

        if validation_batch_request is None:
            return BatchRequest(**substituted_runtime_config.batch_request)

        runtime_batch_request_dict: dict = copy.deepcopy(validation_batch_request)
        for key, val in runtime_batch_request_dict.items():
            if (
                val is not None
                and substituted_runtime_config.batch_request.get(key) is not None
            ):
                raise CheckpointError(
                    f'BatchRequest attribute "{key}" was specified in both validation and top-level CheckpointConfig.'
                )
        runtime_batch_request_dict.update(substituted_runtime_config.batch_request)
        return BatchRequest(**runtime_batch_request_dict)

    # TODO: <Alex>ALEX/Rob -- since this method is static, should we move it to a "util" type of a module?</Alex>
    @staticmethod
    def _validate_validation_dict(validation_dict):
        if validation_dict.get("batch_request") is None:
            raise CheckpointError("validation batch_request cannot be None")
        if not validation_dict.get("expectation_suite_name"):
            raise CheckpointError("validation expectation_suite_name must be specified")
        if not validation_dict.get("action_list"):
            raise CheckpointError("validation action_list cannot be empty")

    def _get_substituted_validation_dict(
        self, substituted_runtime_config: CheckpointConfig, validation_dict: dict
    ) -> dict:
        substituted_validation_dict = {
            "batch_request": self._get_runtime_batch_request(
                substituted_runtime_config=substituted_runtime_config,
                validation_batch_request=validation_dict.get("batch_request"),
            ),
            "expectation_suite_name": validation_dict.get("expectation_suite_name")
            or substituted_runtime_config.expectation_suite_name,
            "action_list": CheckpointConfig.get_updated_action_list(
                base_action_list=substituted_runtime_config.action_list,
                other_action_list=validation_dict.get("action_list", {}),
            ),
            "evaluation_parameters": nested_update(
                substituted_runtime_config.evaluation_parameters,
                validation_dict.get("evaluation_parameters", {}),
            ),
            "runtime_configuration": nested_update(
                substituted_runtime_config.runtime_configuration,
                validation_dict.get("runtime_configuration", {}),
            ),
        }
        if validation_dict.get("name") is not None:
            substituted_validation_dict["name"] = validation_dict["name"]
        self._validate_validation_dict(substituted_validation_dict)
        return substituted_validation_dict

    # TODO: Add eval param processing using updated EvaluationParameterParser and parse_evaluation_parameters function
    def run(
        self,
        template_name: Optional[str] = None,
        run_name_template: Optional[str] = None,
        expectation_suite_name: Optional[str] = None,
        batch_request: Optional[Union[BatchRequest, dict]] = None,
        action_list: Optional[List[dict]] = None,
        evaluation_parameters: Optional[dict] = None,
        runtime_configuration: Optional[dict] = None,
        validations: Optional[List[dict]] = None,
        profilers: Optional[List[dict]] = None,
        run_id=None,
        run_name=None,
        run_time=None,
        result_format=None,
        **kwargs,
    ) -> CheckpointResult:
        assert not (run_id and run_name) and not (
            run_id and run_time
        ), "Please provide either a run_id or run_name and/or run_time."

        run_time = run_time or datetime.now()
        runtime_configuration: dict = runtime_configuration or {}
        result_format: Optional[dict] = result_format or runtime_configuration.get(
            "result_format"
        )
        if result_format is None:
            result_format = {"result_format": "SUMMARY"}

        runtime_kwargs = {
            "template_name": template_name,
            "run_name_template": run_name_template,
            "expectation_suite_name": expectation_suite_name,
            "batch_request": batch_request,
            "action_list": action_list,
            "evaluation_parameters": evaluation_parameters,
            "runtime_configuration": runtime_configuration,
            "validations": validations,
            "profilers": profilers,
        }
        substituted_runtime_config: CheckpointConfig = self.get_substituted_config(
            runtime_kwargs=runtime_kwargs
        )
        run_name_template: Optional[str] = substituted_runtime_config.run_name_template
        validations: list = substituted_runtime_config.validations
        run_results = {}

        if run_name is None and run_name_template is not None:
            run_name: str = get_datetime_string_from_strftime_format(
                format_str=run_name_template, datetime_obj=run_time
            )

        run_id = run_id or RunIdentifier(run_name=run_name, run_time=run_time)

        for idx, validation_dict in enumerate(validations):
            try:
                substituted_validation_dict: dict = (
                    self._get_substituted_validation_dict(
                        substituted_runtime_config=substituted_runtime_config,
                        validation_dict=validation_dict,
                    )
                )
                batch_request: BatchRequest = substituted_validation_dict.get(
                    "batch_request"
                )
                expectation_suite_name: str = substituted_validation_dict.get(
                    "expectation_suite_name"
                )
                action_list: list = substituted_validation_dict.get("action_list")

                validator: Validator = self.data_context.get_validator(
                    batch_request=batch_request,
                    expectation_suite_name=expectation_suite_name,
                )
                action_list_validation_operator: ActionListValidationOperator = (
                    ActionListValidationOperator(
                        data_context=self.data_context,
                        action_list=action_list,
                        result_format=result_format,
                        name=f"{self.name}-checkpoint-validation[{idx}]",
                    )
                )
                val_op_run_result: ValidationOperatorResult = (
                    action_list_validation_operator.run(
                        assets_to_validate=[validator],
                        run_id=run_id,
                        evaluation_parameters=substituted_validation_dict.get(
                            "evaluation_parameters"
                        ),
                        result_format=result_format,
                    )
                )
                run_results.update(val_op_run_result.run_results)
            except CheckpointError as e:
                raise CheckpointError(
                    f"Exception occurred while running validation[{idx}] of checkpoint '{self.name}': {e.message}"
                )
        return CheckpointResult(
            run_id=run_id, run_results=run_results, checkpoint_config=self.config
        )

    def self_check(self, pretty_print=True) -> dict:
        # Provide visibility into parameters that Checkpoint was instantiated with.
        report_object: dict = {"config": self.config.to_json_dict()}

        if pretty_print:
            print(f"\nCheckpoint class name: {self.__class__.__name__}")

        validations_present: bool = (
            self.config.validations
            and isinstance(self.config.validations, list)
            and len(self.config.validations) > 0
        )
        action_list: Optional[list] = self.config.action_list
        action_list_present: bool = (
            action_list is not None
            and isinstance(action_list, list)
            and len(action_list) > 0
        ) or (
            validations_present
            and all(
                [
                    (
                        validation.get("action_list")
                        and isinstance(validation["action_list"], list)
                        and len(validation["action_list"]) > 0
                    )
                    for validation in self.config.validations
                ]
            )
        )
        if pretty_print:
            if not validations_present:
                print(
                    f"""Your current Checkpoint configuration has an empty or missing "validations" attribute.  This
means you must either update your checkpoint configuration or provide an appropriate validations
list programmatically (i.e., when your Checkpoint is run).
                    """
                )
            if not action_list_present:
                print(
                    f"""Your current Checkpoint configuration has an empty or missing "action_list" attribute.  This
means you must provide an appropriate validations list programmatically (i.e., when your Checkpoint
is run), with each validation having its own defined "action_list" attribute.
                    """
                )

        return report_object


class LegacyCheckpoint(Checkpoint):
    def __init__(
        self,
        name: str,
        data_context,
        validation_operator_name: Optional[str] = None,
        batches: Optional[List[dict]] = None,
    ):
        super().__init__(
            name=name,
            data_context=data_context,
            validation_operator_name=validation_operator_name,
            batches=batches,
        )

    @property
    def validation_operator_name(self):
        return self.config.validation_operator_name

    @property
    def batches(self):
        return self.config.batches

    def run(
        self,
        run_id=None,
        evaluation_parameters=None,
        run_name=None,
        run_time=None,
        result_format=None,
        **kwargs,
    ):
        batches_to_validate = self._get_batches_to_validate(self.batches)

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

        return results

    def get_config(self, format="dict"):
        if format == "dict":
            return self.config.to_json_dict()

        elif format == "yaml":
            return self.config.to_yaml_str()

        else:
            raise ValueError(f"Unknown format {format} in LegacyCheckpoint.get_config.")

    def _get_batches_to_validate(self, batches):
        batches_to_validate = []
        for batch in batches:

            batch_kwargs = batch["batch_kwargs"]
            suites = batch["expectation_suite_names"]

            if not suites:
                raise Exception(
                    f"""A batch has no suites associated with it. At least one suite is required.
    - Batch: {json.dumps(batch_kwargs)}
    - Please add at least one suite to checkpoint {self.name}
"""
                )

            for suite_name in batch["expectation_suite_names"]:
                suite = self.data_context.get_expectation_suite(suite_name)
                batch = self.data_context.get_batch(batch_kwargs, suite)

                batches_to_validate.append(batch)

        return batches_to_validate


class SimpleCheckpoint:
    def __init__(
        self,
        name: str,
        data_context,
        config_version: Optional[Union[int, float]] = None,
        template_name: Optional[str] = None,
        module_name: Optional[str] = None,
        class_name: Optional[str] = None,
        configurator: Optional[dict] = SimpleCheckpointConfigurator,
        run_name_template: Optional[str] = None,
        expectation_suite_name: Optional[str] = None,
        batch_request: Optional[Union[BatchRequest, dict]] = None,
        action_list: Optional[List[dict]] = None,
        evaluation_parameters: Optional[dict] = None,
        runtime_configuration: Optional[dict] = None,
        validations: Optional[List[dict]] = None,
        profilers: Optional[List[dict]] = None,
        validation_operator_name: Optional[str] = None,
        batches: Optional[List[dict]] = None,
        # the following are arguments used by SimpleCheckpointConfigurator
        site_names: Optional[Union[str, List[str]]] = "all",
        slack_webhook: Optional[str] = None,
        notify_on: Optional[str] = "all",
        notify_with: Optional[Union[str, List[str]]] = "all",
    ):
        super().__init__(
            name=name,
            data_context=data_context,
            config_version=config_version,
            template_name=template_name,
            module_name=module_name,
            class_name=class_name,
            configurator=configurator,
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
            site_names=site_names,
            slack_webhook=slack_webhook,
            notify_on=notify_on,
            notify_with=notify_with,
        )


# TODO Options in no order:
#  1. slim version of config must be a valid Checkpoint config perhaps by:
#     templates? a folder of yml files?
#  2. configuration builder as a parameter to Checkpoint()
#  3. subclass with defaults
#  - move parameter validation into base Checkpoint?
#  - other ideas?
# Key requirement: simpler appearing configuration
# pattern in data context config with default enums
#
