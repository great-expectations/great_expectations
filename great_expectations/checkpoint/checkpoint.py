import json
from copy import deepcopy
from datetime import datetime
from typing import Any, List, Optional, Union

from great_expectations.core.batch import BatchRequest
from great_expectations.core.util import nested_update
from great_expectations.data_context.types.base import CheckpointConfig
from great_expectations.exceptions import CheckpointError
from great_expectations.validation_operators import ActionListValidationOperator
from great_expectations.validation_operators.types.validation_operator_result import (
    ValidationOperatorResult,
)
from great_expectations.validator.validator import Validator


class Checkpoint:
    def __init__(
        self,
        name: str,
        data_context,
        checkpoint_config: CheckpointConfig,
    ):
        self._data_context = data_context
        self._name = name

        if not isinstance(checkpoint_config, (CheckpointConfig, dict)):
            raise CheckpointError(
                f"Invalid checkpoint_config type - must be CheckpointConfig or "
                f"dict, "
                f"instead got {type(checkpoint_config)}"
            )
        elif isinstance(checkpoint_config, dict):
            checkpoint_config: CheckpointConfig = CheckpointConfig(**checkpoint_config)
        self._config = checkpoint_config

        self._substituted_config = None

    @property
    def name(self):
        return self._name

    @property
    def data_context(self):
        return self._data_context

    @property
    def config(self):
        return self._config

    # TODO: (Rob) should we type the big validation dicts for better validation/prevent duplication
    def get_substituted_config(
        self,
        config: Optional[Union[CheckpointConfig, dict]] = None,
        runtime_kwargs: Optional[dict] = None,
    ):
        runtime_kwargs = runtime_kwargs or {}

        if self._substituted_config is not None and not runtime_kwargs.get(
            "template_name"
        ):
            substituted_config = deepcopy(self._substituted_config)
            if any(runtime_kwargs.values()):
                substituted_config.update(runtime_kwargs=runtime_kwargs)

            return substituted_config
        else:
            if config is None:
                config = self.config
            if isinstance(config, dict):
                config = CheckpointConfig(**config)
            template_name = runtime_kwargs.get("template_name") or config.template_name

            if not template_name:
                substituted_config = deepcopy(config)
                if any(runtime_kwargs.values()):
                    substituted_config.update(runtime_kwargs=runtime_kwargs)

                self._substituted_config = substituted_config
                return substituted_config
            else:
                template_config = self.data_context.get_checkpoint(
                    checkpoint_name=template_name, return_config=True
                )

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
                return substituted_config

    def _get_runtime_batch_request(
        self,
        substituted_runtime_config: CheckpointConfig,
        validation_batch_request: Optional[Union[dict, BatchRequest]] = None,
    ) -> BatchRequest:
        if substituted_runtime_config.batch_request is None:
            return (
                validation_batch_request
                if isinstance(validation_batch_request, (BatchRequest, type(None)))
                else BatchRequest(**validation_batch_request)
            )

        # get batch requests as dicts; since get_json_dict() doesn't include batch_data, re-add
        base_batch_request_batch_data: Any = (
            substituted_runtime_config.batch_request.batch_data
        )
        base_batch_request_dict: dict = (
            substituted_runtime_config.batch_request.get_json_dict()
        )
        base_batch_request_dict["batch_data"] = base_batch_request_batch_data

        if isinstance(validation_batch_request, BatchRequest):
            validation_batch_request_batch_data: Any = (
                validation_batch_request.batch_data
            )
            validation_batch_request: dict = validation_batch_request.get_json_dict()
            validation_batch_request["batch_data"] = validation_batch_request_batch_data
        elif isinstance(validation_batch_request, None):
            return BatchRequest(**base_batch_request_dict)

        runtime_batch_request_dict = deepcopy(validation_batch_request)
        for key, val in runtime_batch_request_dict.items():
            if val is not None and base_batch_request_dict.get(key) is not None:
                raise CheckpointError(
                    f"BatchRequest attribute '{key}' was specified in both validation and top-level CheckpointConfig."
                )
            runtime_batch_request_dict[key] = base_batch_request_dict[key]
        return BatchRequest(**runtime_batch_request_dict)

    def _validate_validation_dict(self, validation_dict):
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

    def get_run_name_from_template(self, run_name_template: str):
        now = datetime.now()
        return now.strftime(run_name_template)

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
    ) -> List[ValidationOperatorResult]:
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
        results = []

        if run_name is None and run_name_template is not None:
            run_name: str = self.get_run_name_from_template(
                run_name_template=run_name_template
            )

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
                action_list_validation_operator: ActionListValidationOperator = ActionListValidationOperator(
                    data_context=self.data_context,
                    action_list=action_list,
                    result_format=result_format,
                    name=f"{self.name}-checkpoint-validation[{idx}]",
                )
                run_result: ValidationOperatorResult = (
                    action_list_validation_operator.run(
                        assets_to_validate=[validator],
                        run_id=run_id,
                        evaluation_parameters=substituted_validation_dict.get(
                            "evaluation_parameters"
                        ),
                        run_name=run_name,
                        run_time=run_time,
                        result_format=result_format,
                    )
                )
                results.append(run_result)
            except CheckpointError as e:
                raise CheckpointError(
                    f"Exception occurred while running validation[{idx}] of checkpoint '{self.name}': {e.message}"
                )
            except Exception as e:
                raise e
        return results

    def self_check(self, pretty_print=True) -> dict:
        # Provide visibility into parameters that Checkpoint was instantiated with.
        report_object: dict = {"config": self.config.to_json_dict()}

        if pretty_print:
            print(f"Checkpoint: {self.__class__.__name__}")

        return report_object


class LegacyCheckpoint(Checkpoint):
    def __init__(
        self,
        name: str,
        data_context,
        checkpoint_config: Union[CheckpointConfig, dict],
    ):
        super().__init__(
            name=name,
            data_context=data_context,
            checkpoint_config=checkpoint_config,
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
