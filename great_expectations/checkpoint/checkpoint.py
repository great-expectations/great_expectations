import json
from copy import deepcopy
from typing import List, Optional, Union

from great_expectations.core.batch import BatchRequest
from great_expectations.data_context.types.base import CheckpointConfig
from great_expectations.exceptions import CheckpointError


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
    ):
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
        substituted_config = self.get_substituted_config(runtime_kwargs=runtime_kwargs)
        # TODO: <Alex>Rob to complete</Alex>

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
