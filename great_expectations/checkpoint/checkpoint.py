import json
from typing import Union, Optional, List

from great_expectations.core.batch import BatchRequest
from great_expectations.data_context.types.base import (
    CheckpointConfig,
    checkpointConfigSchema,
)
from great_expectations.exceptions import CheckpointError


class LegacyCheckpoint(object):
    def __init__(
        self, data_context, name: str, checkpoint_config: Union[CheckpointConfig, dict],
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
            checkpoint_config = checkpointConfigSchema.load(checkpoint_config)
        self._config = checkpoint_config

    @property
    def data_context(self):
        return self._data_context

    @property
    def name(self):
        return self._name

    @property
    def config(self):
        return self._config

    @property
    def validation_operator_name(self):
        return self.config.validation_operator_name

    @property
    def batches(self):
        return self.config.batches

    def run(
            self,
            run_id,
            evaluation_parameters,
            run_name,
            run_time,
            result_format,
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


class Checkpoint(object):
    def __init__(
        self,
        name: str,
        data_context: "DataContext",
        checkpoint_config: CheckpointConfig
    ):
        self._name = name
        self._data_context = data_context
        self._config = checkpoint_config

    @property
    def name(self):
        return self._name

    @property
    def data_context(self):
        return self._data_context

    @property
    def config(self):
        return self._config

    def run(
            self,
            template: Optional[str] = None,
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
        """



        :param kwargs:
        :return:
        """
        validators = self._get_validators_to_validate(self._validators)

        results = self._data_context.run_validation_operator(
            self._validation_operator_name, assets_to_validate=validators,
        )

        return results

    def _get_validators_to_validate(self, validator_config_list):
        validators = []

        for validator_config in validator_config_list:
            print(validator_config)
            validators.append(self._data_context.get_validator(**validator_config))

        return validators
