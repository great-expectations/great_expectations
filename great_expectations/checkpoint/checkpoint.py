import json
import os
from io import StringIO
from typing import Union

from ruamel.yaml import YAML, YAMLError

from great_expectations.data_context import DataContext
from great_expectations.data_context.types.base import LegacyCheckpointConfig, legacyCheckpointConfigSchema
from great_expectations.exceptions import CheckpointError


class LegacyCheckpoint(object):
    def __init__(
        self,
        data_context: DataContext,
        name: str,
        checkpoint_config: Union[LegacyCheckpointConfig, dict],
    ):
        self._data_context = data_context
        self._name = name

        if not isinstance(checkpoint_config, (LegacyCheckpointConfig, dict)):
            raise CheckpointError(f"Invalid checkpoint_config type - must be LegacyCheckpointConfig or "
                                  f"dict, "
                                  f"instead got {type(checkpoint_config)}")
        elif isinstance(checkpoint_config, dict):
            checkpoint_config = legacyCheckpointConfigSchema.load(checkpoint_config)
        self._checkpoint_config = checkpoint_config

    @property
    def data_context(self):
        return self._data_context

    @property
    def name(self):
        return self._name

    @property
    def checkpoint_config(self):
        return self._checkpoint_config

    @property
    def validation_operator_name(self):
        return self.checkpoint_config.validation_operator_name

    @property
    def batches(self):
        return self.checkpoint_config.batches

    def run(self):

        batches_to_validate = self._get_batches_to_validate(self.batches)

        results = self.data_context.run_validation_operator(
            self.validation_operator_name, assets_to_validate=batches_to_validate,
        )

        return results

    def get_config(self, format="dict"):
        if format == "dict":
            return self.checkpoint_config.to_json_dict()

        elif format == "yaml":
            return self.checkpoint_config.to_yaml_str()

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
        data_context: DataContext,
        name: str,
        validation_operator_name: str,
        validators: list,
    ):
        self._data_context = data_context
        self._name = name

        self._validation_operator_name = validation_operator_name
        self._validators = validators

    def run(self):
        validators = self._get_validators_to_validate(self._validators)

        results = self.data_context.run_validation_operator(
            self.validation_operator_name, assets_to_validate=validators,
        )

        return results

    def _get_validators_to_validate(self, validator_config_list):
        validators = []

        for validator_config in validator_config_list:
            print(validator_config)
            validators.append(self._data_context.get_validator(**validator_config))

        return validators
