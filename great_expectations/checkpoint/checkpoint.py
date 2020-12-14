import json
import os
from io import StringIO

from ruamel.yaml import YAML, YAMLError

from great_expectations.data_context import DataContext
from great_expectations.data_context.util import file_relative_path


class LegacyCheckpoint(object):
    def __init__(self,
        data_context,
        name,
        batches,
        validation_operator_name="action_list_operator",
        template={}
    ):
        self.data_context = data_context
        self.name = name

        self.validation_operator_name = validation_operator_name
        self.batches = batches

        self.template = template

    def run(self):

        batches_to_validate = self._get_batches_to_validate(self.batches)

        results = self.data_context.run_validation_operator(
            self.validation_operator_name,
            assets_to_validate=batches_to_validate,
        )

        return results

    def get_config(self, format="dict"):
        config = {
            "validation_operator_name" : self.validation_operator_name,
            "batches" : self.batches,
        }

        if format == "dict":
            return config

        elif format == "yaml":
            self.template["validation_operator_name"] = config["validation_operator_name"]

            if not "batches" in self.template:
                self.template["batches"] = []

            for i, batch in enumerate(config["batches"]):
                #NOTE Abe 2020/10/03: This approach could end up with comments attaching to the wrong batches.
                self.template["batches"][i] = config["batches"][i]

            yaml = YAML()
            yaml.indent(mapping=2, sequence=4, offset=2)

            string_stream = StringIO()
            yaml.dump(self.template, string_stream)
            output_str = string_stream.getvalue()
            string_stream.close()

            return output_str

        else:
            raise ValueError(f"Unknown format {format} in LegacyCheckpoint.get_config.")

    def _get_batches_to_validate(self, batches):
        batches_to_validate = []
        for batch in batches:

            batch_kwargs = batch["batch_kwargs"]
            suites = batch["expectation_suite_names"]

            if not suites:
                raise Exception(f"""A batch has no suites associated with it. At least one suite is required.
    - Batch: {json.dumps(batch_kwargs)}
    - Please add at least one suite to checkpoint {self.name}
""")

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
            self.validation_operator_name,
            assets_to_validate=validators,
        )

        return results

    def _get_validators_to_validate(self, validator_config_list):
        validators = []

        for validator_config in validator_config_list:
            print(validator_config)
            validators.append(
                self._data_context.get_validator(**validator_config)
            )

        return validators
