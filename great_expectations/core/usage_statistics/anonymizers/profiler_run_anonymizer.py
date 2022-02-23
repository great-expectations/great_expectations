import datetime
import logging
from numbers import Number
from typing import Any, Dict, List, Optional, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.checkpoint.util import (
    get_substituted_validation_dict,
    get_validations_with_batch_request_as_dict,
)
from great_expectations.core import RunIdentifier
from great_expectations.core.batch import (
    BatchRequest,
    RuntimeBatchRequest,
    get_batch_request_as_dict,
)
from great_expectations.core.usage_statistics.anonymizers.anonymizer import Anonymizer
from great_expectations.core.usage_statistics.anonymizers.batch_request_anonymizer import (
    BatchRequestAnonymizer,
)
from great_expectations.core.util import get_datetime_string_from_strftime_format
from great_expectations.util import deep_filter_properties_iterable

logger = logging.getLogger(__name__)


class ProfilerRunAnonymizer(Anonymizer):
    def __init__(self, salt: Optional[str] = None) -> None:
        super().__init__(salt=salt)

        self._salt = salt

    def anonymize_profiler_run(
        self, *args: List[Any], **kwargs: dict
    ) -> Dict[str, List[str]]:
        """
        Traverse the entire RuleBasedProfiler configuration structure (as per its formal, validated Marshmallow schema) and
        anonymize every field that can be customized by a user (public fields are recorded as their original names).
        """
        batch_request_anonymizer: BatchRequestAnonymizer = BatchRequestAnonymizer(
            self._salt
        )

        name: Optional[str] = kwargs.get("name")
        anonymized_name: Optional[str] = self.anonymize(name)

        config_version: Union[Number, str] = kwargs.get("config_version", 1.0)
        rules: Dict[str, dict] = kwargs.get("rules", {})

        anonymized_rules: List[dict] = self._anonymize_rules(rules)

        anonymized_profiler_run_properties_dict: Dict[str, List[str]] = {
            "anonymized_name": anonymized_name,
            "config_version": config_version,
            "anonymized_rules": anonymized_rules,
        }

        deep_filter_properties_iterable(
            properties=anonymized_checkpoint_run_properties_dict,
            clean_falsy=True,
            inplace=True,
        )

        return anonymized_checkpoint_run_properties_dict

    def _anonymize_rules(self, rules: Dict[str, dict]) -> List[dict]:
        anonymized_rules: List[dict] = []

        for name, rule in rules.items():
            anonymized_rule: dict = self._anonymize_rule(name, rule)
            anonymized_rules.append(anonymized_rule)

        return anonymized_rules

    def _anonymize_rule(self, name: str, rule: dict) -> dict:
        anonymized_rule: dict = {}
        anonymized_rule["anonymized_name"] = self.anonymize(name)

        domain_builder: Optional[dict] = rule.get("domain_builder")
        if domain_builder is not None:
            anonymized_rule[
                "anonymized_domain_builder"
            ] = self._anonymize_domain_builder(domain_builder)

        parameter_builders: List[dict] = rule.get("parameter_builders", [])
        anonymized_rule[
            "anonymized_parameter_builders"
        ] = self._anonymize_parameter_builders(parameter_builders)

        expectation_configuration_builders: List[dict] = rule.get(
            "expectation_configuration_builders", []
        )
        anonymized_rule[
            "anonymized_expectation_configuration_builders"
        ] = self._anonymize_expectation_configuration_builders(
            expectation_configuration_builders
        )

        return anonymized_rule

    def _anonymize_domain_builder(self, domain_builder: dict) -> dict:
        pass

    def _anonymize_parameter_builders(
        self, parameter_builders: List[dict]
    ) -> List[dict]:
        pass

    def _anonymize_parameter_builder(self, parameter_builder: dict) -> dict:
        pass

    def _anonymize_expectation_configuration_builders(
        self, expectation_configuration_builders: List[dict]
    ) -> List[dict]:
        pass

    def _anonymize_expectation_configuration_builder(
        self, expectation_configuration_builder: dict
    ) -> dict:
        pass

    # noinspection PyUnusedLocal,PyUnresolvedReferences
    @staticmethod
    def resolve_config_using_acceptable_arguments(
        checkpoint: "Checkpoint",  # noqa: F821
        template_name: Optional[str] = None,
        run_name_template: Optional[str] = None,
        expectation_suite_name: Optional[str] = None,
        batch_request: Optional[Union[BatchRequest, RuntimeBatchRequest, dict]] = None,
        action_list: Optional[List[dict]] = None,
        evaluation_parameters: Optional[dict] = None,
        runtime_configuration: Optional[dict] = None,
        validations: Optional[List[dict]] = None,
        profilers: Optional[List[dict]] = None,
        run_id: Optional[Union[str, RunIdentifier]] = None,
        run_name: Optional[str] = None,
        run_time: Optional[Union[str, datetime.datetime]] = None,
        result_format: Optional[Union[str, dict]] = None,
        expectation_suite_ge_cloud_id: Optional[str] = None,
    ) -> dict:
        """
        This method reconciles the Checkpoint configuration (e.g., obtained from the Checkpoint store) with dynamically
        supplied arguments in order to obtain that Checkpoint specification that is ready for running validation on it.
        This procedure is necessecitated by the fact that the Checkpoint configuration is hierarchical in its form,
        which was established for the purposes of making the specification of different Checkpoint capabilities easy.
        In particular, entities, such as BatchRequest, expectation_suite_name, and action_list, can be specified at the
        top Checkpoint level with the suitable ovverrides provided at lower levels (e.g., in the validations section).
        Reconciling and normalizing the Checkpoint configuration is essential for usage statistics, because the exact
        values of the entities in their formally validated form (e.g., BatchRequest) is the required level of detail.
        """
        assert not (run_id and run_name) and not (
            run_id and run_time
        ), "Please provide either a run_id or run_name and/or run_time."

        run_time = run_time or datetime.datetime.now()
        runtime_configuration = runtime_configuration or {}

        batch_request = get_batch_request_as_dict(batch_request=batch_request)
        validations = get_validations_with_batch_request_as_dict(
            validations=validations
        )

        runtime_kwargs: dict = {
            "template_name": template_name,
            "run_name_template": run_name_template,
            "expectation_suite_name": expectation_suite_name,
            "batch_request": batch_request,
            "action_list": action_list,
            "evaluation_parameters": evaluation_parameters,
            "runtime_configuration": runtime_configuration,
            "validations": validations,
            "profilers": profilers,
            "expectation_suite_ge_cloud_id": expectation_suite_ge_cloud_id,
        }
        substituted_runtime_config: dict = checkpoint.get_substituted_config(
            runtime_kwargs=runtime_kwargs
        )
        run_name_template = substituted_runtime_config.get("run_name_template")
        validations = substituted_runtime_config.get("validations") or []
        batch_request = substituted_runtime_config.get("batch_request")
        if len(validations) == 0 and not batch_request:
            raise ge_exceptions.CheckpointError(
                f'Checkpoint "{checkpoint.name}" must contain either a batch_request or validations.'
            )

        if run_name is None and run_name_template is not None:
            run_name = get_datetime_string_from_strftime_format(
                format_str=run_name_template, datetime_obj=run_time
            )

        run_id = run_id or RunIdentifier(run_name=run_name, run_time=run_time)

        validation_dict: dict

        for validation_dict in validations:
            substituted_validation_dict: dict = get_substituted_validation_dict(
                substituted_runtime_config=substituted_runtime_config,
                validation_dict=validation_dict,
            )
            validation_batch_request: Union[
                BatchRequest, RuntimeBatchRequest
            ] = substituted_validation_dict.get("batch_request")
            validation_dict["batch_request"] = validation_batch_request
            validation_expectation_suite_name: str = substituted_validation_dict.get(
                "expectation_suite_name"
            )
            validation_dict[
                "expectation_suite_name"
            ] = validation_expectation_suite_name
            validation_expectation_suite_ge_cloud_id: str = (
                substituted_validation_dict.get("expectation_suite_ge_cloud_id")
            )
            validation_dict[
                "expectation_suite_ge_cloud_id"
            ] = validation_expectation_suite_ge_cloud_id
            validation_action_list: list = substituted_validation_dict.get(
                "action_list"
            )
            validation_dict["action_list"] = validation_action_list

        return substituted_runtime_config
