import datetime
import logging
from numbers import Number
from typing import Any, Dict, List, Optional, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.checkpoint.util import (
    get_batch_request_as_dict,
    get_substituted_validation_dict,
    get_validations_with_batch_request_as_dict,
)
from great_expectations.core import RunIdentifier
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.core.usage_statistics.anonymizers.action_anonymizer import (
    ActionAnonymizer,
)
from great_expectations.core.usage_statistics.anonymizers.anonymizer import Anonymizer
from great_expectations.core.usage_statistics.anonymizers.batch_request_anonymizer import (
    BatchRequestAnonymizer,
)
from great_expectations.core.usage_statistics.anonymizers.types.base import (
    CHECKPOINT_OPTIONAL_TOP_LEVEL_KEYS,
)
from great_expectations.core.util import get_datetime_string_from_strftime_format
from great_expectations.util import deep_filter_properties_iterable

logger = logging.getLogger(__name__)


class CheckpointRunAnonymizer(Anonymizer):
    def __init__(self, salt=None):
        super().__init__(salt=salt)

        self._salt = salt

    # noinspection PyUnusedLocal
    def anonymize_checkpoint_run(self, *args, **kwargs) -> Dict[str, List[str]]:
        """
        Traverse the entire Checkpoint configuration structure (as per its formal, validated Marshmallow schema) and
        anonymize every field that can be customized by a user (public fields are recorded as their original names).
        """
        batch_request_anonymizer: BatchRequestAnonymizer = BatchRequestAnonymizer(
            self._salt
        )
        action_anonymizer: ActionAnonymizer = ActionAnonymizer(self._salt)

        attribute_name: str
        attribute_value: Optional[Union[str, dict]]
        validation_obj: dict
        action_config_dict: dict
        action_name: str
        action_obj: dict

        checkpoint_optional_top_level_keys: List[str] = []

        name: Optional[str] = kwargs.get("name")
        anonymized_name: Optional[str] = self.anonymize(name)

        config_version: Optional[Number] = kwargs.get("config_version")
        config_version: Optional[str]
        if config_version is None:
            config_version = 1

        template_name: Optional[str] = kwargs.get("template_name")
        anonymized_template_name: Optional[str] = self.anonymize(template_name)

        run_name_template: Optional[str] = kwargs.get("run_name_template")
        anonymized_run_name_template: Optional[str] = self.anonymize(run_name_template)

        expectation_suite_name: Optional[str] = kwargs.get("expectation_suite_name")
        anonymized_expectation_suite_name: Optional[str] = self.anonymize(
            expectation_suite_name
        )

        batch_request: Optional[
            Union[BatchRequest, RuntimeBatchRequest, dict]
        ] = kwargs.get("batch_request")
        if batch_request is None:
            batch_request = {}

        anonymized_batch_request: Optional[
            Dict[str, List[str]]
        ] = batch_request_anonymizer.anonymize_batch_request(*(), **batch_request)

        action_list: Optional[List[dict]] = kwargs.get("action_list")
        anonymized_action_list: Optional[List[dict]] = None
        if action_list:
            # noinspection PyBroadException
            try:
                anonymized_action_list = [
                    action_anonymizer.anonymize_action_info(
                        action_name=action_config_dict["name"],
                        action_config=action_config_dict["action"],
                    )
                    for action_config_dict in action_list
                ]
            except Exception:
                logger.debug(
                    "anonymize_checkpoint_run: Unable to create anonymized_action_list payload field"
                )

        validations: Optional[List[dict]] = kwargs.get("validations")
        anonymized_validations: Optional[List[dict]] = []
        if validations:
            for validation_obj in validations:
                validation_batch_request: Optional[
                    Union[BatchRequest, RuntimeBatchRequest, dict]
                ] = validation_obj.get("batch_request")
                if validation_batch_request is None:
                    validation_batch_request = {}

                if isinstance(
                    validation_batch_request, (BatchRequest, RuntimeBatchRequest)
                ):
                    validation_batch_request = validation_batch_request.to_dict()

                anonymized_validation_batch_request: Optional[
                    Optional[Dict[str, List[str]]]
                ] = batch_request_anonymizer.anonymize_batch_request(
                    *(), **validation_batch_request
                )

                validation_expectation_suite_name: Optional[str] = validation_obj.get(
                    "expectation_suite_name"
                )
                anonymized_validation_expectation_suite_name: Optional[
                    str
                ] = self.anonymize(validation_expectation_suite_name)

                validation_action_list: Optional[List[dict]] = validation_obj.get(
                    "action_list"
                )
                anonymized_validation_action_list: Optional[List[dict]] = None
                if validation_action_list:
                    # noinspection PyBroadException
                    try:
                        anonymized_validation_action_list = [
                            action_anonymizer.anonymize_action_info(
                                action_name=action_config_dict["name"],
                                action_config=action_config_dict["action"],
                            )
                            for action_config_dict in validation_action_list
                        ]
                    except Exception:
                        logger.debug(
                            "anonymize_checkpoint_run: Unable to create anonymized_validation_action_list payload field"
                        )

                anonymized_validation: Dict[
                    str, Union[str, Dict[str, Any], List[Dict[str, Any]]]
                ] = {}

                if anonymized_validation_batch_request:
                    anonymized_validation[
                        "anonymized_batch_request"
                    ] = anonymized_validation_batch_request

                if anonymized_validation_expectation_suite_name:
                    anonymized_validation[
                        "anonymized_expectation_suite_name"
                    ] = anonymized_validation_expectation_suite_name

                if anonymized_validation_action_list:
                    anonymized_validation[
                        "anonymized_action_list"
                    ] = anonymized_validation_action_list

                anonymized_validation: Dict[str, Dict[str, Any]] = {
                    "anonymized_batch_request": anonymized_validation_batch_request,
                    "anonymized_expectation_suite_name": anonymized_validation_expectation_suite_name,
                    "anonymized_action_list": anonymized_validation_action_list,
                }

                anonymized_validations.append(anonymized_validation)

        run_id: Optional[Union[str, RunIdentifier]] = kwargs.get("run_id")
        anonymized_run_id: Optional[Union[str, RunIdentifier]]
        if run_id is None:
            anonymized_run_id = None
        else:
            anonymized_run_id = self.anonymize(str(run_id))

        run_name: Optional[str] = kwargs.get("run_name")
        anonymized_run_name: Optional[str]
        if run_name is None:
            anonymized_run_name = None
        else:
            anonymized_run_name = self.anonymize(run_name)

        run_time: Optional[Union[str, datetime.datetime]] = kwargs.get("run_time")
        anonymized_run_time: Optional[str]
        if run_time is None:
            anonymized_run_time = None
        else:
            anonymized_run_time = self.anonymize(str(run_time))

        expectation_suite_ge_cloud_id: Optional[str] = kwargs.get(
            "expectation_suite_ge_cloud_id"
        )
        anonymized_expectation_suite_ge_cloud_id: Optional[str]
        if expectation_suite_ge_cloud_id is None:
            anonymized_expectation_suite_ge_cloud_id = None
        else:
            anonymized_expectation_suite_ge_cloud_id = self.anonymize(
                str(expectation_suite_ge_cloud_id)
            )

        for attribute_name in sorted(CHECKPOINT_OPTIONAL_TOP_LEVEL_KEYS):
            attribute_value = kwargs.get(attribute_name)
            if attribute_value:
                checkpoint_optional_top_level_keys.append(attribute_name)

        anonymized_checkpoint_run_properties_dict: Dict[str, List[str]] = {
            "anonymized_name": anonymized_name,
            "config_version": config_version,
            "anonymized_template_name": anonymized_template_name,
            "anonymized_run_name_template": anonymized_run_name_template,
            "anonymized_expectation_suite_name": anonymized_expectation_suite_name,
            "anonymized_batch_request": anonymized_batch_request,
            "anonymized_action_list": anonymized_action_list,
            "anonymized_validations": anonymized_validations,
            "anonymized_run_id": anonymized_run_id,
            "anonymized_run_name": anonymized_run_name,
            "anonymized_run_time": anonymized_run_time,
            "anonymized_expectation_suite_ge_cloud_id": anonymized_expectation_suite_ge_cloud_id,
            "checkpoint_optional_top_level_keys": checkpoint_optional_top_level_keys,
        }

        deep_filter_properties_iterable(
            properties=anonymized_checkpoint_run_properties_dict,
            clean_falsy=True,
            inplace=True,
        )

        return anonymized_checkpoint_run_properties_dict

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
