import datetime
import logging
from numbers import Number
from typing import Any, Dict, List, Optional, Union

from great_expectations.checkpoint.checkpoint import Checkpoint
from great_expectations.core import RunIdentifier
from great_expectations.core.batch import (
    BatchRequest,
    RuntimeBatchRequest,
    get_batch_request_as_dict,
)
from great_expectations.core.usage_statistics.anonymizers.base import BaseAnonymizer
from great_expectations.core.usage_statistics.anonymizers.types.base import (
    CHECKPOINT_OPTIONAL_TOP_LEVEL_KEYS,
)
from great_expectations.util import deep_filter_properties_iterable

logger = logging.getLogger(__name__)


class CheckpointRunAnonymizer(BaseAnonymizer):
    def anonymize(self, obj: object, *args, **kwargs) -> dict:
        """
        Traverse the entire Checkpoint configuration structure (as per its formal, validated Marshmallow schema) and
        anonymize every field that can be customized by a user (public fields are recorded as their original names).
        """
        assert isinstance(
            obj, Checkpoint
        ), "CheckpointRunAnonymizer can only handle objects of type Checkpoint"

        attribute_name: str
        attribute_value: Optional[Union[str, dict]]
        validation_obj: dict

        checkpoint_optional_top_level_keys: List[str] = []

        name: Optional[str] = kwargs.get("name")
        anonymized_name: Optional[str] = self.anonymize_string(name)

        config_version: Optional[Union[Number, str]] = kwargs.get("config_version")
        if config_version is None:
            config_version = 1.0

        template_name: Optional[str] = kwargs.get("template_name")
        anonymized_template_name: Optional[str] = self.anonymize_string(template_name)

        run_name_template: Optional[str] = kwargs.get("run_name_template")
        anonymized_run_name_template: Optional[str] = self.anonymize_string(
            run_name_template
        )

        expectation_suite_name: Optional[str] = kwargs.get("expectation_suite_name")
        anonymized_expectation_suite_name: Optional[str] = self.anonymize_string(
            expectation_suite_name
        )

        batch_request: Optional[
            Union[BatchRequest, RuntimeBatchRequest, dict]
        ] = kwargs.get("batch_request")
        if batch_request is None:
            batch_request = {}

        anonymized_batch_request: Optional[
            Dict[str, List[str]]
        ] = self.anonymize_batch_request(*(), **batch_request)

        action_list: Optional[List[dict]] = kwargs.get("action_list")
        anonymized_action_list: Optional[List[dict]] = None
        if action_list:
            # noinspection PyBroadException
            try:
                anonymized_action_list = [
                    self._anonymize_action_info(
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

                validation_batch_request = get_batch_request_as_dict(
                    batch_request=validation_batch_request
                )

                anonymized_validation_batch_request: Optional[
                    Optional[Dict[str, List[str]]]
                ] = self.anonymize_batch_request(*(), **validation_batch_request)

                validation_expectation_suite_name: Optional[str] = validation_obj.get(
                    "expectation_suite_name"
                )
                anonymized_validation_expectation_suite_name: Optional[
                    str
                ] = self.anonymize_string(validation_expectation_suite_name)

                validation_action_list: Optional[List[dict]] = validation_obj.get(
                    "action_list"
                )
                anonymized_validation_action_list: Optional[List[dict]] = None
                if validation_action_list:
                    # noinspection PyBroadException
                    try:
                        anonymized_validation_action_list = [
                            self._anonymize_action_info(
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
            anonymized_run_id = self.anonymize_string(str(run_id))

        run_name: Optional[str] = kwargs.get("run_name")
        anonymized_run_name: Optional[str]
        if run_name is None:
            anonymized_run_name = None
        else:
            anonymized_run_name = self.anonymize_string(run_name)

        run_time: Optional[Union[str, datetime.datetime]] = kwargs.get("run_time")
        anonymized_run_time: Optional[str]
        if run_time is None:
            anonymized_run_time = None
        else:
            anonymized_run_time = self.anonymize_string(str(run_time))

        expectation_suite_ge_cloud_id: Optional[str] = kwargs.get(
            "expectation_suite_ge_cloud_id"
        )
        anonymized_expectation_suite_ge_cloud_id: Optional[str]
        if expectation_suite_ge_cloud_id is None:
            anonymized_expectation_suite_ge_cloud_id = None
        else:
            anonymized_expectation_suite_ge_cloud_id = self.anonymize_string(
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

    @staticmethod
    def can_handle(obj: object, *args, **kwargs) -> bool:
        return isinstance(obj, Checkpoint)
