import datetime
import logging
from numbers import Number
from typing import Any, Dict, List, Optional, Union

from ruamel.yaml.comments import CommentedMap

from great_expectations.core import RunIdentifier  # noqa: TCH001
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


class CheckpointAnonymizer(BaseAnonymizer):
    def __init__(
        self,
        aggregate_anonymizer: "Anonymizer",  # noqa: F821
        salt: Optional[str] = None,
    ) -> None:
        super().__init__(salt=salt)

        self._aggregate_anonymizer = aggregate_anonymizer

    def anonymize(self, obj: Optional[object] = None, **kwargs) -> Any:
        if "config" in kwargs:
            return self._anonymize_checkpoint_config(**kwargs)

        return self._anonymize_checkpoint_run(obj=obj, **kwargs)

    def _anonymize_checkpoint_config(self, name: str, config: dict) -> dict:
        """Anonymize Checkpoint objs from the 'great_expectations.checkpoint' module.

        Args:
            name (str): The name of the checkpoint.
            config (dict): The dictionary configuration corresponding to the checkpoint.

        Returns:
            An anonymized dictionary payload that obfuscates user-specific details.
        """
        anonymized_info_dict: dict = {
            "anonymized_name": self._anonymize_string(name),
        }

        # Roundtrip through schema validation to remove any illegal fields add/or restore any missing fields.
        from great_expectations.data_context.types.base import checkpointConfigSchema

        checkpoint_config: dict = checkpointConfigSchema.load(CommentedMap(**config))
        checkpoint_config_dict: dict = checkpointConfigSchema.dump(checkpoint_config)

        self._anonymize_object_info(
            anonymized_info_dict=anonymized_info_dict,
            object_config=checkpoint_config_dict,
        )
        return anonymized_info_dict

    # noinspection PyUnusedLocal
    def _anonymize_checkpoint_run(  # noqa: C901, PLR0912, PLR0915
        self, obj: object, **kwargs
    ) -> dict:
        """
        Traverse the entire Checkpoint configuration structure (as per its formal, validated Marshmallow schema) and
        anonymize every field that can be customized by a user (public fields are recorded as their original names).
        """
        attribute_name: str
        attribute_value: Optional[Union[str, dict]]
        validation_obj: dict

        checkpoint_optional_top_level_keys: List[str] = []

        name: Optional[str] = kwargs.get("name")
        anonymized_name: Optional[str] = self._anonymize_string(name)

        config_version: Optional[Union[Number, str]] = kwargs.get("config_version")
        if config_version is None:
            config_version = 1.0

        template_name: Optional[str] = kwargs.get("template_name")
        anonymized_template_name: Optional[str] = self._anonymize_string(template_name)

        run_name_template: Optional[str] = kwargs.get("run_name_template")
        anonymized_run_name_template: Optional[str] = self._anonymize_string(
            run_name_template
        )

        expectation_suite_name: Optional[str] = kwargs.get("expectation_suite_name")
        anonymized_expectation_suite_name: Optional[str] = self._anonymize_string(
            expectation_suite_name
        )

        batch_request: Optional[
            Union[BatchRequest, RuntimeBatchRequest, dict]
        ] = kwargs.get("batch_request")
        if batch_request is None:
            batch_request = {}

        anonymized_batch_request: Optional[
            Dict[str, List[str]]
        ] = self._aggregate_anonymizer.anonymize(*(), **batch_request)

        action_list: Optional[List[dict]] = kwargs.get("action_list")
        anonymized_action_list: Optional[List[dict]] = None
        if action_list:
            # noinspection PyBroadException
            try:
                anonymized_action_list = [
                    self._aggregate_anonymizer.anonymize(
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
                ] = self._aggregate_anonymizer.anonymize(
                    *(), **validation_batch_request
                )

                validation_expectation_suite_name: Optional[str] = validation_obj.get(
                    "expectation_suite_name"
                )
                anonymized_validation_expectation_suite_name: Optional[
                    str
                ] = self._anonymize_string(validation_expectation_suite_name)

                validation_action_list: Optional[List[dict]] = validation_obj.get(
                    "action_list"
                )
                anonymized_validation_action_list: Optional[List[dict]] = None
                if validation_action_list:
                    # noinspection PyBroadException
                    try:
                        anonymized_validation_action_list = [
                            self._aggregate_anonymizer.anonymize(
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
            anonymized_run_id = self._anonymize_string(str(run_id))

        run_name: Optional[str] = kwargs.get("run_name")
        anonymized_run_name: Optional[str]
        if run_name is None:
            anonymized_run_name = None
        else:
            anonymized_run_name = self._anonymize_string(run_name)

        run_time: Optional[Union[str, datetime.datetime]] = kwargs.get("run_time")
        anonymized_run_time: Optional[str]
        if run_time is None:
            anonymized_run_time = None
        else:
            anonymized_run_time = self._anonymize_string(str(run_time))

        expectation_suite_ge_cloud_id: Optional[str] = kwargs.get(
            "expectation_suite_ge_cloud_id"
        )
        anonymized_expectation_suite_ge_cloud_id: Optional[str]
        if expectation_suite_ge_cloud_id is None:
            anonymized_expectation_suite_ge_cloud_id = None
        else:
            anonymized_expectation_suite_ge_cloud_id = self._anonymize_string(
                expectation_suite_ge_cloud_id
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

    def can_handle(self, obj: Optional[object] = None, **kwargs) -> bool:
        from great_expectations.checkpoint.checkpoint import Checkpoint
        from great_expectations.data_context.types.base import CheckpointConfig

        return obj is not None and isinstance(obj, (Checkpoint, CheckpointConfig))
