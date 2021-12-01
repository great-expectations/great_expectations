import datetime
import logging
from typing import Any, Dict, List, Optional, Union
from numbers import Number

from great_expectations.core import RunIdentifier
from great_expectations.core.batch import BatchRequest
from great_expectations.core.usage_statistics.anonymizers.action_anonymizer import (
    ActionAnonymizer,
)
from great_expectations.core.usage_statistics.anonymizers.anonymizer import Anonymizer
from great_expectations.core.usage_statistics.anonymizers.batch_request_anonymizer import (
    BatchRequestAnonymizer,
)
from great_expectations.util import deep_filter_properties_iterable

logger = logging.getLogger(__name__)


class CheckpointRunAnonymizer(Anonymizer):
    def __init__(self, salt=None):
        super().__init__(salt=salt)

        self._salt = salt

    def anonymize_checkpoint_run(self, *args, **kwargs) -> Dict[str, List[str]]:
        print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.anonymize_checkpoint_run] ARGS: {args} ; TYPE: {str(type(args))}")
        print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.anonymize_checkpoint_run] KWARGS: {kwargs} ; TYPE: {str(type(kwargs))}")
        batch_request_anonymizer: BatchRequestAnonymizer = BatchRequestAnonymizer(
            self._salt
        )

        checkpoint_run_optional_top_level_keys: List[str] = []

        validation_obj: dict

        action_anonymizer: ActionAnonymizer = ActionAnonymizer(self._salt)

        actions_dict: dict
        action_name: str
        action_obj: dict

        name: Optional[str] = kwargs.get("name")
        print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.anonymize_checkpoint_run] NAME: {name} ; TYPE: {str(type(name))}")
        anonymized_name: Optional[str]
        if name is None:
            anonymized_name = None
        else:
            anonymized_name = self.anonymize(name)
        print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.anonymize_checkpoint_run] ANONYMIZED_NAME: {anonymized_name} ; TYPE: {str(type(anonymized_name))}")

        config_version: Optional[Number] = kwargs.get("config_version")
        print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.anonymize_checkpoint_run] CONFIG_VERSION: {config_version} ; TYPE: {str(type(config_version))}")
        anonymized_config_version: Optional[str]
        if config_version is None:
            anonymized_config_version = None
        else:
            anonymized_config_version = self.anonymize(str(config_version))
        print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.anonymize_checkpoint_run] ANONYMIZED_CONFIG_VERSION: {anonymized_config_version} ; TYPE: {str(type(anonymized_config_version))}")

        template_name: Optional[str] = kwargs.get("template_name")
        print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.anonymize_checkpoint_run] TEMPLATE_NAME: {template_name} ; TYPE: {str(type(template_name))}")
        anonymized_template_name: Optional[str]
        if template_name is None:
            anonymized_template_name = None
        else:
            anonymized_template_name = self.anonymize(template_name)
        print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.anonymize_checkpoint_run] ANONYMIZED_TEMPLATE_NAME: {anonymized_template_name} ; TYPE: {str(type(anonymized_template_name))}")

        run_name_template: Optional[str] = kwargs.get("run_name_template")
        anonymized_run_name_template: Optional[str]
        if run_name_template is None:
            anonymized_run_name_template = None
        else:
            anonymized_run_name_template = self.anonymize(run_name_template)

        expectation_suite_name: Optional[str] = kwargs.get("expectation_suite_name")
        print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.anonymize_checkpoint_run] EXPECTATION_SUITE_NAME: {expectation_suite_name} ; TYPE: {str(type(expectation_suite_name))}")
        anonymized_expectation_suite_name: Optional[str]
        if expectation_suite_name is None:
            anonymized_expectation_suite_name = None
        else:
            anonymized_expectation_suite_name = self.anonymize(expectation_suite_name)
        print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.anonymize_checkpoint_run] ANONYMIZED_EXPECTATION_SUITE_NAME: {anonymized_expectation_suite_name} ; TYPE: {str(type(anonymized_expectation_suite_name))}")

        batch_request: Optional[Union[BatchRequest, dict]] = kwargs.get("batch_request")
        print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.anonymize_checkpoint_run] BATCH_REQUEST: {batch_request} ; TYPE: {str(type(batch_request))}")
        anonymized_batch_request: Optional[Dict[str, List[str]]] = None
        if batch_request:
            # noinspection PyBroadException
            try:
                anonymized_batch_request = (
                    batch_request_anonymizer.anonymize_batch_request(
                        *(),
                        **batch_request
                        # TODO: <Alex>ALEX</Alex>
                        # **{
                        #     "batch_request": batch_request,
                        # },
                        # TODO: <Alex>ALEX</Alex>
                    )
                )
            except Exception:
                print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.anonymize_checkpoint_run] ANONYMIZED_BATCH_REQUEST: GODDAMMIT!!!!!!!!!!")
                logger.debug(
                    "anonymize_checkpoint_run: Unable to create anonymized_batch_request payload field"
                )
        print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.anonymize_checkpoint_run] ANONYMIZED_BATCH_REQUEST: {anonymized_batch_request} ; TYPE: {str(type(anonymized_batch_request))}")

        include_anonymized_batch_request: bool = anonymized_batch_request is not None

        action_list: Optional[List[dict]] = kwargs.get("action_list")
        anonymized_action_list: Optional[List[dict]] = None
        if action_list:
            # noinspection PyBroadException
            try:
                anonymized_action_list = [
                    action_anonymizer.anonymize_action_info(
                        action_name=action_name, action_obj=action_obj
                    )
                    for actions_dict in action_list
                    for action_name, action_obj in actions_dict.items()
                ]
            except Exception:
                logger.debug(
                    "anonymize_checkpoint_run: Unable to create anonymized_action_list payload field"
                )

        validations: Optional[List[dict]] = kwargs.get("validations")
        anonymized_validations: Optional[List[dict]] = []
        if validations:
            for validation_obj in validations:
                validations_batch_request: Optional[
                    Union[BatchRequest, dict]
                ] = validation_obj.get("batch_request")
                # print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.anonymize_checkpoint_run] IN-VALIDATION_BATCH_REQUEST-BATCH_REQUEST_EXISTS: {batch_request} ; TYPE: {str(type(batch_request))}")
                if isinstance(validations_batch_request, BatchRequest):
                    validations_batch_request = validations_batch_request.to_json_dict()

                effective_batch_request: dict
                if isinstance(batch_request, BatchRequest):
                    effective_batch_request = dict(batch_request.to_json_dict(), **validations_batch_request)
                else:
                    effective_batch_request = dict(batch_request, **validations_batch_request)

                validations_batch_request = effective_batch_request
                print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.anonymize_checkpoint_run] VALIDATION_BATCH_REQUEST: {validations_batch_request} ; TYPE: {str(type(validations_batch_request))}")

                anonymized_validation_batch_request: Optional[
                    Dict[str, List[str]]
                ] = None
                if validations_batch_request:
                    # noinspection PyBroadException
                    try:
                        anonymized_validation_batch_request = batch_request_anonymizer.anonymize_batch_request(
                            *(),
                            **validations_batch_request
                            # TODO: <Alex>ALEX</Alex>
                            # **{
                            #     "batch_request": validations_batch_request,
                            # },
                            # TODO: <Alex>ALEX</Alex>
                        )
                        print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.anonymize_checkpoint_run] ANONYMIZED_VALIDATION_BATCH_REQUEST: {anonymized_validation_batch_request} ; TYPE: {str(type(anonymized_validation_batch_request))}")
                    except Exception:
                        print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.anonymize_checkpoint_run] ANONYMIZED_VALIDATION_BATCH_REQUEST: GODDAMMIT!!!!!!!!!!")
                        logger.debug(
                            "anonymize_checkpoint_run: Unable to create anonymized_batch_request payload field"
                        )

                validations_expectation_suite_name: Optional[str] = validation_obj.get(
                    "expectation_suite_name"
                )
                anonymized_validations_expectation_suite_name: Optional[str]
                if validations_expectation_suite_name is None:
                    anonymized_validations_expectation_suite_name = None
                else:
                    anonymized_validations_expectation_suite_name = self.anonymize(
                        validations_expectation_suite_name
                    )

                validations_action_list: Optional[List[dict]] = validation_obj.get(
                    "action_list"
                )
                anonymized_validations_action_list: Optional[List[dict]] = None
                if validations_action_list:
                    # noinspection PyBroadException
                    try:
                        anonymized_validations_action_list = [
                            action_anonymizer.anonymize_action_info(
                                action_name=action_name, action_obj=action_obj
                            )
                            for actions_dict in validations_action_list
                            for action_name, action_obj in actions_dict.items()
                        ]
                    except Exception:
                        logger.debug(
                            "anonymize_checkpoint_run: Unable to create anonymized_validation_action_list payload field"
                        )

                anonymized_validation: Dict[str, Union[str, Dict[str, Any], List[Dict[str, Any]]]] = {}

                if anonymized_validation_batch_request:
                    anonymized_validation["anonymized_batch_request"] = anonymized_validation_batch_request

                if anonymized_validations_expectation_suite_name:
                    anonymized_validation["anonymized_expectation_suite_name"] = anonymized_validations_expectation_suite_name

                if anonymized_validations_action_list:
                    anonymized_validation["anonymized_action_list"] = anonymized_validations_action_list

            anonymized_validation: Dict[str, Dict[str, Any]]
            num_anonymized_validation_batch_requests: int = len([anonymized_validation for anonymized_validation in anonymized_validations if "anonymized_batch_request" in anonymized_validation])
            if num_anonymized_validation_batch_requests == len(validations):
                include_anonymized_batch_request = True

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

        evaluation_parameters: Optional[dict] = kwargs.get("evaluation_parameters")
        if evaluation_parameters:
            checkpoint_run_optional_top_level_keys.append("evaluation_parameters")

        runtime_configuration: Optional[dict] = kwargs.get("runtime_configuration")
        if runtime_configuration:
            checkpoint_run_optional_top_level_keys.append("runtime_configuration")

        result_format: Optional[Union[str, dict]] = kwargs.get("result_format")
        print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.anonymize_checkpoint_run] RESULT_FORMAT: {result_format} ; TYPE: {str(type(result_format))}")
        if result_format:
            checkpoint_run_optional_top_level_keys.append("result_format")
            print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.anonymize_checkpoint_run] APPENDED_RESULT_FORMAT: {checkpoint_run_optional_top_level_keys} ; TYPE: {str(type(checkpoint_run_optional_top_level_keys))}")

        profilers: Optional[List[dict]] = kwargs.get("profilers")
        if profilers:
            checkpoint_run_optional_top_level_keys.append("profilers")

        anonymized_checkpoint_run_properties_dict: Dict[str, List[str]] = {
            "anonymized_name": anonymized_name,
            "anonymized_config_version": anonymized_config_version,
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
            "checkpoint_run_optional_top_level_keys": checkpoint_run_optional_top_level_keys,
        }
        if not include_anonymized_batch_request:
            anonymized_checkpoint_run_properties_dict.pop("anonymized_batch_request", None)
        print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.anonymize_checkpoint_run] ANONYMIZED_CHECKPOINT_RUN_PROPERTIES_DICT:\n{anonymized_checkpoint_run_properties_dict} ; TYPE: {str(type(anonymized_checkpoint_run_properties_dict))}")

        deep_filter_properties_iterable(
            properties=anonymized_checkpoint_run_properties_dict,
            clean_falsy=True,
            inplace=True,
        )
        print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.anonymize_checkpoint_run] ANONYMIZED_CHECKPOINT_RUN_PROPERTIES_DICT-CLEANED:\n{anonymized_checkpoint_run_properties_dict} ; TYPE: {str(type(anonymized_checkpoint_run_properties_dict))}")

        print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.anonymize_checkpoint_run] ORIGINAL_BATCH_REQUEST-AT_RETURN: {batch_request} ; TYPE: {str(type(batch_request))}")
        return anonymized_checkpoint_run_properties_dict
