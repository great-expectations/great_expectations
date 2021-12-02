import datetime
import logging
from numbers import Number
from typing import Any, Dict, List, Optional, Union

from great_expectations.core import RunIdentifier
from great_expectations.core.batch import BatchRequest
from great_expectations.core.usage_statistics.anonymizers.action_anonymizer import (
    ActionAnonymizer,
)
from great_expectations.core.usage_statistics.anonymizers.anonymizer import Anonymizer
from great_expectations.core.usage_statistics.anonymizers.batch_request_anonymizer import (
    BatchRequestAnonymizer,
)

# TODO: <Alex>ALEX</Alex>
# from great_expectations.validation_operators import (
#     MicrosoftTeamsNotificationAction,
#     NoOpAction,
#     PagerdutyAlertAction,
#     SlackNotificationAction,
#     StoreEvaluationParametersAction,
#     StoreMetricsAction,
#     StoreValidationResultAction,
#     UpdateDataDocsAction,
#     ValidationAction,
# )
# TODO: <Alex>ALEX</Alex>
# TODO: <Alex>ALEX</Alex>
from great_expectations.util import deep_filter_properties_iterable

# TODO: <Alex>ALEX</Alex>

logger = logging.getLogger(__name__)


class CheckpointRunAnonymizer(Anonymizer):
    def __init__(self, salt=None):
        super().__init__(salt=salt)

        self._salt = salt

        # TODO: <Alex>ALEX</Alex>
        # ordered bottom up in terms of inheritance order
        # self._ge_classes = [
        #     StoreMetricsAction,
        #     NoOpAction,
        #     StoreValidationResultAction,
        #     StoreEvaluationParametersAction,
        #     SlackNotificationAction,
        #     PagerdutyAlertAction,
        #     MicrosoftTeamsNotificationAction,
        #     UpdateDataDocsAction,
        #     ValidationAction,
        # ]
        # TODO: <Alex>ALEX</Alex>

    def anonymize_checkpoint_run(self, *args, **kwargs) -> Dict[str, List[str]]:
        print(
            f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] ARGS: {args} ; TYPE: {str(type(args))}"
        )
        print(
            f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] KWARGS: {kwargs} ; TYPE: {str(type(kwargs))}"
        )
        batch_request_anonymizer: BatchRequestAnonymizer = BatchRequestAnonymizer(
            self._salt
        )

        checkpoint_run_optional_top_level_keys: List[str] = []

        validation_obj: dict

        action_anonymizer: ActionAnonymizer = ActionAnonymizer(self._salt)

        action_config_dict: dict
        action_name: str
        action_obj: dict

        name: Optional[str] = kwargs.get("name")
        print(
            f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] NAME: {name} ; TYPE: {str(type(name))}"
        )
        anonymized_name: Optional[str]
        if name is None:
            anonymized_name = None
        else:
            anonymized_name = self.anonymize(name)
        print(
            f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] ANONYMIZED_NAME: {anonymized_name} ; TYPE: {str(type(anonymized_name))}"
        )

        config_version: Optional[Number] = kwargs.get("config_version")
        print(
            f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] CONFIG_VERSION: {config_version} ; TYPE: {str(type(config_version))}"
        )
        anonymized_config_version: Optional[str]
        if config_version is None:
            anonymized_config_version = None
        else:
            anonymized_config_version = self.anonymize(str(config_version))
        print(
            f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] ANONYMIZED_CONFIG_VERSION: {anonymized_config_version} ; TYPE: {str(type(anonymized_config_version))}"
        )

        template_name: Optional[str] = kwargs.get("template_name")
        print(
            f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] TEMPLATE_NAME: {template_name} ; TYPE: {str(type(template_name))}"
        )
        anonymized_template_name: Optional[str]
        if template_name is None:
            anonymized_template_name = None
        else:
            anonymized_template_name = self.anonymize(template_name)
        print(
            f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] ANONYMIZED_TEMPLATE_NAME: {anonymized_template_name} ; TYPE: {str(type(anonymized_template_name))}"
        )

        run_name_template: Optional[str] = kwargs.get("run_name_template")
        anonymized_run_name_template: Optional[str]
        if run_name_template is None:
            anonymized_run_name_template = None
        else:
            anonymized_run_name_template = self.anonymize(run_name_template)

        expectation_suite_name: Optional[str] = kwargs.get("expectation_suite_name")
        print(
            f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] EXPECTATION_SUITE_NAME: {expectation_suite_name} ; TYPE: {str(type(expectation_suite_name))}"
        )
        anonymized_expectation_suite_name: Optional[str]
        if expectation_suite_name is None:
            anonymized_expectation_suite_name = None
        else:
            anonymized_expectation_suite_name = self.anonymize(expectation_suite_name)
        print(
            f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] ANONYMIZED_EXPECTATION_SUITE_NAME: {anonymized_expectation_suite_name} ; TYPE: {str(type(anonymized_expectation_suite_name))}"
        )

        batch_request: Optional[Union[BatchRequest, dict]] = kwargs.get("batch_request")
        print(
            f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] BATCH_REQUEST: {batch_request} ; TYPE: {str(type(batch_request))}"
        )
        anonymized_batch_request: Optional[Dict[str, List[str]]] = None
        if batch_request:
            # noinspection PyBroadException
            try:
                anonymized_batch_request = batch_request_anonymizer.anonymize_batch_request(
                    *(),
                    **batch_request
                    # TODO: <Alex>ALEX</Alex>
                    # **{
                    #     "batch_request": batch_request,
                    # },
                    # TODO: <Alex>ALEX</Alex>
                )
            except Exception:
                print(
                    f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] ANONYMIZED_BATCH_REQUEST: GODDAMMIT!!!!!!!!!!"
                )
                logger.debug(
                    "anonymize_checkpoint_run: Unable to create anonymized_batch_request payload field"
                )
        print(
            f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] ANONYMIZED_BATCH_REQUEST: {anonymized_batch_request} ; TYPE: {str(type(anonymized_batch_request))}"
        )

        include_anonymized_batch_request: bool = anonymized_batch_request is not None

        action_list: Optional[List[dict]] = kwargs.get("action_list")
        anonymized_action_list: Optional[List[dict]] = None
        print(
            f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] ACTION_LIST: {action_list} ; TYPE: {str(type(action_list))}"
        )
        if action_list:
            # TODO: <Alex>ALEX</Alex>
            # # noinspection PyBroadException
            # try:
            #     anonymized_action_list = [
            #         action_anonymizer.anonymize_action_info(
            #             action_name=action_name, action_obj=action_obj
            #         )
            #         for action_config_dict in action_list
            #         for action_name, action_obj in action_config_dict.items()
            #     ]
            # except Exception:
            #     logger.debug(
            #         "anonymize_checkpoint_run: Unable to create anonymized_action_list payload field"
            #     )
            # noinspection PyBroadException
            # TODO: <Alex>ALEX</Alex>
            try:
                anonymized_action_list = [
                    action_anonymizer.anonymize_action_info(
                        action_name=action_config_dict["name"],
                        action_config=action_config_dict["action"],
                    )
                    for action_config_dict in action_list
                    # for action_name, action_obj in action_config_dict.items()
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
                # print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] IN-VALIDATION_BATCH_REQUEST-BATCH_REQUEST_EXISTS: {batch_request} ; TYPE: {str(type(batch_request))}")
                if isinstance(validations_batch_request, BatchRequest):
                    validations_batch_request = validations_batch_request.to_json_dict()

                effective_batch_request: dict
                if isinstance(batch_request, BatchRequest):
                    effective_batch_request = dict(
                        batch_request.to_json_dict(), **validations_batch_request
                    )
                else:
                    effective_batch_request = dict(
                        batch_request, **validations_batch_request
                    )

                validations_batch_request = effective_batch_request
                print(
                    f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] VALIDATION_BATCH_REQUEST: {validations_batch_request} ; TYPE: {str(type(validations_batch_request))}"
                )

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
                        print(
                            f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] ANONYMIZED_VALIDATION_BATCH_REQUEST: {anonymized_validation_batch_request} ; TYPE: {str(type(anonymized_validation_batch_request))}"
                        )
                    except Exception:
                        print(
                            f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] ANONYMIZED_VALIDATION_BATCH_REQUEST: GODDAMMIT!!!!!!!!!!"
                        )
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
                print(
                    f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] VALIDATION_ACTION_LIST: {validations_action_list} ; TYPE: {str(type(validations_action_list))}"
                )
                # TODO: <Alex>ALEX</Alex>
                # if validations_action_list:
                #     # noinspection PyBroadException
                #     try:
                #         anonymized_validations_action_list = [
                #             action_anonymizer.anonymize_action_info(
                #                 action_name=action_name, action_obj=action_obj
                #             )
                #             for action_config_dict in validations_action_list
                #             for action_name, action_obj in action_config_dict.items()
                #         ]
                #     except Exception:
                #         logger.debug(
                #             "anonymize_checkpoint_run: Unable to create anonymized_validation_action_list payload field"
                #         )
                # TODO: <Alex>ALEX</Alex>
                if validations_action_list:
                    # noinspection PyBroadException
                    try:
                        anonymized_validations_action_list = [
                            action_anonymizer.anonymize_action_info(
                                action_name=action_config_dict["name"],
                                action_config=action_config_dict["action"],
                            )
                            for action_config_dict in validations_action_list
                            # for action_name, action_obj in action_config_dict.items()
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

                if anonymized_validations_expectation_suite_name:
                    anonymized_validation[
                        "anonymized_expectation_suite_name"
                    ] = anonymized_validations_expectation_suite_name

                if anonymized_validations_action_list:
                    anonymized_validation[
                        "anonymized_action_list"
                    ] = anonymized_validations_action_list

            anonymized_validation: Dict[str, Dict[str, Any]]
            num_anonymized_validation_batch_requests: int = len(
                [
                    anonymized_validation
                    for anonymized_validation in anonymized_validations
                    if "anonymized_batch_request" in anonymized_validation
                ]
            )
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
        print(
            f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] RESULT_FORMAT: {result_format} ; TYPE: {str(type(result_format))}"
        )
        if result_format:
            checkpoint_run_optional_top_level_keys.append("result_format")
            print(
                f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] APPENDED_RESULT_FORMAT: {checkpoint_run_optional_top_level_keys} ; TYPE: {str(type(checkpoint_run_optional_top_level_keys))}"
            )

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
            anonymized_checkpoint_run_properties_dict.pop(
                "anonymized_batch_request", None
            )
        print(
            f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] ANONYMIZED_CHECKPOINT_RUN_PROPERTIES_DICT:\n{anonymized_checkpoint_run_properties_dict} ; TYPE: {str(type(anonymized_checkpoint_run_properties_dict))}"
        )

        # TODO: <Alex>ALEX</Alex>
        # deep_filter_properties_iterable(
        #     properties=anonymized_checkpoint_run_properties_dict,
        #     clean_falsy=True,
        #     inplace=True,
        # )
        # TODO: <Alex>ALEX</Alex>
        # print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] ANONYMIZED_CHECKPOINT_RUN_PROPERTIES_DICT-CLEANED:\n{anonymized_checkpoint_run_properties_dict} ; TYPE: {str(type(anonymized_checkpoint_run_properties_dict))}")
        # TODO: <Alex>ALEX</Alex>

        print(
            f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] ORIGINAL_BATCH_REQUEST-AT_RETURN: {batch_request} ; TYPE: {str(type(batch_request))}"
        )
        # TODO: <Alex>ALEX</Alex>
        return anonymized_checkpoint_run_properties_dict
        # TODO: <Alex>ALEX</Alex>

        # TODO: <Alex>ALEX</Alex>
        # anonymized_info_dict: dict = {
        #     "anonymized_name": self.anonymize(name),
        # }
        # TODO: <Alex>ALEX</Alex>

        # TODO: <Alex>ALEX</Alex>
        # return self.anonymize_object_info(
        #     object_=args[0],
        #     anonymized_info_dict=anonymized_checkpoint_run_properties_dict,
        #     ge_classes=self._ge_classes,
        # )
        # a = self.anonymize_object_info(
        #     object_=args[0],
        #     anonymized_info_dict=anonymized_checkpoint_run_properties_dict,
        #     ge_classes=self._ge_classes,
        # )
        # print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] ANONYMIZED_OBJECT_INFO-AT_RETURN: {a} ; TYPE: {str(type(a))}")
        # return a
        # # TODO: <Alex>ALEX</Alex>

    # def is_parent_class_recognized(self, config):
    #     return self._is_parent_class_recognized(
    #         classes_to_check=self._ge_classes,
    #         object_config=config,
    # )
