import datetime
import logging
from typing import Any, Dict, List, Optional, Union
from numbers import Number

import great_expectations.exceptions as ge_exceptions
# TODO: <Alex>ALEX</Alex>
# from great_expectations.checkpoint import Checkpoint
# TODO: <Alex>ALEX</Alex>
from great_expectations.checkpoint.util import get_substituted_validation_dict
from great_expectations.core import RunIdentifier
from great_expectations.core.batch import (
    BatchRequest,
    get_batch_request_dict,
)
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
from great_expectations.core.util import get_datetime_string_from_strftime_format
from great_expectations.data_context.types.base import CheckpointConfig
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
        print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] ARGS: {args} ; TYPE: {str(type(args))}")
        print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] KWARGS: {kwargs} ; TYPE: {str(type(kwargs))}")
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
        print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] NAME: {name} ; TYPE: {str(type(name))}")
        anonymized_name: Optional[str]
        if name is None:
            anonymized_name = None
        else:
            anonymized_name = self.anonymize(name)
        print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] ANONYMIZED_NAME: {anonymized_name} ; TYPE: {str(type(anonymized_name))}")

        config_version: Optional[Number] = kwargs.get("config_version")
        print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] CONFIG_VERSION: {config_version} ; TYPE: {str(type(config_version))}")
        anonymized_config_version: Optional[str]
        if config_version is None:
            anonymized_config_version = None
        else:
            anonymized_config_version = self.anonymize(str(config_version))
        print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] ANONYMIZED_CONFIG_VERSION: {anonymized_config_version} ; TYPE: {str(type(anonymized_config_version))}")

        template_name: Optional[str] = kwargs.get("template_name")
        print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] TEMPLATE_NAME: {template_name} ; TYPE: {str(type(template_name))}")
        anonymized_template_name: Optional[str]
        if template_name is None:
            anonymized_template_name = None
        else:
            anonymized_template_name = self.anonymize(template_name)
        print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] ANONYMIZED_TEMPLATE_NAME: {anonymized_template_name} ; TYPE: {str(type(anonymized_template_name))}")

        run_name_template: Optional[str] = kwargs.get("run_name_template")
        anonymized_run_name_template: Optional[str]
        if run_name_template is None:
            anonymized_run_name_template = None
        else:
            anonymized_run_name_template = self.anonymize(run_name_template)

        expectation_suite_name: Optional[str] = kwargs.get("expectation_suite_name")
        print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] EXPECTATION_SUITE_NAME: {expectation_suite_name} ; TYPE: {str(type(expectation_suite_name))}")
        anonymized_expectation_suite_name: Optional[str]
        if expectation_suite_name is None:
            anonymized_expectation_suite_name = None
        else:
            anonymized_expectation_suite_name = self.anonymize(expectation_suite_name)
        print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] ANONYMIZED_EXPECTATION_SUITE_NAME: {anonymized_expectation_suite_name} ; TYPE: {str(type(anonymized_expectation_suite_name))}")

        batch_request: Optional[Union[BatchRequest, dict]] = kwargs.get("batch_request")
        print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] BATCH_REQUEST: {batch_request} ; TYPE: {str(type(batch_request))}")
        if batch_request is None:
            batch_request = {}

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
                print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] ANONYMIZED_BATCH_REQUEST: GODDAMMIT!!!!!!!!!!")
                logger.debug(
                    "anonymize_checkpoint_run: Unable to create anonymized_batch_request payload field"
                )
        print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] ANONYMIZED_BATCH_REQUEST: {anonymized_batch_request} ; TYPE: {str(type(anonymized_batch_request))}")

        include_anonymized_batch_request: bool = anonymized_batch_request is not None
        print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] INCLUDE_ANONYMIZED_BATCH_REQUEST: {include_anonymized_batch_request} ; TYPE: {str(type(include_anonymized_batch_request))}")

        action_list: Optional[List[dict]] = kwargs.get("action_list")
        anonymized_action_list: Optional[List[dict]] = None
        print(f'\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] ACTION_LIST: {action_list} ; TYPE: {str(type(action_list))}')
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
                        action_name=action_config_dict["name"], action_config=action_config_dict["action"]
                    )
                    for action_config_dict in action_list
                    # for action_name, action_obj in action_config_dict.items()
                ]
            except Exception:
                print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] ANONYMIZED_ACTION_LIST: GODDAMMIT!!!!!!!!!!")
                logger.debug(
                    "anonymize_checkpoint_run: Unable to create anonymized_action_list payload field"
                )

        validations: Optional[List[dict]] = kwargs.get("validations")
        anonymized_validations: Optional[List[dict]] = []
        print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] VALIDATIONS: {validations} ; TYPE: {str(type(validations))}")
        if validations:
            for validation_obj in validations:
                print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] VALIDATION_OBJECT: {validation_obj} ; TYPE: {str(type(validation_obj))}")
                validation_batch_request: Optional[
                    Union[BatchRequest, dict]
                ] = validation_obj.get("batch_request")
                print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] IN-VALIDATION_BATCH_REQUEST-BATCH_REQUEST_EXISTS: {validation_batch_request} ; TYPE: {str(type(validation_batch_request))}")
                if validation_batch_request is None:
                    validation_batch_request = {}

                # TODO: <Alex>ALEX</Alex>
                # effective_batch_request: dict
                #
                # if isinstance(batch_request, BatchRequest):
                #     batch_request = batch_request.to_dict()
                # print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] IN-VALIDATION_BATCH_REQUEST-AS_DICT: {batch_request} ; TYPE: {str(type(batch_request))}")
                # TODO: <Alex>ALEX</Alex>

                if isinstance(validation_batch_request, BatchRequest):
                    validation_batch_request = validation_batch_request.to_dict()
                print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] IN-VALIDATION_VALIDATION_BATCH_REQUEST-AS_DICT: {validation_batch_request} ; TYPE: {str(type(validation_batch_request))}")

                # TODO: <Alex>ALEX</Alex>
                # effective_batch_request = dict(**batch_request, **validation_batch_request)
                # print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] IN-VALIDATION_EFFECTIVE_BATCH_REQUEST-AS_DICT: {effective_batch_request} ; TYPE: {str(type(effective_batch_request))}")
                # TODO: <Alex>ALEX</Alex>

                # TODO: <Alex>ALEX</Alex>
                # validation_batch_request = effective_batch_request
                # print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] EFFECTIVE_VALIDATION_BATCH_REQUEST: {validation_batch_request} ; TYPE: {str(type(validation_batch_request))}")
                # TODO: <Alex>ALEX</Alex>

                anonymized_validation_batch_request: Optional[
                    Dict[str, List[str]]
                ] = None
                if validation_batch_request:
                    # noinspection PyBroadException
                    try:
                        anonymized_validation_batch_request = batch_request_anonymizer.anonymize_batch_request(
                            *(),
                            **validation_batch_request
                            # TODO: <Alex>ALEX</Alex>
                            # **{
                            #     "batch_request": validation_batch_request,
                            # },
                            # TODO: <Alex>ALEX</Alex>
                        )
                        print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] ANONYMIZED_VALIDATION_BATCH_REQUEST: {anonymized_validation_batch_request} ; TYPE: {str(type(anonymized_validation_batch_request))}")
                    except Exception:
                        print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] ANONYMIZED_VALIDATION_BATCH_REQUEST: GODDAMMIT!!!!!!!!!!")
                        logger.debug(
                            "anonymize_checkpoint_run: Unable to create anonymized_batch_request payload field"
                        )

                validation_expectation_suite_name: Optional[str] = validation_obj.get(
                    "expectation_suite_name"
                )
                print(f'\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] VALIDATION_EXPECTATION_SUITE_NAME: {validation_expectation_suite_name} ; TYPE: {str(type(validation_expectation_suite_name))}')
                anonymized_validation_expectation_suite_name: Optional[str]
                if validation_expectation_suite_name is None:
                    anonymized_validation_expectation_suite_name = None
                else:
                    anonymized_validation_expectation_suite_name = self.anonymize(
                        validation_expectation_suite_name
                    )
                print(f'\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] ANONYMIZED_VALIDATION_EXPECTATION_SUITE_NAME: {anonymized_validation_expectation_suite_name} ; TYPE: {str(type(anonymized_validation_expectation_suite_name))}')

                validation_action_list: Optional[List[dict]] = validation_obj.get(
                    "action_list"
                )
                anonymized_validation_action_list: Optional[List[dict]] = None
                print(f'\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] VALIDATION_ACTION_LIST: {validation_action_list} ; TYPE: {str(type(validation_action_list))}')
                # TODO: <Alex>ALEX</Alex>
                # if validation_action_list:
                #     # noinspection PyBroadException
                #     try:
                #         anonymized_validation_action_list = [
                #             action_anonymizer.anonymize_action_info(
                #                 action_name=action_name, action_obj=action_obj
                #             )
                #             for action_config_dict in validation_action_list
                #             for action_name, action_obj in action_config_dict.items()
                #         ]
                #     except Exception:
                #         logger.debug(
                #             "anonymize_checkpoint_run: Unable to create anonymized_validation_action_list payload field"
                #         )
                # TODO: <Alex>ALEX</Alex>
                if validation_action_list:
                    # noinspection PyBroadException
                    try:
                        anonymized_validation_action_list = [
                            action_anonymizer.anonymize_action_info(
                                action_name=action_config_dict["name"], action_config=action_config_dict["action"]
                            )
                            for action_config_dict in validation_action_list
                            # for action_name, action_obj in action_config_dict.items()
                        ]
                    except Exception:
                        logger.debug(
                            "anonymize_checkpoint_run: Unable to create anonymized_validation_action_list payload field"
                        )

                print(f'\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] ANONYMIZED_VALIDATION_ACTION_LIST: {anonymized_validation_action_list} ; TYPE: {str(type(anonymized_validation_action_list))}')
                anonymized_validation: Dict[str, Union[str, Dict[str, Any], List[Dict[str, Any]]]] = {}

                if anonymized_validation_batch_request:
                    anonymized_validation["anonymized_batch_request"] = anonymized_validation_batch_request

                if anonymized_validation_expectation_suite_name:
                    anonymized_validation["anonymized_expectation_suite_name"] = anonymized_validation_expectation_suite_name

                if anonymized_validation_action_list:
                    anonymized_validation["anonymized_action_list"] = anonymized_validation_action_list

                anonymized_validation: Dict[str, Dict[str, Any]] = {
                    "anonymized_batch_request": anonymized_validation_batch_request,
                    "anonymized_expectation_suite_name": anonymized_validation_expectation_suite_name,
                    "anonymized_action_list": anonymized_action_list,
                }
                print(f'\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] ANONYMIZED_VALIDATION: {anonymized_validation_action_list} ; TYPE: {str(type(anonymized_validation))}')

                anonymized_validations.append(anonymized_validation)

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

        # TODO: <Alex>ALEX</Alex>
        # result_format: Optional[Union[str, dict]] = kwargs.get("result_format")
        # TODO: <Alex>ALEX</Alex>
        # TODO: <Alex>ALEX</Alex>
        # print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] RESULT_FORMAT: {result_format} ; TYPE: {str(type(result_format))}")
        # if result_format:
        #     checkpoint_run_optional_top_level_keys.append("result_format")
        #     print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] APPENDED_RESULT_FORMAT: {checkpoint_run_optional_top_level_keys} ; TYPE: {str(type(checkpoint_run_optional_top_level_keys))}")
        # TODO: <Alex>ALEX</Alex>

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
        print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] ANONYMIZED_CHECKPOINT_RUN_PROPERTIES_DICT:\n{anonymized_checkpoint_run_properties_dict} ; TYPE: {str(type(anonymized_checkpoint_run_properties_dict))}")

        # TODO: <Alex>ALEX</Alex>
        deep_filter_properties_iterable(
            properties=anonymized_checkpoint_run_properties_dict,
            clean_falsy=True,
            inplace=True,
        )
        # TODO: <Alex>ALEX</Alex>
        print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] ANONYMIZED_CHECKPOINT_RUN_PROPERTIES_DICT-CLEANED:\n{anonymized_checkpoint_run_properties_dict} ; TYPE: {str(type(anonymized_checkpoint_run_properties_dict))}")
        # TODO: <Alex>ALEX</Alex>

        # print(f"\n[ALEX_TEST] [CheckpointRunAnonymizer.ANONYMIZE_CHECKPOINT_RUN] ORIGINAL_BATCH_REQUEST-AT_RETURN: {batch_request} ; TYPE: {str(type(batch_request))}")
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
    # TODO: <Alex>ALEX</Alex>
    # TODO: <Alex>ALEX</Alex>
    # noinspection PyUnusedLocal
    # TODO: <Alex>ALEX</Alex>
    def resolve_config_using_acceptable_arguments(
        self,
        checkpoint: "Checkpoint",
        template_name: Optional[str] = None,
        run_name_template: Optional[str] = None,
        expectation_suite_name: Optional[str] = None,
        batch_request: Optional[Union[dict, BatchRequest]] = None,
        action_list: Optional[List[dict]] = None,
        evaluation_parameters: Optional[dict] = None,
        runtime_configuration: Optional[dict] = None,
        validations: Optional[List[dict]] = None,
        profilers: Optional[List[dict]] = None,
        run_id: Optional[Union[str, RunIdentifier]] = None,
        run_name: Optional[str] = None,
        run_time: Optional[Union[str, datetime.datetime]] = None,
        # TODO: <Alex>ALEX</Alex>
        result_format: Optional[Union[str, dict]] = None,
        # TODO: <Alex>ALEX</Alex>
        expectation_suite_ge_cloud_id: Optional[str] = None,
    ) -> CheckpointConfig: #) -> Tuple[dict, CheckpointConfig]:
        print(f'\n[ALEX_TEST] [CHECKPOINT.RESOLVE_CONFIG_USING_ACCEPTABLE_ARGUMENTS] BATCH_REQUEST:\n{batch_request} ; TYPE: {str(type(batch_request))}')
        print(f'\n[ALEX_TEST] [CHECKPOINT.RESOLVE_CONFIG_USING_ACCEPTABLE_ARGUMENTS] SELF-0: {self} ; TYPE: {str(type(self))}')
        assert not (run_id and run_name) and not (
            run_id and run_time
        ), "Please provide either a run_id or run_name and/or run_time."

        run_time = run_time or datetime.datetime.now()
        runtime_configuration = runtime_configuration or {}
        # TODO: <Alex>ALEX</Alex>
        # result_format = result_format or runtime_configuration.get("result_format")
        # TODO: <Alex>ALEX</Alex>

        batch_request, validations = get_batch_request_dict(
            batch_request=batch_request, validations=validations
        )
        print(f'\n[ALEX_TEST] [CHECKPOINT.RESOLVE_CONFIG_USING_ACCEPTABLE_ARGUMENTS] BATCH_REQUEST-UPDATED:\n{batch_request} ; TYPE: {str(type(batch_request))}')

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
        # TODO: <Alex>ALEX</Alex>
        # print(f'\n[ALEX_TEST] [CHECKPOINT.RESOLVE_CONFIG_USING_ACCEPTABLE_ARGUMENTS] BATCH_REQUEST-BEFORE_SUBSTITUTION:\n{batch_request} ; TYPE: {str(type(batch_request))}')
        # print(f'\n[ALEX_TEST] [CHECKPOINT.RESOLVE_CONFIG_USING_ACCEPTABLE_ARGUMENTS] SELF-1.SUBSTITUTED_CONFIG:\n{self._substituted_config} ; TYPE: {str(type(self._substituted_config))}')
        # print(f'\n[ALEX_TEST] [CHECKPOINT.RESOLVE_CONFIG_USING_ACCEPTABLE_ARGUMENTS] SELF-1: {self} ; TYPE: {str(type(self))}')
        substituted_runtime_config: CheckpointConfig = checkpoint.get_substituted_config(
            runtime_kwargs=runtime_kwargs
        )
        # print(f'\n[ALEX_TEST] [CHECKPOINT.RESOLVE_CONFIG_USING_ACCEPTABLE_ARGUMENTS] BATCH_REQUEST-RIGHT_AFTER_SUBSTITUTION:\n{batch_request} ; TYPE: {str(type(batch_request))}')
        # print(f'\n[ALEX_TEST] [CHECKPOINT.RESOLVE_CONFIG_USING_ACCEPTABLE_ARGUMENTS] SUBSTITUTED_CONFIG:\n{substituted_runtime_config} ; TYPE: {str(type(substituted_runtime_config))}')
        # print(f'\n[ALEX_TEST] [CHECKPOINT.RESOLVE_CONFIG_USING_ACCEPTABLE_ARGUMENTS] BATCH_REQUEST-RIGHT_AFTER_SUBSTITUTED_CONFIG:\n{batch_request} ; TYPE: {str(type(batch_request))}')
        # print(f'\n[ALEX_TEST] [CHECKPOINT.RESOLVE_CONFIG_USING_ACCEPTABLE_ARGUMENTS] SELF-2.SUBSTITUTED_CONFIG:\n{self._substituted_config} ; TYPE: {str(type(self._substituted_config))}')
        # print(f'\n[ALEX_TEST] [CHECKPOINT.RESOLVE_CONFIG_USING_ACCEPTABLE_ARGUMENTS] SELF-2: {self} ; TYPE: {str(type(self))}')
        # TODO: <Alex>ALEX</Alex>
        run_name_template = substituted_runtime_config.run_name_template
        validations = substituted_runtime_config.validations
        batch_request = substituted_runtime_config.batch_request
        # print(f'\n[ALEX_TEST] [CHECKPOINT.RESOLVE_CONFIG_USING_ACCEPTABLE_ARGUMENTS] BATCH_REQUEST-SUBSTITUTED:\n{batch_request} ; TYPE: {str(type(batch_request))}')
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
            validation_batch_request: BatchRequest = substituted_validation_dict.get(
                "batch_request"
            )
            validation_dict["batch_request"] = validation_batch_request
            validation_expectation_suite_name: str = substituted_validation_dict.get(
                "expectation_suite_name"
            )
            validation_dict["expectation_suite_name"] = validation_expectation_suite_name
            validation_expectation_suite_ge_cloud_id: str = substituted_validation_dict.get(
                "expectation_suite_ge_cloud_id"
            )
            validation_dict["expectation_suite_ge_cloud_id"] = validation_expectation_suite_ge_cloud_id
            validation_action_list: list = substituted_validation_dict.get("action_list")
            validation_dict["action_list"] = validation_action_list

        # TODO: <Alex>ALEX</Alex>
        # num_validation_batch_requests: int = len([validation_dict for validation_dict in validations if "batch_request" in validation_dict])
        # if num_validation_batch_requests == len(validations):
        #     substituted_runtime_config.batch_request = None
        # TODO: <Alex>ALEX</Alex>

        # TODO: <Alex>ALEX</Alex>
        # runtime_kwargs.update(
        #     {
        #         "run_name_template": run_name_template,
        #         "batch_request": batch_request,
        #         "validations": validations,
        #         "run_id": run_id,
        #         # TODO: <Alex>ALEX</Alex>
        #         "run_name": run_name,
        #         "run_time": run_time,
        #         # TODO: <Alex>ALEX</Alex>
        #         # TODO: <Alex>ALEX</Alex>
        #         # "result_format": result_format,
        #         # TODO: <Alex>ALEX</Alex>
        #     }
        # )
        # TODO: <Alex>ALEX</Alex>
        # print(f"\n[ALEX_TEST] [CHECKPOINT.RESOLVE_CONFIG_USING_ACCEPTABLE_ARGUMENTS] RUN_ID: {run_id} ; TYPE: {str(type(run_id))}")
        # print(f"\n[ALX_TEST] [CHECKPOINT.RESOLVE_CONFIG_USING_ACCEPTABLE_ARGUMENTS] RUN_NAME: {run_name} ; TYPE: {str(type(run_name))}")
        # print(f"\n[ALEX_TEST] [CHECKPOINT.RESOLVE_CONFIG_USING_ACCEPTABLE_ARGUMENTS] RUN_TIME: {run_time} ; TYPE: {str(type(run_time))}")

        # print(f"\n[ALEX_TEST] [CHECKPOINT.RESOLVE_CONFIG_USING_ACCEPTABLE_ARGUMENTS] RUNTIME_KWARGS:\n{runtime_kwargs} ; TYPE: {str(type(runtime_kwargs))}")
        # deep_filter_properties_iterable(
        #     properties=runtime_kwargs,
        #     clean_falsy=True,
        #     inplace=True,
        # )
        # print(f"\n[ALEX_TEST] [CHECKPOINT.RESOLVE_CONFIG_USING_ACCEPTABLE_ARGUMENTS] RUNTIME_KWARGS-DEEP_CLEANED:\n{runtime_kwargs} ; TYPE: {str(type(runtime_kwargs))}")

        # TODO: <Alex>ALEX</Alex>
        # runtime_kwargs["substituted_runtime_config"] = substituted_runtime_config
        # TODO: <Alex>ALEX</Alex>
        # print(f'\n[ALEX_TEST] [CHECKPOINT.RESOLVE_CONFIG_USING_ACCEPTABLE_ARGUMENTS] BATCH_REQUEST-AT_RETURN:\n{runtime_kwargs["batch_request"]} ; TYPE: {str(type(runtime_kwargs["batch_request"]))}')

        # TODO: <Alex>ALEX</Alex>
        # return runtime_kwargs, substituted_runtime_config
        # TODO: <Alex>ALEX</Alex>
        return substituted_runtime_config
    # TODO: <Alex>ALEX</Alex>

