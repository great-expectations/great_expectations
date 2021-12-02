from great_expectations.core.usage_statistics.anonymizers.anonymizer import Anonymizer
from great_expectations.validation_operators import (
    MicrosoftTeamsNotificationAction,
    NoOpAction,
    PagerdutyAlertAction,
    SlackNotificationAction,
    StoreEvaluationParametersAction,
    StoreMetricsAction,
    StoreValidationResultAction,
    UpdateDataDocsAction,
    ValidationAction,
)


class ActionAnonymizer(Anonymizer):
    def __init__(self, salt=None):
        super().__init__(salt=salt)

        # ordered bottom up in terms of inheritance order
        self._ge_classes = [
            StoreMetricsAction,
            NoOpAction,
            StoreValidationResultAction,
            StoreEvaluationParametersAction,
            SlackNotificationAction,
            PagerdutyAlertAction,
            MicrosoftTeamsNotificationAction,
            UpdateDataDocsAction,
            ValidationAction,
        ]

    def anonymize_action_info(self, action_name, action_obj=None, action_config=None):
        print(f'\n[ALEX_TEST] [ACTION_ANONYMIZER.ANONYMIZE_ACTION_INFO] ACTION_NAME: {action_name} ; TYPE: {str(type(action_name))}')
        print(f'\n[ALEX_TEST] [ACTION_ANONYMIZER.ANONYMIZE_ACTION_INFO] ACTION_OBJ: {action_obj} ; TYPE: {str(type(action_obj))}')
        print(f'\n[ALEX_TEST] [ACTION_ANONYMIZER.ANONYMIZE_ACTION_INFO] ACTION_CONFIG: {action_config} ; TYPE: {str(type(action_config))}')
        anonymized_info_dict: dict = {
            "anonymized_name": self.anonymize(action_name),
        }
        print(f'\n[ALEX_TEST] [ACTION_ANONYMIZER.ANONYMIZE_ACTION_INFO] ACTION_ANONYMIZED_INFO_DICT-0: {anonymized_info_dict} ; TYPE: {str(type(anonymized_info_dict))}')

        self.anonymize_object_info(
            object_=action_obj,
            object_config=action_config,
            anonymized_info_dict=anonymized_info_dict,
            ge_classes=self._ge_classes,
            runtime_environment={"module_name": "great_expectations.checkpoint"}
        )

        # TODO: <Alex>ALEX</Alex>
        # anonymized_info_dict.update(
        #     self.anonymize_object_info(
        #         object_=action_obj,
        #         anonymized_info_dict=anonymized_info_dict,
        #         ge_classes=self._ge_classes,
        #     )
        # )
        #
        print(f'\n[ALEX_TEST] [ACTION_ANONYMIZER.ANONYMIZE_ACTION_INFO] RETURNING_ACTION_ANONYMIZED_INFO_DICT: {anonymized_info_dict} ; TYPE: {str(type(anonymized_info_dict))}')
        return anonymized_info_dict
        # TODO: <Alex>ALEX</Alex>
        # return self.anonymize_object_info(
        #     object_=action_obj,
        #     anonymized_info_dict=anonymized_info_dict,
        #     ge_classes=self._ge_classes,
        # )
