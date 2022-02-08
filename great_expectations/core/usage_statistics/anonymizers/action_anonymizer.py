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
        anonymized_info_dict: dict = {
            "anonymized_name": self.anonymize(action_name),
        }

        self.anonymize_object_info(
            object_=action_obj,
            object_config=action_config,
            anonymized_info_dict=anonymized_info_dict,
            ge_classes=self._ge_classes,
            runtime_environment={"module_name": "great_expectations.checkpoint"},
        )

        return anonymized_info_dict
