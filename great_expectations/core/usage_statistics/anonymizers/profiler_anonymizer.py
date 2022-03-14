from great_expectations.core.usage_statistics.anonymizers.anonymizer import Anonymizer


class ProfilerAnonymizer(Anonymizer):
    def __init__(self, salt=None):
        super().__init__(salt=salt)

    def anonymize_profiler_info(self, name: str, config: dict) -> dict:
        anonymized_info_dict: dict = {
            "anonymized_name": self.anonymize(name),
        }
        self.anonymize_object_info(
            anonymized_info_dict=anonymized_info_dict,
            object_config=config,
        )
        return anonymized_info_dict
