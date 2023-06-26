from typing import Optional

from great_expectations.core.usage_statistics.anonymizers.base import BaseAnonymizer


class ExpectationSuiteAnonymizer(BaseAnonymizer):
    def __init__(
        self,
        aggregate_anonymizer: "Anonymizer",  # noqa: F821
        salt: Optional[str] = None,
    ) -> None:
        super().__init__(salt=salt)

        self._aggregate_anonymizer = aggregate_anonymizer

    def anonymize(
        self, obj: Optional["ExpectationSuite"] = None, **kwargs  # noqa: F821
    ) -> dict:
        # Exit early if null
        expectation_suite = obj
        if not expectation_suite:
            return {}

        # Exit early if no expectations to anonymize
        expectations = expectation_suite.expectations
        if not expectations:
            return {}

        anonymized_info_dict = {}
        anonymized_expectation_counts = list()

        expectation_types = [
            expectation.expectation_type for expectation in expectations
        ]
        for expectation_type in set(expectation_types):
            expectation_info = {"count": expectation_types.count(expectation_type)}
            self._anonymize_expectation(expectation_type, expectation_info)
            anonymized_expectation_counts.append(expectation_info)

        anonymized_info_dict["anonymized_name"] = self._anonymize_string(
            expectation_suite.expectation_suite_name
        )
        anonymized_info_dict["expectation_count"] = len(expectations)
        anonymized_info_dict[
            "anonymized_expectation_counts"
        ] = anonymized_expectation_counts

        return anonymized_info_dict

    def _anonymize_expectation(
        self, expectation_type: Optional[str], info_dict: dict
    ) -> None:
        """Anonymize Expectation objs from 'great_expectations.expectations'.

        Args:
            expectation_type (Optional[str]): The string name of the Expectation.
            info_dict (dict): A dictionary to update within this function.
        """
        if expectation_type in self.CORE_GX_EXPECTATION_TYPES:
            info_dict["expectation_type"] = expectation_type
        else:
            info_dict["anonymized_expectation_type"] = self._anonymize_string(
                expectation_type
            )

    def can_handle(self, obj: Optional[object] = None, **kwargs) -> bool:
        return "expectation_suite" in kwargs
