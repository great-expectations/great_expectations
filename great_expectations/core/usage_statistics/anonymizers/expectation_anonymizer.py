from great_expectations.core.usage_statistics.anonymizers.base import BaseAnonymizer


class ExpectationAnonymizer(BaseAnonymizer):
    pass

    # def _anonymize_expectation_suite_info(
    #     self, expectation_suite: "ExpectationSuite"  # noqa: F821
    # ) -> dict:
    #     """Anonymize ExpectationSuite objs from the 'great_expectations.core' module.

    #     Args:
    #         expectation_suite ("ExpectationSuite"): An instance of ExpectationSuite.

    #     Returns:
    #         An anonymized dictionary payload that obfuscates user-specific details.
    #     """
    #     anonymized_info_dict = {}
    #     anonymized_expectation_counts = list()

    #     expectations = expectation_suite.expectations
    #     expectation_types = [
    #         expectation.expectation_type for expectation in expectations
    #     ]
    #     for expectation_type in set(expectation_types):
    #         expectation_info = {"count": expectation_types.count(expectation_type)}
    #         self._anonymize_expectation(expectation_type, expectation_info)
    #         anonymized_expectation_counts.append(expectation_info)

    #     anonymized_info_dict["anonymized_name"] = self._anonymize_string(
    #         expectation_suite.expectation_suite_name
    #     )
    #     anonymized_info_dict["expectation_count"] = len(expectations)
    #     anonymized_info_dict[
    #         "anonymized_expectation_counts"
    #     ] = anonymized_expectation_counts

    #     return anonymized_info_dict

    # def _anonymize_expectation(
    #     self, expectation_type: Optional[str], info_dict: dict
    # ) -> None:
    #     """Anonymize Expectation objs from 'great_expectations.expectations'.

    #     Args:
    #         expectation_type (Optional[str]): The string name of the Expectation.
    #         info_dict (dict): A dictionary to update within this function.
    #     """
    #     if expectation_type in self.CORE_GE_EXPECTATION_TYPES:
    #         info_dict["expectation_type"] = expectation_type
    #     else:
    #         info_dict["anonymized_expectation_type"] = self._anonymize_string(
    #             expectation_type
    #         )
