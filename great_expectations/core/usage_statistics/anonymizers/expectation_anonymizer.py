from typing import Optional

from great_expectations.core.usage_statistics.anonymizers.base import BaseAnonymizer


class ExpectationSuiteAnonymizer(BaseAnonymizer):
    def __init__(
        self, aggregate_anonymizer: "Anonymizer", salt: Optional[str] = None
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        super().__init__(salt=salt)
        self._aggregate_anonymizer = aggregate_anonymizer

    def anonymize(self, obj: Optional["ExpectationSuite"] = None, **kwargs) -> dict:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        expectation_suite = obj
        anonymized_info_dict = {}
        anonymized_expectation_counts = list()
        expectations = expectation_suite.expectations
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
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Anonymize Expectation objs from 'great_expectations.expectations'.\n\n        Args:\n            expectation_type (Optional[str]): The string name of the Expectation.\n            info_dict (dict): A dictionary to update within this function.\n        "
        if expectation_type in self.CORE_GE_EXPECTATION_TYPES:
            info_dict["expectation_type"] = expectation_type
        else:
            info_dict["anonymized_expectation_type"] = self._anonymize_string(
                expectation_type
            )

    def can_handle(self, obj: Optional[object] = None, **kwargs) -> bool:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return "expectation_suite" in kwargs
