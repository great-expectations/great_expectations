from typing import Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.expectations.expectation import (
    ColumnPairMapExpectation,
    InvalidExpectationConfigurationError,
)


class ExpectColumnPairValuesToBeInSet(ColumnPairMapExpectation):
    "\n    Expect paired values from columns A and B to belong to a set of valid pairs.\n\n        For example:\n        ::\n            >>>d = {'fruit': ['appple','apple','apple','banana','banana'], \n                    'color': ['red','green','yellow','yellow','red']}\n            >>>my_df = pd.DataFrame(data=d)\n            >>> my_df.expect_column_pair_values_to_be_in_set('fruit',\n                                                             'color',\n                                                            [ ('apple','red'),\n                                                              ('apple','green'),\n                                                              ('apple','yellow'),\n                                                              ('banana','yellow'),\n                                                            ]\n                                                            )\n            {\n                \"success\": false,\n                \"meta\": {},\n                \"exception_info\": {\n                    \"raised_exception\": false,\n                    \"exception_traceback\": null,\n                    \"exception_message\": null\n                },\n                \"result\": {\n                    \"element_count\": 5,\n                    \"unexpected_count\": 1,\n                    \"unexpected_percent\": 20.0,\n                    \"partial_unexpected_list\": [\n                    [\n                        \"banana\",\n                        \"red\"\n                    ]\n                    ],\n                    \"missing_count\": 0,\n                    \"missing_percent\": 0.0,\n                    \"unexpected_percent_total\": 20.0,\n                    \"unexpected_percent_nonmissing\": 20.0\n                }\n            }\n\n    Args:\n        column_A (str): The first column name\n        column_B (str): The second column name\n        value_pairs_set (list of tuples): All the valid pairs to be matched\n\n    Keyword Args:\n        ignore_row_if (str): \"both_values_are_missing\", \"either_value_is_missing\", \"neither\"\n\n    Other Parameters:\n        result_format (str or None):             Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n        include_config (boolean):             If True, then include the expectation config as part of the result object.         catch_exceptions (boolean or None):             If True, then catch exceptions and include them as part of the result object.         meta (dict or None):             A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification.\n\n    Returns:\n        An ExpectationSuiteValidationResult\n"
    library_metadata = {
        "maturity": "production",
        "tags": ["core expectation", "multi-column expectation"],
        "contributors": ["@great_expectations"],
        "requirements": [],
        "has_full_test_suite": True,
        "manually_reviewed_code": True,
    }
    map_metric = "column_pair_values.in_set"
    success_keys = ("value_pairs_set", "ignore_row_if", "mostly")
    default_kwarg_values = {
        "mostly": 1.0,
        "ignore_row_if": "both_values_are_missing",
        "row_condition": None,
        "condition_parser": None,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }
    args_keys = ("column_A", "column_B", "value_pairs_set")

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration]
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        super().validate_configuration(configuration)
        if configuration is None:
            configuration = self.configuration
        try:
            assert ("column_A" in configuration.kwargs) and (
                "column_B" in configuration.kwargs
            ), "both columns must be provided"
            assert (
                "value_pairs_set" in configuration.kwargs
            ), "must provide value_pairs_set"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))
