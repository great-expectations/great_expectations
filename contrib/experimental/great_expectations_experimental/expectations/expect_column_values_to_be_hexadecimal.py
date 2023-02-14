from typing import Optional

from great_expectations.core import ExpectationConfiguration
from great_expectations.expectations.regex_based_column_map_expectation import (
    RegexBasedColumnMapExpectation,
)


class ExpectColumnValuesToBeHexadecimal(RegexBasedColumnMapExpectation):
    """Expect column values to be valid hexadecimals."""

    regex_camel_name = "HexadecimalNumber"
    regex = r"^[0-9a-fA-F]+$"
    semantic_type_name_plural = "hexadecimals"

    map_metric = RegexBasedColumnMapExpectation.register_metric(
        regex_camel_name=regex_camel_name,
        regex_=regex,
    )

    library_metadata = {
        "maturity": "experimental",
        "tags": ["experimental"],
        "contributors": [
            "@andrewsx",
            "@mkopec87",
        ],
    }

    examples = [
        {
            "data": {
                "a": ["3", "aa", "ba", "5A", "60F", "Gh"],
                "b": ["Verify", "String", "3Z", "X", "yy", "sun"],
                "c": ["0", "BB", "21D", "ca", "20", "1521D"],
                "d": ["c8", "ffB", "11x", "apple", "ran", "woven"],
                "e": ["a8", "21", "2.0", "1B", "4AA", "31"],
                "f": ["a8", "41", "ca", "", "0", "31"],
            },
            "suppress_test_for": ["mssql", "bigquery", "snowflake"],
            "tests": [
                {
                    "title": "positive_test_with_mostly",
                    "include_in_gallery": True,
                    "exact_match_out": False,
                    "in": {"column": "a", "mostly": 0.6},
                    "out": {
                        "success": True,
                        "unexpected_index_list": [5],
                        "unexpected_list": ["Gh"],
                    },
                },
                {
                    "title": "negative_test_without_mostly",
                    "include_in_gallery": True,
                    "exact_match_out": False,
                    "in": {"column": "b"},
                    "out": {
                        "success": False,
                        "unexpected_index_list": [0, 1, 2, 3, 4, 5],
                        "unexpected_list": ["Verify", "String", "3Z", "X", "yy", "sun"],
                    },
                },
                {
                    "title": "positive_test_without_mostly",
                    "include_in_gallery": True,
                    "exact_match_out": False,
                    "in": {"column": "c"},
                    "out": {
                        "success": True,
                        "unexpected_index_list": [],
                        "unexpected_list": [],
                    },
                },
                {
                    "title": "negative_test_with_mostly",
                    "include_in_gallery": True,
                    "exact_match_out": False,
                    "in": {"column": "d", "mostly": 0.6},
                    "out": {
                        "success": False,
                        "unexpected_index_list": [2, 3, 4, 5],
                        "unexpected_list": ["11x", "apple", "ran", "woven"],
                    },
                },
                {
                    "title": "negative_test_with_float",
                    "include_in_gallery": True,
                    "exact_match_out": False,
                    "in": {"column": "e"},
                    "out": {
                        "success": False,
                        "unexpected_index_list": [2],
                        "unexpected_list": ["2.0"],
                    },
                },
                {
                    "title": "negative_test_with_empty_value",
                    "include_in_gallery": True,
                    "exact_match_out": False,
                    "in": {"column": "f"},
                    "out": {
                        "success": False,
                        "unexpected_index_list": [3],
                        "unexpected_list": [""],
                    },
                },
            ],
        }
    ]

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        necessary configuration arguments have been provided for the validation of the expectation.

        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            None. Raises InvalidExpectationConfigurationError if the config is not validated successfully
        """

        super().validate_configuration(configuration)


if __name__ == "__main__":
    ExpectColumnValuesToBeHexadecimal().print_diagnostic_checklist()
