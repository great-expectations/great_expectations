"""
This is a template for creating custom RegexBasedColumnMapExpectations.
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_regex_based_column_map_expectations
"""

from typing import Dict, Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.exceptions.exceptions import (
    InvalidExpectationConfigurationError,
)
from great_expectations.expectations.regex_based_column_map_expectation import (
    RegexBasedColumnMapExpectation,
    RegexColumnMapMetricProvider,
)


# <snippet>
# This class defines the Expectation itself
class ExpectColumnValuesToBeValidGeohash(RegexBasedColumnMapExpectation):
    """Expect values in this column to be a valid geohash."""

    # These values will be used to configure the metric created by your expectation
    regex_camel_name = "Geohash"
    regex = "^[0123456789bcdefghjkmnpqrstuvwxyz]+$"
    semantic_type_name_plural = "geohashes"

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "valid_geohash": ["dpz8"],
                "invalid_alphanumeric": ["apz8"],  # "a" is an invalid geohash char
                "invalid_non_alphanumeric": ["dp2-"],
                "empty": [""],
            },
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "valid_geohash"},
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "invalid_alphanumeric", "mostly": 1},
                    "out": {
                        "success": False,
                    },
                },
                {
                    "title": "invalid_non_alphanumeric",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "invalid_non_alphanumeric", "mostly": 1},
                    "out": {
                        "success": False,
                    },
                },
                {
                    "title": "empty",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "empty", "mostly": 1},
                    "out": {
                        "success": False,
                    },
                },
            ],
        }
    ]

    # Here your regex is used to create a custom metric for this expectation
    map_metric = RegexBasedColumnMapExpectation.register_metric(
        regex_camel_name=regex_camel_name,
        regex_=regex,
    )

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "tags": [
            "geospatial",
            "hackathon-22",
        ],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@chrisarnold91",  # Don't forget to add your github handle here!
        ],
    }


# </snippet>
if __name__ == "__main__":
    ExpectColumnValuesToBeValidGeohash().print_diagnostic_checklist()
