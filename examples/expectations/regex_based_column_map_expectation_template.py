"""
This is a template for creating custom RegexBasedColumnMapExpectations.
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_regex_based_column_map_expectations
"""

from great_expectations.expectations.regex_based_column_map_expectation import (
    RegexBasedColumnMapExpectation,
)


# This class defines the Expectation itself
class ExpectColumnValuesToMatchSomeRegex(RegexBasedColumnMapExpectation):
    """TODO: Add a docstring here"""

    # These values will be used to configure the metric created by your expectation
    regex_camel_name = "RegexName"
    regex = "regex pattern"
    semantic_type_name_plural = None

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = []

    # Here your regex is used to create a custom metric for this expectation
    map_metric = RegexBasedColumnMapExpectation.register_metric(
        regex_camel_name=regex_camel_name,
        regex_=regex,
    )

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "tags": [],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@your_name_here",  # Don't forget to add your github handle here!
        ],
    }


if __name__ == "__main__":
    ExpectColumnValuesToMatchSomeRegex().print_diagnostic_checklist()
