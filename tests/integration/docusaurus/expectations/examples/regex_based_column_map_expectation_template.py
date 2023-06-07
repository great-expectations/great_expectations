"""
This is a template for creating custom RegexBasedColumnMapExpectations.
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_regex_based_column_map_expectations
"""

from great_expectations.expectations.regex_based_column_map_expectation import (
    RegexBasedColumnMapExpectation,
)


# This class defines the Expectation itself
# <snippet name="tests/integration/docusaurus/expectations/examples/regex_based_column_map_expectation_template.py ExpectColumnValuesToMatchSomeRegex class_def">
class ExpectColumnValuesToMatchSomeRegex(RegexBasedColumnMapExpectation):
    # </snippet>
    # <snippet name="tests/integration/docusaurus/expectations/examples/regex_based_column_map_expectation_template.py docstring">
    """TODO: Add a docstring here"""
    # </snippet>

    # These values will be used to configure the metric created by your expectation
    # <snippet name="tests/integration/docusaurus/expectations/examples/regex_based_column_map_expectation_template.py definition">
    regex_camel_name = "RegexName"
    regex = "regex pattern"
    # </snippet>
    # <snippet name="tests/integration/docusaurus/expectations/examples/regex_based_column_map_expectation_template.py plural">
    semantic_type_name_plural = None
    # </snippet>

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = []

    # Here your regex is used to create a custom metric for this expectation
    map_metric = RegexBasedColumnMapExpectation.register_metric(
        regex_camel_name=regex_camel_name,
        regex_=regex,
    )

    # This object contains metadata for display in the public Gallery
    # <snippet name="tests/integration/docusaurus/expectations/examples/regex_based_column_map_expectation_template.py library_metadata">
    library_metadata = {
        "tags": [],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@your_name_here",  # Don't forget to add your github handle here!
        ],
    }


# </snippet>
if __name__ == "__main__":
    # <snippet name="tests/integration/docusaurus/expectations/examples/regex_based_column_map_expectation_template.py diagnostics">
    ExpectColumnValuesToMatchSomeRegex().print_diagnostic_checklist()
#     </snippet>
