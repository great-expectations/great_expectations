"""
This is a template for creating custom SetBasedColumnMapExpectations.
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_set_based_column_map_expectations
"""

from great_expectations.expectations.set_based_column_map_expectation import (
    SetBasedColumnMapExpectation,
)


# This class defines the Expectation itself
# <snippet name="tests/integration/docusaurus/expectations/examples/set_based_column_map_expectation_template.py ExpectColumnValuesToBeInSomeSet class_def">
class ExpectColumnValuesToBeInSomeSet(SetBasedColumnMapExpectation):
    # </snippet>
    # <snippet name="tests/integration/docusaurus/expectations/examples/set_based_column_map_expectation_template.py docstring">
    """TODO: Add a docstring here"""
    # </snippet>

    # These values will be used to configure the metric created by your expectation
    # <snippet name="tests/integration/docusaurus/expectations/examples/set_based_column_map_expectation_template.py set">
    set_ = []

    set_camel_name = "SetName"
    # </snippet>
    # <snippet name="tests/integration/docusaurus/expectations/examples/set_based_column_map_expectation_template.py semantic_name">
    set_semantic_name = None
    # </snippet>

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    # <snippet name="tests/integration/docusaurus/expectations/examples/set_based_column_map_expectation_template.py examples">
    examples = []
    # </snippet>

    # Here your regex is used to create a custom metric for this expectation
    map_metric = SetBasedColumnMapExpectation.register_metric(
        set_camel_name=set_camel_name,
        set_=set_,
    )

    # This object contains metadata for display in the public Gallery
    # <snippet name="tests/integration/docusaurus/expectations/examples/set_based_column_map_expectation_template.py library_metadata">
    library_metadata = {
        "tags": ["set-based"],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@your_name_here",  # Don't forget to add your github handle here!
        ],
    }


# </snippet>
if __name__ == "__main__":
    # <snippet name="tests/integration/docusaurus/expectations/examples/set_based_column_map_expectation_template.py diagnostics">
    ExpectColumnValuesToBeInSomeSet().print_diagnostic_checklist()
#     </snippet>
