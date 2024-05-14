import pathlib
import tempfile

import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite

temp_dir = tempfile.TemporaryDirectory()
full_path_to_project_directory = pathlib.Path(temp_dir.name).resolve()
data_directory = pathlib.Path(
    gx.__file__,
    "..",
    "..",
    "tests",
    "test_sets",
    "taxi_yellow_tripdata_samples",
).resolve(strict=True)

# <snippet name="docs/docusaurus/docs/oss/guides/expectations/how_to_create_and_edit_an_expectationsuite_domain_knowledge.py get_data_context">
import great_expectations as gx

context = gx.get_context(project_root_dir=full_path_to_project_directory)
# </snippet>


# <snippet name="docs/docusaurus/docs/oss/guides/expectations/how_to_create_and_edit_an_expectationsuite_domain_knowledge.py create_expectation_suite">
suite = context.suites.add(ExpectationSuite(name="my_suite"))
# </snippet>

# <snippet name="docs/docusaurus/docs/oss/guides/expectations/how_to_create_and_edit_an_expectationsuite_domain_knowledge.py create_expectation_1">
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)

# Create an Expectation
expectation_configuration_1 = ExpectationConfiguration(
    # Name of expectation type being added
    expectation_type="expect_table_columns_to_match_ordered_list",
    # These are the arguments of the expectation
    # The keys allowed in the dictionary are Parameters and
    # Keyword Arguments of this Expectation Type
    kwargs={
        "column_list": [
            "account_id",
            "user_id",
            "transaction_id",
            "transaction_type",
            "transaction_amt_usd",
        ]
    },
    # This is how you can optionally add a comment about this expectation.
    # It will be rendered in Data Docs.
    # See this guide for details:
    # `How to add comments to Expectations and display them in Data Docs`.
    meta={
        "notes": {
            "format": "markdown",
            "content": "Some clever comment about this expectation. **Markdown** `Supported`",
        }
    },
)
# Add the Expectation to the suite
suite.add_expectation_configuration(
    expectation_configuration=expectation_configuration_1
)
# </snippet>

# <snippet name="docs/docusaurus/docs/oss/guides/expectations/how_to_create_and_edit_an_expectationsuite_domain_knowledge.py create_expectation_2">
expectation_configuration_2 = ExpectationConfiguration(
    expectation_type="expect_column_values_to_be_in_set",
    kwargs={
        "column": "transaction_type",
        "value_set": ["purchase", "refund", "upgrade"],
    },
    # Note optional comments omitted
)
suite.add_expectation_configuration(
    expectation_configuration=expectation_configuration_2
)
# </snippet>

# <snippet name="docs/docusaurus/docs/oss/guides/expectations/how_to_create_and_edit_an_expectationsuite_domain_knowledge.py create_expectation_3">
expectation_configuration_3 = ExpectationConfiguration(
    expectation_type="expect_column_values_to_not_be_null",
    kwargs={
        "column": "account_id",
        "mostly": 1.0,
    },
    meta={
        "notes": {
            "format": "markdown",
            "content": "Some clever comment about this expectation. **Markdown** `Supported`",
        }
    },
)
suite.add_expectation_configuration(
    expectation_configuration=expectation_configuration_3
)
# </snippet>

# <snippet name="docs/docusaurus/docs/oss/guides/expectations/how_to_create_and_edit_an_expectationsuite_domain_knowledge.py create_expectation_4">
expectation_configuration_4 = ExpectationConfiguration(
    expectation_type="expect_column_values_to_not_be_null",
    kwargs={
        "column": "user_id",
        "mostly": 0.75,
    },
    meta={
        "notes": {
            "format": "markdown",
            "content": "Some clever comment about this expectation. **Markdown** `Supported`",
        }
    },
)
suite.add_expectation_configuration(
    expectation_configuration=expectation_configuration_4
)
# </snippet>

# Does the ExpectationSuite contain what we expect
assert len(suite.expectations) == 4
assert suite.expectations[0] == expectation_configuration_1.to_domain_obj()
assert suite.expectations[1] == expectation_configuration_2.to_domain_obj()
assert suite.expectations[2] == expectation_configuration_3.to_domain_obj()
assert suite.expectations[3] == expectation_configuration_4.to_domain_obj()

# <snippet name="docs/docusaurus/docs/oss/guides/expectations/how_to_create_and_edit_an_expectationsuite_domain_knowledge.py save_expectation_suite">
context.save_expectation_suite(expectation_suite=suite)
# </snippet>
