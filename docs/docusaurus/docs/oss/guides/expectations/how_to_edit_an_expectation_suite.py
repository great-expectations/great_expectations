# ruff: noqa: I001, E401, B018

# <snippet name="docs/docusaurus/docs/oss/guides/expectations/how_to_edit_an_expectation_suite.py import_expectation_configuration">
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)

# </snippet>

# NOTE: The following code is only for testing and can be ignored by users.
import sys, io


stdout = sys.stdout
sys.stdout = io.StringIO()

# <snippet name="docs/docusaurus/docs/oss/guides/expectations/how_to_edit_an_expectation_suite.py get_context">
import great_expectations as gx
import great_expectations.expectations as gxe

context = gx.get_context()
# </snippet>

# <snippet name="docs/docusaurus/docs/oss/guides/expectations/how_to_edit_an_expectation_suite.py create_validator">
context.data_sources.pandas_default.read_csv(
    "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
)
# </snippet>


my_suite = context.suites.add(ExpectationSuite(name="my_suite"))
my_suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="pickup_datetime"))
my_suite.add_expectation(
    gxe.ExpectColumnValuesToBeBetween(
        column="passenger_count", min_value=1, max_value=6
    )
)

# <snippet name="docs/docusaurus/docs/oss/guides/expectations/how_to_edit_an_expectation_suite.py show_suite">
my_suite.show_expectations_by_expectation_type()
# </snippet>

# <snippet name="docs/docusaurus/docs/oss/guides/expectations/how_to_edit_an_expectation_suite.py example_dict_1">

{
    "expect_column_values_to_be_between": {
        "column": "passenger_count",
        "max_value": 6,
        "min_value": 1,
        "mostly": 1.0,
        "strict_max": False,
        "strict_min": False,
    }
}
# </snippet>

# <snippet name="docs/docusaurus/docs/oss/guides/expectations/how_to_edit_an_expectation_suite.py example_configuration_1">
config = ExpectationConfiguration(
    type="expect_column_values_to_be_between",
    kwargs={
        "column": "passenger_count",
        "max_value": 6,
        "min_value": 1,
        "mostly": 1.0,
        "strict_max": False,
        "strict_min": False,
    },
)
# </snippet>


# <snippet name="docs/docusaurus/docs/oss/guides/expectations/how_to_edit_an_expectation_suite.py updated_configuration">
updated_config = ExpectationConfiguration(
    type="expect_column_values_to_be_between",
    kwargs={
        "column": "passenger_count",
        "min_value": 1,
        "max_value": 4,
        #'max_value': 6,
        "mostly": 1.0,
        "strict_max": False,
        "strict_min": False,
    },
)
# </snippet>

# <snippet name="docs/docusaurus/docs/oss/guides/expectations/how_to_edit_an_expectation_suite.py add_configuration">
my_suite.add_expectation_configuration(updated_config)
# </snippet>

assert len(my_suite.expectations) == 2
assert isinstance(my_suite.expectations[0], gxe.ExpectColumnValuesToNotBeNull)
assert isinstance(my_suite.expectations[1], gxe.ExpectColumnValuesToBeBetween)
assert my_suite.expectations[1] == updated_config.to_domain_obj()


# <snippet name="docs/docusaurus/docs/oss/guides/expectations/how_to_edit_an_expectation_suite.py find_configuration">
config_to_search = ExpectationConfiguration(
    type="expect_column_values_to_be_between",
    kwargs={"column": "passenger_count"},
)
found_expectation = my_suite.find_expectations(config_to_search, match_type="domain")

# This assertion will succeed because the ExpectationConfiguration has been updated.
assert len(found_expectation) == 1
# </snippet>
assert found_expectation[0].to_domain_obj() == updated_config.to_domain_obj()

# <snippet name="docs/docusaurus/docs/oss/guides/expectations/how_to_edit_an_expectation_suite.py remove_configuration">
config_to_remove = ExpectationConfiguration(
    type="expect_column_values_to_be_between",
    kwargs={"column": "passenger_count"},
)
my_suite.remove_expectation(
    config_to_remove, match_type="domain", remove_multiple_matches=False
)

found_expectation = my_suite.find_expectations(config_to_remove, match_type="domain")

# This assertion will fail because the ExpectationConfiguration has been removed.
assert found_expectation != [updated_config]
my_suite.show_expectations_by_expectation_type()
# </snippet>

assert len(my_suite.expectations) == 1
assert my_suite.expectation_configurations[0] == ExpectationConfiguration(
    type="expect_column_values_to_not_be_null",
    kwargs={"column": "pickup_datetime"},
)

# <snippet name="docs/docusaurus/docs/oss/guides/expectations/how_to_edit_an_expectation_suite.py save_suite">
context.save_expectation_suite(my_suite)
# </snippet>
