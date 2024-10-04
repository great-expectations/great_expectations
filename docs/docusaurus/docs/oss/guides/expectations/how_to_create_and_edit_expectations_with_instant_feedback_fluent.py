# <snippet name="docs/docusaurus/docs/oss/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_fluent.py imports and data context">
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite

context = gx.get_context()
# </snippet>

context.data_sources.add_pandas(
    name="my_datasource",
).add_csv_asset(
    name="my_data_asset",
    filepath_or_buffer="./data/yellow_tripdata_sample_2019-01.csv",
)

# <snippet name="docs/docusaurus/docs/oss/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_fluent.py get_data_asset_and_build_batch_request">
data_asset = context.data_sources.get("my_datasource").get_asset("my_data_asset")
batch_request = data_asset.build_batch_request()
# </snippet>

# <snippet name="docs/docusaurus/docs/oss/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_fluent.py create_expectation_suite">
context.suites.add(ExpectationSuite(name="my_expectation_suite"))
# Optional. Run assert "my_expectation_suite" in context.suites.all() to veriify the Expectation Suite was created.
# </snippet>

# <snippet name="docs/docusaurus/docs/oss/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_fluent.py get_validator_and_inspect_data">
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name="my_expectation_suite",
)
validator.head()
# </snippet>

# this snippet is only for users who are not using a jupyter notebook
# <snippet name="docs/docusaurus/docs/oss/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_fluent.py inspect_data_no_jupyter">
print(validator.head())
# </snippet>

# <snippet name="docs/docusaurus/docs/oss/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_fluent.py interactive_validation">
validator.expect_column_values_to_not_be_null(column="vendor_id")
# </snippet>

# this snippet is only for users who are not using a jupyter notebook
# <snippet name="docs/docusaurus/docs/oss/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_fluent.py interactive_validation_no_jupyter">
expectation_validation_result = validator.expect_column_values_to_not_be_null(
    column="vendor_id"
)
print(expectation_validation_result)
# </snippet>

# <snippet name="docs/docusaurus/docs/oss/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_fluent.py save_expectation_suite">
validator.save_expectation_suite(discard_failed_expectations=False)
# </snippet>
