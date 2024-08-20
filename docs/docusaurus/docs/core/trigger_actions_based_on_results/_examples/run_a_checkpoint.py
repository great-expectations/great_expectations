"""
This is an example script for how to run a Checkpoint.

To test, run:
pytest --docs-tests -k "docs_example_run_a_checkpoint" tests/integration/test_script_runner.py
"""


def set_up_context_for_example(context):
    # Create a Batch Definition (monthly)
    source_folder = "./data/folder_with_data"
    data_source_name = "my_data_source"
    asset_name = "my_data_asset"
    batch_definition_name = "my_batch_definition"
    batch_definition_regex = (
        r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"
    )
    batch_definition = (
        context.data_sources.add_pandas_filesystem(
            name=data_source_name, base_directory=source_folder
        )
        .add_csv_asset(name=asset_name)
        .add_batch_definition_monthly(
            name=batch_definition_name, regex=batch_definition_regex
        )
    )
    assert batch_definition.name == batch_definition_name

    # Create an Expectation Suite
    expectation_suite = context.suites.add(
        gx.ExpectationSuite(name="my_expectation_suite")
    )
    # Add some Expectations
    expectation_suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="pickup_datetime")
    )
    expectation_suite.add_expectation(
        # <snippet name="docs/docusaurus/docs/core/trigger_actions_based_on_results/_examples/run_a_checkpoint.py - example Expectation">
        gx.expectations.ExpectColumnMaxToBeBetween(
            column="fare",
            min_value={"$PARAMETER": "expect_fare_max_to_be_above"},
            max_value={"$PARAMETER": "expect_fare_max_to_be_below"},
        )
        # </snippet>
    )

    # Create a Validation Definition
    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(
            data=batch_definition,
            suite=expectation_suite,
            name="my_validation_definition",
        )
    )

    # Create a Checkpoint
    checkpoint_name = "my_checkpoint"
    validation_definitions = [validation_definition]
    action_list = []
    context.checkpoints.add(
        gx.Checkpoint(
            name=checkpoint_name,
            validation_definitions=validation_definitions,
            actions=action_list,
            result_format={"result_format": "COMPLETE"},
        )
    )


# EXAMPLE SCRIPT STARTS HERE:
# <snippet name="docs/docusaurus/docs/core/trigger_actions_based_on_results/_examples/run_a_checkpoint.py - full code example">
import great_expectations as gx

context = gx.get_context()
# Hide this
set_up_context_for_example(context)

checkpoint = context.checkpoints.get("my_checkpoint")

batch_parameters = {"month": "01", "year": "2019"}

# <snippet name="docs/docusaurus/docs/core/trigger_actions_based_on_results/_examples/run_a_checkpoint.py - define Expectation Parameters">
expectation_parameters = {
    "expect_fare_max_to_be_above": 5.00,
    "expect_fare_max_to_be_below": 1000.00,
}
# </snippet>

# <snippet name="docs/docusaurus/docs/core/trigger_actions_based_on_results/_examples/run_a_checkpoint.py - run a Checkpoint">
validation_results = checkpoint.run(
    batch_parameters=batch_parameters, expectation_parameters=expectation_parameters
)
# </snippet>
# </snippet>
