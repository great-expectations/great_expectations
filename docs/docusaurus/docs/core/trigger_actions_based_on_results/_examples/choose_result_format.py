"""
This is an example script for how to choose a Result Format.

To test, run:
pytest --docs-tests -k "docs_example_choose_result_format" tests/integration/test_script_runner.py
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
    # ColumnMap
    expectation = gx.expectations.ExpectColumnValuesToBeInSet(
        column="passenger_count", value_set=[1, 2, 3, 4, 5]
    )
    expectation_suite.add_expectation(expectation)
    # ColumnAggregate
    expectation = gx.expectations.ExpectColumnMaxToBeBetween(
        column="passenger_count", min_value=4, max_value=5
    )
    expectation_suite.add_expectation(expectation)

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
# <snippet name="docs/docusaurus/docs/core/trigger_actions_based_on_results/_examples/choose_result_format.py - full code example">
import great_expectations as gx

context = gx.get_context()
# Hide this
set_up_context_for_example(context)

validation_name = "my_validation_definition"
validation_definition = context.validation_definitions.get(validation_name)

checkpoint_name = "my_checkpoint"
checkpoint = context.checkpoints.get(checkpoint_name)

# BOOLEAN_ONLY Result Format
# <snippet name="docs/docusaurus/docs/core/trigger_actions_based_on_results/_examples/choose_result_format.py - boolean_only Result Format">
boolean_result_format_dict = {"result_format": "BOOLEAN_ONLY"}
# </snippet>
validation_definition.run(result_format=boolean_result_format_dict)
# checkpoint.run(result_format=boolean_result_format_dict)

# BASIC Result Format
# <snippet name="docs/docusaurus/docs/core/trigger_actions_based_on_results/_examples/choose_result_format.py - basic Result Format">
basic_result_format_dict = {"result_format": "BASIC"}
# </snippet>
validation_definition.run(result_format=basic_result_format_dict)
# checkpoint.run(result_format=basic_result_format_dict)

# SUMMARY Result Format
# <snippet name="docs/docusaurus/docs/core/trigger_actions_based_on_results/_examples/choose_result_format.py - summary Result Format">
summary_result_format_dict = {"result_format": "SUMMARY"}
# </snippet>
validation_definition.run(result_format=summary_result_format_dict)
# checkpoint.run(result_format=summary_result_format_dict)

# COMPLETE Result Format
# <snippet name="docs/docusaurus/docs/core/trigger_actions_based_on_results/_examples/choose_result_format.py - complete Result Format">
complete_result_format_dict = {"result_format": "COMPLETE"}
# </snippet>
validation_definition.run(result_format=complete_result_format_dict)
# checkpoint.run(result_format=complete_result_format_dict)
# </snippet>
