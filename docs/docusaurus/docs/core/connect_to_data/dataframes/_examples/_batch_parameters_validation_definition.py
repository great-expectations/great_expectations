# Define Batch Parameters for a pandas dataframe
import pandas

csv_path = "./data/folder_with_data/yellow_tripdata_sample_2019-01.csv"
dataframe = pandas.read_csv(csv_path)

batch_parameters = {"dataframe": dataframe}


def set_up_context_for_example(context):
    data_source = context.data_sources.add_pandas(name="my_data_source")
    data_asset = data_source.add_dataframe_asset(name="my_dataframe_data_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        "my_batch_definition"
    )

    # Create an Expectation Suite
    suite = context.suites.add(gx.ExpectationSuite(name="my_expectation_suite"))
    # Add an Expectation to the Expectation Suite
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="pickup_datetime")
    )
    # Add a Validation Definition
    context.validation_definitions.add(
        gx.ValidationDefinition(
            data=batch_definition, suite=suite, name="my_validation_definition"
        )
    )


# <snippet name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_batch_parameters_validation_definition.py - validation_definition.run() example">
import great_expectations as gx

context = gx.get_context()
# Hide this
set_up_context_for_example(context)

# Retrieve a Validation Definition that uses the dataframe Batch Definition
validation_definition_name = "my_validation_definition"
validation_definition = context.validation_definitions.get(validation_definition_name)

# Validate the dataframe by passing it to the Validation Definition as Batch Parameters.
validation_results = validation_definition.run(batch_parameters=batch_parameters)
print(validation_results)
# </snippet>
