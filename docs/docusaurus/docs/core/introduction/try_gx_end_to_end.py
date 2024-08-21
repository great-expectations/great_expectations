"""
This example script allows the user to try out GX by validating Expectations
 against sample data.

The snippet tags are used to insert the corresponding code into the
 Great Expectations documentation.  They can be disregarded by anyone
 reviewing this script.
"""

# <snippet name="docs/docusaurus/docs/core/introduction/try_gx_end_to_end.py full end2end script">

# Import required modules from GX library.
# <snippet name="docs/docusaurus/docs/core/introduction/try_gx_end_to_end.py import gx library">
import great_expectations as gx

# </snippet>
# Create Data Context.
# <snippet name="docs/docusaurus/docs/core/introduction/try_gx_end_to_end.py create data context">
context = gx.get_context()
# </snippet>

# Connect to data.
# Create Data Source, Data Asset, Batch Definition, and Batch.
# <snippet name="docs/docusaurus/docs/core/introduction/try_gx_end_to_end.py connect to data and get batch">
connection_string = "postgresql+psycopg2://try_gx:try_gx@postgres.workshops.greatexpectations.io/gx_example_db"

data_source = context.data_sources.add_postgres(
    "postgres db", connection_string=connection_string
)
data_asset = data_source.add_table_asset(name="taxi data", table_name="nyc_taxi_data")

batch_definition = data_asset.add_batch_definition_whole_table("batch definition")
batch = batch_definition.get_batch()
# </snippet>

# Create Expectation Suite containing two Expectations.
# <snippet name="docs/docusaurus/docs/core/introduction/try_gx_end_to_end.py create expectation suite">
suite = context.suites.add(
    gx.core.expectation_suite.ExpectationSuite(name="expectations")
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="passenger_count", min_value=1, max_value=6
    )
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(column="fare_amount", min_value=0)
)
# </snippet>

# Create Validation Definition.
# <snippet name="docs/docusaurus/docs/core/introduction/try_gx_end_to_end.py create validation definition">
validation_definition = context.validation_definitions.add(
    gx.core.validation_definition.ValidationDefinition(
        name="validation definition",
        data=batch_definition,
        suite=suite,
    )
)
# </snippet>

# Create Checkpoint, run Checkpoint, and capture result.
# <snippet name="docs/docusaurus/docs/core/introduction/try_gx_end_to_end.py create and run checkpoint">
checkpoint = context.checkpoints.add(
    gx.checkpoint.checkpoint.Checkpoint(
        name="checkpoint", validation_definitions=[validation_definition]
    )
)

checkpoint_result = checkpoint.run()
print(checkpoint_result.describe())
# </snippet>

# </snippet>
# Above snippet ends the full end-to-end script.

end_to_end_output = """
# <snippet name="docs/docusaurus/docs/core/introduction/try_gx_end_to_end.py checkpoint result">
{
    "success": false,
    "statistics": {
        "evaluated_validations": 1,
        "success_percent": 0.0,
        "successful_validations": 0,
        "unsuccessful_validations": 1
    },
    "validation_results": [
        {
            "success": false,
            "statistics": {
                "evaluated_expectations": 2,
                "successful_expectations": 1,
                "unsuccessful_expectations": 1,
                "success_percent": 50.0
            },
            "expectations": [
                {
                    "expectation_type": "expect_column_values_to_be_between",
                    "success": true,
                    "kwargs": {
                        "batch_id": "postgres db-taxi data",
                        "column": "passenger_count",
                        "min_value": 1.0,
                        "max_value": 6.0
                    },
                    "result": {
                        "element_count": 20000,
                        "unexpected_count": 0,
                        "unexpected_percent": 0.0,
                        "partial_unexpected_list": [],
                        "missing_count": 0,
                        "missing_percent": 0.0,
                        "unexpected_percent_total": 0.0,
                        "unexpected_percent_nonmissing": 0.0,
                        "partial_unexpected_counts": []
                    }
                },
                {
                    "expectation_type": "expect_column_values_to_be_between",
                    "success": false,
                    "kwargs": {
                        "batch_id": "postgres db-taxi data",
                        "column": "fare_amount",
                        "min_value": 0.0
                    },
                    "result": {
                        "element_count": 20000,
                        "unexpected_count": 14,
                        "unexpected_percent": 0.06999999999999999,
                        "partial_unexpected_list": [
                            -0.01,
                            -52.0,
                            -0.1,
                            -5.5,
                            -3.0,
                            -52.0,
                            -4.0,
                            -0.01,
                            -52.0,
                            -0.1,
                            -5.5,
                            -3.0,
                            -52.0,
                            -4.0
                        ],
                        "missing_count": 0,
                        "missing_percent": 0.0,
                        "unexpected_percent_total": 0.06999999999999999,
                        "unexpected_percent_nonmissing": 0.06999999999999999,
                        "partial_unexpected_counts": [
                            {
                                "value": -52.0,
                                "count": 4
                            },
                            {
                                "value": -5.5,
                                "count": 2
                            },
                            {
                                "value": -4.0,
                                "count": 2
                            },
                            {
                                "value": -3.0,
                                "count": 2
                            },
                            {
                                "value": -0.1,
                                "count": 2
                            },
                            {
                                "value": -0.01,
                                "count": 2
                            }
                        ]
                    }
                }
            ],
            "result_url": null
        }
    ]
}
# </snippet>
"""

checkpoint_summary = checkpoint_result.describe_dict()

assert checkpoint_summary["success"] is False
assert len(checkpoint_summary["validation_results"][0]["expectations"]) == 2
