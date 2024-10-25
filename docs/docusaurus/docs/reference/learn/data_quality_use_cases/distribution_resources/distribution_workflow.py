"""
To run this test locally, use the postgresql database docker container.

1. From the repo root dir, run:
cd assets/docker/postgresql
docker compose up

2. Run the following command from the repo root dir in a second terminal:
pytest --postgresql --docs-tests -k "data_quality_use_case_volume_workflow" tests/integration/test_script_runner.py
"""

# <snippet name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/distribution_resources/distribution_workflow.py full workflow">
import great_expectations as gx
import great_expectations.expectations as gxe

# Create Data Context.
context = gx.get_context()

# Connect to data.
# Create Data Source, Data Asset, Batch Definition, and Batch.
CONNECTION_STRING = "postgresql+psycopg2://try_gx:try_gx@postgres.workshops.greatexpectations.io/gx_learn_data_quality"

data_source = context.data_sources.add_postgres(
    "postgres database", connection_string=CONNECTION_STRING
)
data_asset = data_source.add_table_asset(
    name="purchases", table_name="distribution_purchases"
)

batch_definition = data_asset.add_batch_definition_whole_table("batch definition")
batch = batch_definition.get_batch()

# Create an Expectation Suite containing distribution Expectations.
expectation_suite = context.suites.add(
    gx.core.expectation_suite.ExpectationSuite(name="purchase amount expectation suite")
)

purchase_amount_distribution_expectation = gxe.ExpectColumnKLDivergenceToBeLessThan(
    column="purchase_amount",
    partition_object={
        "bins": [0, 500, 1000, 1500, 2000, 2500, 3000, 3500, 4000],
        "weights": [0.3, 0.2, 0.05, 0.1, 0.2, 0.0, 0.05, 0.1],
    },
    threshold=0.1,
)

purchase_amount_mean_expectation = gxe.ExpectColumnMeanToBeBetween(
    column="purchase_amount", min_value=1250, max_value=1500
)

purchase_amount_median_expectation = gxe.ExpectColumnMedianToBeBetween(
    column="purchase_amount", min_value=1000, max_value=1250
)

expectation_suite.add_expectation(purchase_amount_distribution_expectation)
expectation_suite.add_expectation(purchase_amount_mean_expectation)
expectation_suite.add_expectation(purchase_amount_median_expectation)

# Validate Batch using Expectation.
validation_result = batch.validate(expectation_suite)

# Print the validation results.
print(f"Expectation Suite passed: {validation_result['success']}\n")

for result in validation_result["results"]:
    expectation_type = result["expectation_config"]["type"]
    expectation_passed = result["success"]
    print(f"{expectation_type}: {expectation_passed}")
# </snippet>

# Check output matches what is in the docs.

assert validation_result["success"] is True
