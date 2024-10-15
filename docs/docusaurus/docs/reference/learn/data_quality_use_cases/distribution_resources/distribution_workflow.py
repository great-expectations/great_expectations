"""
To run this test locally, use the postgresql database docker container.

1. From the repo root dir, run:
cd assets/docker/postgresql
docker compose up

2. Run the following command from the repo root dir in a second terminal:
pytest --postgresql --docs-tests -k "data_quality_use_case_volume_workflow" tests/integration/test_script_runner.py
"""

# ruff: noqa: I001
# Adding noqa rule so that GX and Pandas imports don't get reordered by linter.

# <snippet name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/distribution_resources/distribution_workflow.py full workflow">
import great_expectations as gx
import great_expectations.expectations as gxe

# Create Data Context.
context = gx.get_context()

# Connect to sample data, create Data Source and Data Asset.
CONNECTION_STRING = "postgresql+psycopg2://try_gx:try_gx@postgres.workshops.greatexpectations.io/gx_learn_data_quality"

data_source = context.data_sources.add_postgres(
    "postgres database", connection_string=CONNECTION_STRING
)
data_asset = data_source.add_table_asset(
    name="purchases", table_name="distribution_purchases"
)

# Create an Expectation Suite
suite = context.add_expectation_suite(
    expectation_suite_name="purchase_amount_distribution_suite"
)

# Add distribution Expectations
purchase_amount_distribution_expectation = gxe.ExpectColumnKlDivergenceToBeLessThan(
    column="purchase_amount",
    partition_object={
        "bins": [0, 500, 1000, 1500, 2000, 2500, 3000, 3500, 4000],
        "weights": [0.3, 0.2, 0.15, 0.1, 0.1, 0.05, 0.05, 0.05],
    },
    threshold=0.1,
)

purchase_amount_mean_expectation = gxe.ExpectColumnMeanToBeBetween(
    column="purchase_amount", min_value=500, max_value=2000
)
purchase_amount_median_expectation = gxe.ExpectColumnMedianToBeBetween(
    column="purchase_amount", min_value=250, max_value=1500
)

suite.add_expectation(purchase_amount_distribution_expectation)
suite.add_expectation(purchase_amount_mean_expectation)
suite.add_expectation(purchase_amount_median_expectation)

# Validate the data asset and capture the result
validator = context.get_validator(
    batch_request=data_asset.build_batch_request(), expectation_suite=suite
)
results = validator.validate()

# Print the validation results
print(results)
# </snippet>
