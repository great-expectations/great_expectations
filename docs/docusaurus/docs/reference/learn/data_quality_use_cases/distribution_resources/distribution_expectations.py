"""
To run this test locally, use the postgresql database docker container.

1. From the repo root dir, run:
cd assets/docker/postgresql
docker compose up

2. Run the following command from the repo root dir in a second terminal:
pytest --postgresql --docs-tests -k "data_quality_use_case_distribution_expectations" tests/integration/test_script_runner.py
"""

# This section loads sample data to use for CI testing of the script.
import pathlib

import great_expectations as gx
import great_expectations.expectations as gxe
from tests.test_utils import load_data_into_test_database

CONNECTION_STRING = "postgresql+psycopg2://postgres:@localhost/test_ci"

GX_ROOT_DIR = pathlib.Path(gx.__file__).parent.parent

# Add test data to database for testing.
load_data_into_test_database(
    table_name="purchases",
    csv_path=str(
        GX_ROOT_DIR
        / "tests/test_sets/learn_data_quality_use_cases/distribution_purchases.csv"
    ),
    connection_string=CONNECTION_STRING,
)

context = gx.get_context()

datasource = context.data_sources.add_postgres(
    "postgres database", connection_string=CONNECTION_STRING
)

data_asset = datasource.add_table_asset(name="data asset", table_name="purchases")
batch_definition = data_asset.add_batch_definition_whole_table("batch definition")
batch = batch_definition.get_batch()

suite = context.suites.add(gx.ExpectationSuite(name="example distribution expectations"))

#############################
# Start Expectation snippets.

suite.add_expectation(
    # <snippet name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/distribution_resources/distribution_expectations.py ExpectColumnMeanToBeBetween">
    gxe.ExpectColumnMeanToBeBetween(
        column="purchase_amount",
        min_value=50,
        max_value=1000
    )
    # </snippet>
)

suite.add_expectation(
    # <snippet name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/distribution_resources/distribution_expectations.py ExpectColumnQuantileValuesToBeBetween">
    gxe.ExpectColumnQuantileValuesToBeBetween(
        column="purchase_amount",
        quantile_ranges={
            "quantiles": [0.5, 0.9],
            "value_ranges": [
                [50, 200],
                [500, 2000]
            ]
        }
    )
    # </snippet>
)

suite.add_expectation(
    # <snippet name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/distribution_resources/distribution_expectations.py ExpectColumnStdevToBeBetween">
    gxe.ExpectColumnStdevToBeBetween(
        column="purchase_amount",
        min_value=500,
        max_value=2000
    )
    # </snippet>
)

suite.add_expectation(
    # <snippet name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/distribution_resources/distribution_expectations.py ExpectColumnValuesToBeBetween">
    gxe.ExpectColumnValuesToBeBetween(
        column="product_rating",
        min_value=1,
        max_value=5,
    )
    # </snippet>
)

suite.add_expectation(
    # <snippet name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/distribution_resources/distribution_expectations.py ExpectColumnValueZScoresToBeLessThan">
    gxe.ExpectColumnValueZScoresToBeLessThan(
        column="purchase_amount",
        threshold=3,
    )
    # </snippet>
)

detecting_anomalies_expectations = [
    # <snippet name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/distribution_resources/distribution_expectations.py detecting_anomalies">
    # Validate that purchase_amount is within expected range
    gxe.ExpectColumnValuesToBeBetween(
        column="purchase_amount",
        min_value=1,
        max_value=10000,
    ),

    # Validate that the mean of purchase_amount is within expected range
    gxe.ExpectColumnMeanToBeBetween(
        column="purchase_amount",
        min_value=200,
        max_value=500
    )
    # </snippet>
]

[suite.add_expectation(expectation) for expectation in detecting_anomalies_expectations]

suite.add_expectation(
    # <snippet name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/distribution_resources/distribution_expectations.py model_data_drift">

    # Validate that KL divergence is below threshold
    gxe.ExpectColumnKlDivergenceToBeLessThan(
        column="purchase_amount",
        partition_object={
            # Set up reference distribution (e.g., from training data)
            "bins": [0, 100, 500, 1000, 5000, 10000],
            "weights": [0.05, 0.25, 0.35, 0.25, 0.1]
        },
        threshold=0.1
    )
    # </snippet>
)

suite.add_expectation(
    # <snippet name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/distribution_resources/distribution_expectations.py time_series_consistency">
    gxe.ExpectColumnQuantileValuesToBeBetween(
        column="purchase_amount",
        quantile_ranges={
            "quantiles": [0.25, 0.5, 0.75],
            "value_ranges": [
                [100, 150],
                [200, 250],
                [300, 400]
            ]
        }
    )
    # </snippet>
)

results = batch.validate(suite)
