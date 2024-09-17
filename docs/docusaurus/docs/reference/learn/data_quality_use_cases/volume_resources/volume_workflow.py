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

# <snippet name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/volume_resources/volume_workflow.py full example code">
import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd

# Create Data Context.
context = gx.get_context()

# Connect to sample data, create Data Source and Data Asset.
CONNECTION_STRING = "postgresql+psycopg2://try_gx:try_gx@postgres.workshops.greatexpectations.io/gx_learn_data_quality"

data_source = context.data_sources.add_postgres(
    "postgres database", connection_string=CONNECTION_STRING
)
data_asset = data_source.add_table_asset(
    name="financial transfers table", table_name="volume_financial_transfers"
)

# Add a Batch Definition with partitioning by day.
batch_definition = data_asset.add_batch_definition_daily(
    name="daily transfers", column="transfer_ts"
)

# Create an Expectation testing that each batch (day) contains between 1 and 5 rows.
volume_expectation = gxe.ExpectTableRowCountToBeBetween(min_value=1, max_value=5)

# Validate data volume for each day in date range and capture result.
START_DATE = "2024-05-01"
END_DATE = "2024-05-07"

validation_results_by_day = []

for date in list(pd.date_range(start=START_DATE, end=END_DATE).to_pydatetime()):
    daily_batch = batch_definition.get_batch(
        batch_parameters={"year": date.year, "month": date.month, "day": date.day}
    )

    result = daily_batch.validate(volume_expectation)
    validation_results_by_day.append(
        {
            "date": date,
            "expectation passed": result["success"],
            "observed rows": result["result"]["observed_value"],
        }
    )

pd.DataFrame(validation_results_by_day)
# </snippet>

df = pd.DataFrame(validation_results_by_day)

# Check output matches what is in the docs.
expected_values = [
    ["2024-05-01", True, 4],
    ["2024-05-02", True, 5],
    ["2024-05-03", True, 5],
    ["2024-05-04", True, 5],
    ["2024-05-05", True, 5],
    ["2024-05-06", False, 6],
    ["2024-05-07", True, 5],
]

for idx, row in enumerate(expected_values):
    results = df.iloc[idx]
    assert str(results["date"])[0:10] == row[0]
    assert bool(results["expectation passed"]) is row[1]
    assert results["observed rows"] == row[2]
