# this is jst te

import pathlib
from typing import TYPE_CHECKING

import great_expectations as gx
from great_expectations.core.yaml_handler import YAMLHandler

if TYPE_CHECKING:
    from great_expectations.core.batch import BatchRequest
    from great_expectations.datasource.fluent.interfaces import DataAsset


def test_me():
    YAMLHandler()
    context = gx.get_context()
    current_path = pathlib.Path(__file__).parents[1]
    data_path = (
        current_path
        / ".."
        / ".."
        / "tests"
        / "test_sets"
        / "taxi_yellow_tripdata_samples"
        / "first_ten_trips_in_each_file"
    )
    context.sources.add_pandas_filesystem(
        "taxi_multi_batch_datasource",
        base_directory=data_path,  # replace with your data directory
    ).add_csv_asset(
        "all_years",
        batching_regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
    )
    expectation_suite_name = "my_missingness_assistant_suite"
    expectation_suite = context.add_or_update_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    all_years_asset: DataAsset = context.datasources[
        "taxi_multi_batch_datasource"
    ].get_asset("all_years")

    multi_batch_all_years_batch_request: BatchRequest = (
        all_years_asset.build_batch_request()
    )
    exclude_column_names = [
        "VendorID",
        "pickup_datetime",
        "dropoff_datetime",
        "RatecodeID",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "congestion_surcharge",
    ]
    data_assistant_result = context.assistants.missingness.run(
        batch_request=multi_batch_all_years_batch_request,
        exclude_column_names=exclude_column_names,
    )
    expectation_suite = data_assistant_result.get_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    context.add_or_update_expectation_suite(expectation_suite=expectation_suite)
    checkpoint = context.add_or_update_checkpoint(
        name=f"yellow_tripdata_sample_{expectation_suite_name}",
        validations=[
            {
                "batch_request": multi_batch_all_years_batch_request,
                "expectation_suite_name": expectation_suite_name,
            }
        ],
    )
    checkpoint_result = checkpoint.run()
    print(checkpoint_result)
    print("hello")
    res = data_assistant_result.plot_expectations_and_metrics()
    print(res)
