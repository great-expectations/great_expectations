from typing import List

import pandas as pd

import great_expectations as ge
from great_expectations import DataContext
from tests.integration.fixtures.split_data.splitter_test_cases_and_fixtures import (
    TaxiSplittingTestCase,
    TaxiSplittingTestCases,
    TaxiTestData,
)
from tests.test_utils import load_and_concatenate_csvs

spark = ge.core.util.get_or_create_spark_application()

if __name__ == "test_script_module":
    # 0. Load data

    test_df: pd.DataFrame = load_and_concatenate_csvs(
        csv_paths=[
            "./data/ten_trips_from_each_month/yellow_tripdata_sample_10_trips_from_each_month.csv"
        ],
        convert_column_names_to_datetime=["pickup_datetime", "dropoff_datetime"],
        load_full_dataset=True,
    )
    assert len(test_df) == 360

    taxi_test_data: TaxiTestData = TaxiTestData(
        test_df, test_column_name="pickup_datetime"
    )
    taxi_splitting_test_cases: TaxiSplittingTestCases = TaxiSplittingTestCases(
        taxi_test_data
    )

    test_cases: List[TaxiSplittingTestCase] = taxi_splitting_test_cases.test_cases()

    # spark_df = spark.createDataFrame(test_df.to_records())

    # for test_case in test_cases:
    #
    #     print("Testing splitter method:", test_case.splitter_method_name)
    #
    #     # 1. Setup
    #
    #     context: DataContext = ge.get_context()
    #
    #     datasource_name: str = "test_datasource"
    #     context.add_datasource(
    #         name=datasource_name,
    #         class_name="Datasource",
    #         execution_engine={
    #             "class_name": "SparkDFExecutionEngine",
    #         },
    #     )
    #
    #     # 2. Set up data connector
    #     data_connector_name: str = "test_data_connector"
    #     column_name: str = taxi_splitting_test_cases.test_column_name
    #     data_connector: ConfiguredAssetSqlDataConnector = (
    #         ConfiguredAssetSqlDataConnector(
    #             name=data_connector_name,
    #             datasource_name=datasource_name,
    #             execution_engine=context.datasources[datasource_name].execution_engine,
    #             assets={
    #                 data_asset_name: {
    #                     "splitter_method": test_case.splitter_method_name,
    #                     "splitter_kwargs": test_case.splitter_kwargs,
    #                 }
    #             },
    #         )
    #     )
