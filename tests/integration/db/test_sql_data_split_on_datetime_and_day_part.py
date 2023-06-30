import pandas as pd

from tests.integration.db.taxi_data_utils import (
    _execute_taxi_splitting_test_cases,
    loaded_table,
)
from tests.integration.fixtures.split_and_sample_data.splitter_test_cases_and_fixtures import (
    TaxiSplittingTestCasesBase,
    TaxiSplittingTestCasesDateTime,
    TaxiTestData,
)
from tests.test_utils import get_connection_string_and_dialect

if __name__ == "test_script_module":
    dialect: str
    connection_string: str
    dialect, connection_string = get_connection_string_and_dialect(
        athena_db_name_env_var="ATHENA_TEN_TRIPS_DB_NAME"
    )
    print(f"Testing dialect: {dialect}")

    with loaded_table(dialect=dialect, connection_string=connection_string) as table:
        table_name: str = table.table_name
        test_df: pd.DataFrame = table.inserted_dataframe

        test_column_name: str = "pickup_datetime"

        taxi_test_data: TaxiTestData = TaxiTestData(
            test_df=test_df,
            test_column_name=test_column_name,
            test_column_names=None,
        )
        taxi_splitting_test_cases: TaxiSplittingTestCasesBase = (
            TaxiSplittingTestCasesDateTime(taxi_test_data=taxi_test_data)
        )
        _execute_taxi_splitting_test_cases(
            taxi_splitting_test_cases=taxi_splitting_test_cases,
            connection_string=connection_string,
            table_name=table_name,
        )
