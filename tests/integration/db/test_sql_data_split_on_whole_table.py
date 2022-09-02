import pandas as pd

from tests.integration.db.taxi_data_utils import (
    _execute_taxi_splitting_test_cases,
    _get_loaded_table,
)
from tests.integration.fixtures.split_and_sample_data.splitter_test_cases_and_fixtures import (
    TaxiSplittingTestCasesBase,
    TaxiSplittingTestCasesWholeTable,
    TaxiTestData,
)
from tests.test_utils import LoadedTable, get_connection_string_and_dialect

if __name__ == "test_script_module":

    dialect: str
    connection_string: str
    dialect, connection_string = get_connection_string_and_dialect(
        athena_db_name_env_var="ATHENA_TEN_TRIPS_DB_NAME"
    )
    print(f"Testing dialect: {dialect}")

    loaded_table: LoadedTable = _get_loaded_table(dialect=dialect)

    table_name: str = loaded_table.table_name
    test_df: pd.DataFrame = loaded_table.inserted_dataframe

    taxi_test_data: TaxiTestData = TaxiTestData(
        test_df=test_df,
        test_column_name=None,
        test_column_names=None,
    )
    taxi_splitting_test_cases: TaxiSplittingTestCasesBase = (
        TaxiSplittingTestCasesWholeTable(taxi_test_data=taxi_test_data)
    )
    _execute_taxi_splitting_test_cases(
        taxi_splitting_test_cases=taxi_splitting_test_cases,
        connection_string=connection_string,
        table_name=table_name,
    )
