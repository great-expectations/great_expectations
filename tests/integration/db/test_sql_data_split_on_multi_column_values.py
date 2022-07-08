from typing import List

import numpy as np
import pandas as pd

from tests.integration.db.taxi_data_utils import (
    _execute_taxi_splitting_test_cases,
    _get_loaded_table,
    _is_dialect_athena,
)
from tests.integration.fixtures.split_and_sample_data.splitter_test_cases_and_fixtures import (
    TaxiSplittingTestCasesBase,
    TaxiSplittingTestCasesMultiColumnValues,
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

    test_column_names: List[str] = [
        "rate_code_id",
        "payment_type",
    ]

    if _is_dialect_athena(dialect):
        test_column_name: str
        df_null: pd.DataFrame
        df_nonnull: pd.DataFrame
        for test_column_name in test_column_names:
            df_null = test_df[test_df[test_column_name].isnull()]
            df_null[test_column_name] = df_null[test_column_name].apply(
                lambda x: None if np.isnan(x) else x
            )
            df_nonnull = test_df[~test_df[test_column_name].isnull()]
            df_nonnull[test_column_name] = df_nonnull[test_column_name].astype(int)
            test_df = pd.concat([df_null, df_nonnull])

    taxi_test_data: TaxiTestData = TaxiTestData(
        test_df=test_df,
        test_column_name=None,
        test_column_names=test_column_names,
    )
    taxi_splitting_test_cases: TaxiSplittingTestCasesBase = (
        TaxiSplittingTestCasesMultiColumnValues(taxi_test_data=taxi_test_data)
    )
    _execute_taxi_splitting_test_cases(
        taxi_splitting_test_cases=taxi_splitting_test_cases,
        connection_string=connection_string,
        table_name=table_name,
    )
