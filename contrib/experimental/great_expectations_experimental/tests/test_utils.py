import os
from typing import List

# noinspection PyUnresolvedReferences
from great_expectations.data_context.util import file_relative_path

# noinspection PyUnresolvedReferences

# constants used by the sql example
pg_hostname = os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost")
CONNECTION_STRING: str = f"postgresql+psycopg2://postgres:@{pg_hostname}/test_ci"


def load_data_into_postgres_database(sa):
    """
    Method to load our 2019 and 2020 taxi data into a postgres database.  This is a helper method
    called by test_sql_happy_path_statistics_data_assistant().
    """

    from tests.test_utils import load_data_into_test_database

    file_name: str
    data_paths: List[str] = [
        file_relative_path(
            __file__,
            os.path.join(  # noqa: PTH118
                "..",
                "..",
                "..",
                "..",
                "..",
                "..",
                "tests",
                "test_sets",
                "taxi_yellow_tripdata_samples",
                file_name,
            ),
        )
        for file_name in [
            "yellow_tripdata_samples/yellow_tripdata_sample_2019-01.csv",
            "yellow_tripdata_samples/yellow_tripdata_sample_2019-02.csv",
            "yellow_tripdata_samples/yellow_tripdata_sample_2019-03.csv",
            "yellow_tripdata_samples/yellow_tripdata_sample_2019-04.csv",
            "yellow_tripdata_samples/yellow_tripdata_sample_2019-05.csv",
            "yellow_tripdata_samples/yellow_tripdata_sample_2019-06.csv",
            "yellow_tripdata_samples/yellow_tripdata_sample_2019-07.csv",
            "yellow_tripdata_samples/yellow_tripdata_sample_2019-08.csv",
            "yellow_tripdata_samples/yellow_tripdata_sample_2019-09.csv",
            "yellow_tripdata_samples/yellow_tripdata_sample_2019-10.csv",
            "yellow_tripdata_samples/yellow_tripdata_sample_2019-11.csv",
            "yellow_tripdata_samples/yellow_tripdata_sample_2019-12.csv",
        ]
    ]
    table_name: str = "yellow_tripdata_sample_2019"

    engine: sa.engine.Engine = sa.create_engine(CONNECTION_STRING)
    connection: sa.engine.Connection = engine.connect()

    # ensure we aren't appending to an existing table
    # noinspection SqlDialectInspection,SqlNoDataSourceInspection
    connection.execute(sa.text(f"DROP TABLE IF EXISTS {table_name}"))
    for data_path in data_paths:
        load_data_into_test_database(
            table_name=table_name,
            csv_path=data_path,
            connection_string=CONNECTION_STRING,
            load_full_dataset=True,
            drop_existing_table=False,
            convert_colnames_to_datetime=["pickup_datetime", "dropoff_datetime"],
        )

    # 2020 data
    file_name = "yellow_tripdata_sample_2020-01.csv"
    data_paths: List[str] = [
        file_relative_path(
            __file__,
            os.path.join(  # noqa: PTH118
                "..",
                "..",
                "..",
                "..",
                "..",
                "..",
                "tests",
                "test_sets",
                "taxi_yellow_tripdata_samples",
                file_name,
            ),
        )
    ]
    table_name: str = "yellow_tripdata_sample_2020"

    engine: sa.engine.Engine = sa.create_engine(CONNECTION_STRING)
    connection: sa.engine.Connection = engine.connect()

    # ensure we aren't appending to an existing table
    # noinspection SqlDialectInspection,SqlNoDataSourceInspection
    connection.execute(sa.text(f"DROP TABLE IF EXISTS {table_name}"))
    for data_path in data_paths:
        load_data_into_test_database(
            table_name=table_name,
            csv_path=data_path,
            connection_string=CONNECTION_STRING,
            load_full_dataset=True,
            drop_existing_table=False,
            convert_colnames_to_datetime=["pickup_datetime", "dropoff_datetime"],
        )
