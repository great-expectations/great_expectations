import datetime
import os
from typing import List
from unittest import mock

import pandas as pd
import pandas.api.types as ptypes
import pytest

import great_expectations.exceptions as gx_exceptions
from great_expectations.core.batch_spec import (
    PathBatchSpec,
    RuntimeDataBatchSpec,
    S3BatchSpec,
)
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.execution_engine.partition_and_sample.data_partitioner import (
    DatePart,
)
from great_expectations.execution_engine.partition_and_sample.pandas_data_partitioner import (
    PandasDataPartitioner,
)
from tests.execution_engine.partition_and_sample.partition_and_sample_test_cases import (
    MULTIPLE_DATE_PART_BATCH_IDENTIFIERS,
    MULTIPLE_DATE_PART_DATE_PARTS,
    SINGLE_DATE_PART_BATCH_IDENTIFIERS,
    SINGLE_DATE_PART_DATE_PARTS,
)

# Here we add PandasDataPartitioner specific test cases to the generic test cases:
SINGLE_DATE_PART_DATE_PARTS += [
    pytest.param(
        [PandasDataPartitioner.date_part.MONTH],
        id="month getting date parts from PandasDataPartitioner.date_part",
    )
]
MULTIPLE_DATE_PART_DATE_PARTS += [
    pytest.param(
        [PandasDataPartitioner.date_part.YEAR, PandasDataPartitioner.date_part.MONTH],
        id="year_month getting date parts from PandasDataPartitioner.date_part",
    )
]


@pytest.fixture
def simple_multi_year_pandas_df():
    df: pd.DataFrame = pd.DataFrame(
        data={
            "input_timestamp": [
                "2018-01-01 12:00:00.000",
                "2018-10-02 12:00:00.000",
                "2019-01-01 12:00:00.000",
                "2019-10-02 12:00:00.000",
                "2019-11-03 12:00:00.000",
                "2020-01-01 12:00:00.000",
                "2020-10-02 12:00:00.000",
                "2020-11-03 12:00:00.000",
                "2020-12-04 12:00:00.000",
            ]
        }
    )
    df["timestamp"] = pd.to_datetime(df["input_timestamp"])

    assert ptypes.is_datetime64_any_dtype(df.timestamp)

    assert len(df.index) == 9
    return df


@pytest.fixture
def test_s3_files(s3, s3_bucket, test_df_small_csv):
    keys: List[str] = [
        "path/A-100.csv",
        "path/A-101.csv",
        "directory/B-1.csv",
        "directory/B-2.csv",
        "alpha-1.csv",
        "alpha-2.csv",
    ]
    for key in keys:
        s3.put_object(Bucket=s3_bucket, Body=test_df_small_csv, Key=key)
    return s3_bucket, keys


@pytest.fixture
def batch_with_partition_on_whole_table_s3(test_s3_files) -> S3BatchSpec:
    bucket, keys = test_s3_files
    path = keys[0]
    full_path = f"s3a://{os.path.join(bucket, path)}"  # noqa: PTH118

    batch_spec = S3BatchSpec(
        path=full_path,
        reader_method="read_csv",
        partitioner_method="_partition_on_whole_table",
    )
    return batch_spec


@pytest.mark.big
@pytest.mark.parametrize(
    "partitioner_kwargs_year,num_values_in_df",
    [
        pytest.param(year, num_values, id=year)
        for year, num_values in {"2018": 2, "2019": 3, "2020": 4}.items()
    ],
)
def test_get_batch_with_partition_on_year(
    partitioner_kwargs_year,
    num_values_in_df,
    simple_multi_year_pandas_df: pd.DataFrame,
):
    engine = PandasExecutionEngine()

    partitioned_df: pd.DataFrame = engine.get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=simple_multi_year_pandas_df,
            partitioner_method="partition_on_year",
            partitioner_kwargs={
                "column_name": "timestamp",
                "batch_identifiers": {"timestamp": partitioner_kwargs_year},
            },
        )
    ).dataframe
    assert len(partitioned_df) == num_values_in_df
    assert len(partitioned_df.columns) == 2


@pytest.mark.big
@pytest.mark.parametrize(
    "column_batch_identifier,num_values_in_df",
    [
        pytest.param(column_batch_identifier, num_values, id=column_batch_identifier)
        for column_batch_identifier, num_values in {
            "2018-01-01": 3,
            "2018-01-02": 3,
            "2018-01-03": 2,
            "2018-01-04": 1,
        }.items()
    ],
)
def test_get_batch_with_partition_on_date_parts_day(
    column_batch_identifier,
    num_values_in_df,
    simple_multi_year_pandas_df: pd.DataFrame,
):
    engine = PandasExecutionEngine()

    partitioned_df: pd.DataFrame = engine.get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=simple_multi_year_pandas_df,
            partitioner_method="partition_on_date_parts",
            partitioner_kwargs={
                "column_name": "timestamp",
                "batch_identifiers": {"timestamp": column_batch_identifier},
                "date_parts": [DatePart.DAY],
            },
        )
    ).dataframe

    assert len(partitioned_df) == num_values_in_df
    assert len(partitioned_df.columns) == 2


@pytest.mark.big
@pytest.mark.parametrize(
    "batch_identifiers_for_column",
    SINGLE_DATE_PART_BATCH_IDENTIFIERS,
)
@pytest.mark.parametrize(
    "date_parts",
    SINGLE_DATE_PART_DATE_PARTS,
)
def test_partition_on_date_parts_single_date_parts(
    batch_identifiers_for_column, date_parts, simple_multi_year_pandas_df
):
    """What does this test and why?

    partition_on_date_parts should still filter the correct rows from the input dataframe when passed a single element list
     date_parts that is a string, DatePart enum objects, mixed case string.
     To match our interface it should accept a dateutil parseable string as the batch identifier
     or a datetime and also fail when parameters are invalid.
    """  # noqa: E501
    data_partitioner: PandasDataPartitioner = PandasDataPartitioner()
    column_name: str = "timestamp"
    result: pd.DataFrame = data_partitioner.partition_on_date_parts(
        df=simple_multi_year_pandas_df,
        column_name=column_name,
        batch_identifiers={column_name: batch_identifiers_for_column},
        date_parts=date_parts,
    )
    assert len(result) == 3


@pytest.mark.big
@pytest.mark.parametrize(
    "batch_identifiers_for_column",
    MULTIPLE_DATE_PART_BATCH_IDENTIFIERS,
)
@pytest.mark.parametrize(
    "date_parts",
    MULTIPLE_DATE_PART_DATE_PARTS,
)
def test_partition_on_date_parts_multiple_date_parts(
    batch_identifiers_for_column, date_parts, simple_multi_year_pandas_df
):
    """What does this test and why?

    partition_on_date_parts should still filter the correct rows from the input dataframe when passed
     date parts that are strings, DatePart enum objects, a mixture and mixed case.
     To match our interface it should accept a dateutil parseable string as the batch identifier
     or a datetime and also fail when parameters are invalid.
    """  # noqa: E501
    data_partitioner: PandasDataPartitioner = PandasDataPartitioner()
    column_name: str = "timestamp"
    result: pd.DataFrame = data_partitioner.partition_on_date_parts(
        df=simple_multi_year_pandas_df,
        column_name=column_name,
        batch_identifiers={column_name: batch_identifiers_for_column},
        date_parts=date_parts,
    )
    assert len(result) == 1


@pytest.mark.big
@mock.patch(
    "great_expectations.execution_engine.partition_and_sample.pandas_data_partitioner.PandasDataPartitioner.partition_on_date_parts"
)
@pytest.mark.parametrize(
    "partitioner_method_name,called_with_date_parts",
    [
        ("partition_on_year", [DatePart.YEAR]),
        ("partition_on_year_and_month", [DatePart.YEAR, DatePart.MONTH]),
        (
            "partition_on_year_and_month_and_day",
            [DatePart.YEAR, DatePart.MONTH, DatePart.DAY],
        ),
    ],
)
def test_named_date_part_methods(
    mock_partition_on_date_parts: mock.MagicMock,  # noqa: TID251
    partitioner_method_name: str,
    called_with_date_parts: List[DatePart],
    simple_multi_year_pandas_df: pd.DataFrame,
):
    """Test that a partially pre-filled version of partition_on_date_parts() was called with the appropriate params.
    For example, partition_on_year.
    """  # noqa: E501
    data_partitioner: PandasDataPartitioner = PandasDataPartitioner()
    column_name: str = "column_name"
    batch_identifiers: dict = {column_name: {"year": 2018, "month": 10, "day": 31}}

    getattr(data_partitioner, partitioner_method_name)(
        df=simple_multi_year_pandas_df,
        column_name=column_name,
        batch_identifiers=batch_identifiers,
    )

    mock_partition_on_date_parts.assert_called_with(
        df=simple_multi_year_pandas_df,
        column_name=column_name,
        batch_identifiers=batch_identifiers,
        date_parts=called_with_date_parts,
    )


@pytest.mark.big
@pytest.mark.parametrize(
    "underscore_prefix",
    [
        pytest.param("_", id="underscore prefix"),
        pytest.param("", id="no underscore prefix"),
    ],
)
@pytest.mark.parametrize(
    "partitioner_method_name",
    [
        pytest.param(partitioner_method_name, id=partitioner_method_name)
        for partitioner_method_name in [
            "partition_on_year",
            "partition_on_year_and_month",
            "partition_on_year_and_month_and_day",
            "partition_on_date_parts",
            "partition_on_whole_table",
            "partition_on_column_value",
            "partition_on_converted_datetime",
            "partition_on_divided_integer",
            "partition_on_mod_integer",
            "partition_on_multi_column_values",
            "partition_on_hashed_column",
        ]
    ],
)
def test_get_partitioner_method(underscore_prefix: str, partitioner_method_name: str):
    data_partitioner: PandasDataPartitioner = PandasDataPartitioner()

    partitioner_method_name_with_prefix = f"{underscore_prefix}{partitioner_method_name}"

    assert data_partitioner.get_partitioner_method(partitioner_method_name_with_prefix) == getattr(
        data_partitioner, partitioner_method_name
    )


@pytest.mark.unit
def test_get_batch_with_partition_on_whole_table_runtime(test_df):
    partitioned_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(batch_data=test_df, partitioner_method="_partition_on_whole_table")
    )
    assert partitioned_df.dataframe.shape == (120, 10)


@pytest.mark.filesystem
def test_get_batch_with_partition_on_whole_table_filesystem(
    test_folder_connection_path_csv,
):
    test_df = PandasExecutionEngine().get_batch_data(
        PathBatchSpec(
            path=os.path.join(  # noqa: PTH118
                test_folder_connection_path_csv, "test.csv"
            ),
            reader_method="read_csv",
            partitioner_method="_partition_on_whole_table",
        )
    )
    assert test_df.dataframe.shape == (5, 2)


@pytest.mark.big
def test_get_batch_with_partition_on_whole_table_s3(
    batch_with_partition_on_whole_table_s3, test_df_small
):
    df = PandasExecutionEngine().get_batch_data(batch_spec=batch_with_partition_on_whole_table_s3)
    assert df.dataframe.shape == test_df_small.shape


@pytest.mark.big
def test_get_batch_with_partition_on_column_value(test_df):
    partitioned_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df,
            partitioner_method="_partition_on_column_value",
            partitioner_kwargs={
                "column_name": "batch_id",
                "batch_identifiers": {"batch_id": 2},
            },
        )
    )
    assert partitioned_df.dataframe.shape == (12, 10)
    assert (partitioned_df.dataframe.batch_id == 2).all()

    partitioned_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df,
            partitioner_method="_partition_on_column_value",
            partitioner_kwargs={
                "column_name": "date",
                "batch_identifiers": {"date": datetime.date(2020, 1, 30)},
            },
        )
    )
    assert partitioned_df.dataframe.shape == (3, 10)


@pytest.mark.big
def test_get_batch_with_partition_on_converted_datetime(test_df):
    partitioned_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df,
            partitioner_method="_partition_on_converted_datetime",
            partitioner_kwargs={
                "column_name": "timestamp",
                "batch_identifiers": {"timestamp": "2020-01-30"},
            },
        )
    )
    assert partitioned_df.dataframe.shape == (3, 10)


@pytest.mark.big
def test_get_batch_with_partition_on_divided_integer(test_df):
    partitioned_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df,
            partitioner_method="_partition_on_divided_integer",
            partitioner_kwargs={
                "column_name": "id",
                "divisor": 10,
                "batch_identifiers": {"id": 5},
            },
        )
    )
    assert partitioned_df.dataframe.shape == (10, 10)
    assert partitioned_df.dataframe.id.min() == 50
    assert partitioned_df.dataframe.id.max() == 59


@pytest.mark.big
def test_get_batch_with_partition_on_mod_integer(test_df):
    partitioned_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df,
            partitioner_method="_partition_on_mod_integer",
            partitioner_kwargs={
                "column_name": "id",
                "mod": 10,
                "batch_identifiers": {"id": 5},
            },
        )
    )
    assert partitioned_df.dataframe.shape == (12, 10)
    assert partitioned_df.dataframe.id.min() == 5
    assert partitioned_df.dataframe.id.max() == 115


@pytest.mark.big
def test_get_batch_with_partition_on_multi_column_values(test_df):
    partitioned_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df,
            partitioner_method="_partition_on_multi_column_values",
            partitioner_kwargs={
                "column_names": ["y", "m", "d"],
                "batch_identifiers": {
                    "y": 2020,
                    "m": 1,
                    "d": 5,
                },
            },
        )
    )
    assert partitioned_df.dataframe.shape == (4, 10)
    assert (partitioned_df.dataframe.date == datetime.date(2020, 1, 5)).all()

    with pytest.raises(ValueError):
        # noinspection PyUnusedLocal
        partitioned_df = PandasExecutionEngine().get_batch_data(
            RuntimeDataBatchSpec(
                batch_data=test_df,
                partitioner_method="_partition_on_multi_column_values",
                partitioner_kwargs={
                    "column_names": ["I", "dont", "exist"],
                    "batch_identifiers": {
                        "y": 2020,
                        "m": 1,
                        "d": 5,
                    },
                },
            )
        )


@pytest.mark.big
def test_get_batch_with_partition_on_hashed_column(test_df):
    with pytest.raises(gx_exceptions.ExecutionEngineError):
        # noinspection PyUnusedLocal
        partitioned_df = PandasExecutionEngine().get_batch_data(
            RuntimeDataBatchSpec(
                batch_data=test_df,
                partitioner_method="_partition_on_hashed_column",
                partitioner_kwargs={
                    "column_name": "favorite_color",
                    "hash_digits": 1,
                    "batch_identifiers": {
                        "hash_value": "a",
                    },
                    "hash_function_name": "I_am_not_valid",
                },
            )
        )

    partitioned_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df,
            partitioner_method="_partition_on_hashed_column",
            partitioner_kwargs={
                "column_name": "favorite_color",
                "hash_digits": 1,
                "batch_identifiers": {
                    "hash_value": "a",
                },
                "hash_function_name": "sha256",
            },
        )
    )
    assert partitioned_df.dataframe.shape == (8, 10)
