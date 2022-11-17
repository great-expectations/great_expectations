import datetime
import os
from typing import List
from unittest import mock

import pandas as pd
import pandas.api.types as ptypes
import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations.core import IDDict
from great_expectations.core.batch import BatchDefinition
from great_expectations.core.batch_spec import (
    PathBatchSpec,
    RuntimeDataBatchSpec,
    S3BatchSpec,
)
from great_expectations.datasource.data_connector import ConfiguredAssetS3DataConnector
from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine
from great_expectations.execution_engine.split_and_sample.data_splitter import DatePart
from great_expectations.execution_engine.split_and_sample.pandas_data_splitter import (
    PandasDataSplitter,
)
from tests.execution_engine.split_and_sample.split_and_sample_test_cases import (
    MULTIPLE_DATE_PART_BATCH_IDENTIFIERS,
    MULTIPLE_DATE_PART_DATE_PARTS,
    SINGLE_DATE_PART_BATCH_IDENTIFIERS,
    SINGLE_DATE_PART_DATE_PARTS,
)

# Here we add PandasDataSplitter specific test cases to the generic test cases:
SINGLE_DATE_PART_DATE_PARTS += [
    pytest.param(
        [PandasDataSplitter.date_part.MONTH],
        id="month getting date parts from PandasDataSplitter.date_part",
    )
]
MULTIPLE_DATE_PART_DATE_PARTS += [
    pytest.param(
        [PandasDataSplitter.date_part.YEAR, PandasDataSplitter.date_part.MONTH],
        id="year_month getting date parts from PandasDataSplitter.date_part",
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
def batch_with_split_on_whole_table_s3(test_s3_files) -> S3BatchSpec:
    bucket, keys = test_s3_files
    path = keys[0]
    full_path = f"s3a://{os.path.join(bucket, path)}"

    batch_spec = S3BatchSpec(
        path=full_path,
        reader_method="read_csv",
        splitter_method="_split_on_whole_table",
    )
    return batch_spec


@pytest.mark.integration
@pytest.mark.parametrize(
    "splitter_kwargs_year,num_values_in_df",
    [
        pytest.param(year, num_values, id=year)
        for year, num_values in {"2018": 2, "2019": 3, "2020": 4}.items()
    ],
)
def test_get_batch_with_split_on_year(
    splitter_kwargs_year,
    num_values_in_df,
    simple_multi_year_pandas_df: pd.DataFrame,
):
    engine = PandasExecutionEngine()

    split_df: pd.DataFrame = engine.get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=simple_multi_year_pandas_df,
            splitter_method="split_on_year",
            splitter_kwargs={
                "column_name": "timestamp",
                "batch_identifiers": {"timestamp": splitter_kwargs_year},
            },
        )
    ).dataframe
    assert len(split_df) == num_values_in_df
    assert len(split_df.columns) == 2


@pytest.mark.integration
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
def test_get_batch_with_split_on_date_parts_day(
    column_batch_identifier,
    num_values_in_df,
    simple_multi_year_pandas_df: pd.DataFrame,
):
    engine = PandasExecutionEngine()

    split_df: pd.DataFrame = engine.get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=simple_multi_year_pandas_df,
            splitter_method="split_on_date_parts",
            splitter_kwargs={
                "column_name": "timestamp",
                "batch_identifiers": {"timestamp": column_batch_identifier},
                "date_parts": [DatePart.DAY],
            },
        )
    ).dataframe

    assert len(split_df) == num_values_in_df
    assert len(split_df.columns) == 2


@pytest.mark.parametrize(
    "batch_identifiers_for_column",
    SINGLE_DATE_PART_BATCH_IDENTIFIERS,
)
@pytest.mark.parametrize(
    "date_parts",
    SINGLE_DATE_PART_DATE_PARTS,
)
def test_split_on_date_parts_single_date_parts(
    batch_identifiers_for_column, date_parts, simple_multi_year_pandas_df
):
    """What does this test and why?

    split_on_date_parts should still filter the correct rows from the input dataframe when passed a single element list
     date_parts that is a string, DatePart enum objects, mixed case string.
     To match our interface it should accept a dateutil parseable string as the batch identifier
     or a datetime and also fail when parameters are invalid.
    """
    data_splitter: PandasDataSplitter = PandasDataSplitter()
    column_name: str = "timestamp"
    result: pd.DataFrame = data_splitter.split_on_date_parts(
        df=simple_multi_year_pandas_df,
        column_name=column_name,
        batch_identifiers={column_name: batch_identifiers_for_column},
        date_parts=date_parts,
    )
    assert len(result) == 3


@pytest.mark.parametrize(
    "batch_identifiers_for_column",
    MULTIPLE_DATE_PART_BATCH_IDENTIFIERS,
)
@pytest.mark.parametrize(
    "date_parts",
    MULTIPLE_DATE_PART_DATE_PARTS,
)
def test_split_on_date_parts_multiple_date_parts(
    batch_identifiers_for_column, date_parts, simple_multi_year_pandas_df
):
    """What does this test and why?

    split_on_date_parts should still filter the correct rows from the input dataframe when passed
     date parts that are strings, DatePart enum objects, a mixture and mixed case.
     To match our interface it should accept a dateutil parseable string as the batch identifier
     or a datetime and also fail when parameters are invalid.
    """
    data_splitter: PandasDataSplitter = PandasDataSplitter()
    column_name: str = "timestamp"
    result: pd.DataFrame = data_splitter.split_on_date_parts(
        df=simple_multi_year_pandas_df,
        column_name=column_name,
        batch_identifiers={column_name: batch_identifiers_for_column},
        date_parts=date_parts,
    )
    assert len(result) == 1


@mock.patch(
    "great_expectations.execution_engine.split_and_sample.pandas_data_splitter.PandasDataSplitter.split_on_date_parts"
)
@pytest.mark.parametrize(
    "splitter_method_name,called_with_date_parts",
    [
        ("split_on_year", [DatePart.YEAR]),
        ("split_on_year_and_month", [DatePart.YEAR, DatePart.MONTH]),
        (
            "split_on_year_and_month_and_day",
            [DatePart.YEAR, DatePart.MONTH, DatePart.DAY],
        ),
    ],
)
def test_named_date_part_methods(
    mock_split_on_date_parts: mock.MagicMock,
    splitter_method_name: str,
    called_with_date_parts: List[DatePart],
    simple_multi_year_pandas_df: pd.DataFrame,
):
    """Test that a partially pre-filled version of split_on_date_parts() was called with the appropriate params.
    For example, split_on_year.
    """
    data_splitter: PandasDataSplitter = PandasDataSplitter()
    column_name: str = "column_name"
    batch_identifiers: dict = {column_name: {"year": 2018, "month": 10, "day": 31}}

    getattr(data_splitter, splitter_method_name)(
        df=simple_multi_year_pandas_df,
        column_name=column_name,
        batch_identifiers=batch_identifiers,
    )

    mock_split_on_date_parts.assert_called_with(
        df=simple_multi_year_pandas_df,
        column_name=column_name,
        batch_identifiers=batch_identifiers,
        date_parts=called_with_date_parts,
    )


@pytest.mark.parametrize(
    "underscore_prefix",
    [
        pytest.param("_", id="underscore prefix"),
        pytest.param("", id="no underscore prefix"),
    ],
)
@pytest.mark.parametrize(
    "splitter_method_name",
    [
        pytest.param(splitter_method_name, id=splitter_method_name)
        for splitter_method_name in [
            "split_on_year",
            "split_on_year_and_month",
            "split_on_year_and_month_and_day",
            "split_on_date_parts",
            "split_on_whole_table",
            "split_on_column_value",
            "split_on_converted_datetime",
            "split_on_divided_integer",
            "split_on_mod_integer",
            "split_on_multi_column_values",
            "split_on_hashed_column",
        ]
    ],
)
def test_get_splitter_method(underscore_prefix: str, splitter_method_name: str):
    data_splitter: PandasDataSplitter = PandasDataSplitter()

    splitter_method_name_with_prefix = f"{underscore_prefix}{splitter_method_name}"

    assert data_splitter.get_splitter_method(
        splitter_method_name_with_prefix
    ) == getattr(data_splitter, splitter_method_name)


def test_get_batch_with_split_on_whole_table_runtime(test_df):
    split_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df, splitter_method="_split_on_whole_table"
        )
    )
    assert split_df.dataframe.shape == (120, 10)


def test_get_batch_with_split_on_whole_table_filesystem(
    test_folder_connection_path_csv,
):
    test_df = PandasExecutionEngine().get_batch_data(
        PathBatchSpec(
            path=os.path.join(test_folder_connection_path_csv, "test.csv"),
            reader_method="read_csv",
            splitter_method="_split_on_whole_table",
        )
    )
    assert test_df.dataframe.shape == (5, 2)


def test_get_batch_with_split_on_whole_table_s3(
    batch_with_split_on_whole_table_s3, test_df_small
):
    df = PandasExecutionEngine().get_batch_data(
        batch_spec=batch_with_split_on_whole_table_s3
    )
    assert df.dataframe.shape == test_df_small.shape


def test_get_batch_with_split_on_whole_table_s3_with_configured_asset_s3_data_connector(
    test_s3_files, test_df_small
):
    bucket, _keys = test_s3_files
    expected_df = test_df_small

    execution_engine: ExecutionEngine = PandasExecutionEngine()

    my_data_connector = ConfiguredAssetS3DataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        bucket=bucket,
        execution_engine=execution_engine,
        prefix="",
        assets={"alpha": {}},
        default_regex={
            "pattern": "alpha-(.*)\\.csv",
            "group_names": ["index"],
        },
    )
    batch_def = BatchDefinition(
        datasource_name="FAKE_DATASOURCE_NAME",
        data_connector_name="my_data_connector",
        data_asset_name="alpha",
        batch_identifiers=IDDict(index=1),
        batch_spec_passthrough={
            "reader_method": "read_csv",
            "splitter_method": "_split_on_whole_table",
        },
    )
    test_df = execution_engine.get_batch_data(
        batch_spec=my_data_connector.build_batch_spec(batch_definition=batch_def)
    )
    assert test_df.dataframe.shape == expected_df.shape

    # if key does not exist
    batch_def_no_key = BatchDefinition(
        datasource_name="FAKE_DATASOURCE_NAME",
        data_connector_name="my_data_connector",
        data_asset_name="alpha",
        batch_identifiers=IDDict(index=9),
        batch_spec_passthrough={
            "reader_method": "read_csv",
            "splitter_method": "_split_on_whole_table",
        },
    )
    with pytest.raises(ge_exceptions.ExecutionEngineError):
        execution_engine.get_batch_data(
            batch_spec=my_data_connector.build_batch_spec(
                batch_definition=batch_def_no_key
            )
        )


def test_get_batch_with_split_on_column_value(test_df):
    split_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df,
            splitter_method="_split_on_column_value",
            splitter_kwargs={
                "column_name": "batch_id",
                "batch_identifiers": {"batch_id": 2},
            },
        )
    )
    assert split_df.dataframe.shape == (12, 10)
    assert (split_df.dataframe.batch_id == 2).all()

    split_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df,
            splitter_method="_split_on_column_value",
            splitter_kwargs={
                "column_name": "date",
                "batch_identifiers": {"date": datetime.date(2020, 1, 30)},
            },
        )
    )
    assert split_df.dataframe.shape == (3, 10)


def test_get_batch_with_split_on_converted_datetime(test_df):
    split_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df,
            splitter_method="_split_on_converted_datetime",
            splitter_kwargs={
                "column_name": "timestamp",
                "batch_identifiers": {"timestamp": "2020-01-30"},
            },
        )
    )
    assert split_df.dataframe.shape == (3, 10)


def test_get_batch_with_split_on_divided_integer(test_df):
    split_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df,
            splitter_method="_split_on_divided_integer",
            splitter_kwargs={
                "column_name": "id",
                "divisor": 10,
                "batch_identifiers": {"id": 5},
            },
        )
    )
    assert split_df.dataframe.shape == (10, 10)
    assert split_df.dataframe.id.min() == 50
    assert split_df.dataframe.id.max() == 59


def test_get_batch_with_split_on_mod_integer(test_df):
    split_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df,
            splitter_method="_split_on_mod_integer",
            splitter_kwargs={
                "column_name": "id",
                "mod": 10,
                "batch_identifiers": {"id": 5},
            },
        )
    )
    assert split_df.dataframe.shape == (12, 10)
    assert split_df.dataframe.id.min() == 5
    assert split_df.dataframe.id.max() == 115


def test_get_batch_with_split_on_multi_column_values(test_df):
    split_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df,
            splitter_method="_split_on_multi_column_values",
            splitter_kwargs={
                "column_names": ["y", "m", "d"],
                "batch_identifiers": {
                    "y": 2020,
                    "m": 1,
                    "d": 5,
                },
            },
        )
    )
    assert split_df.dataframe.shape == (4, 10)
    assert (split_df.dataframe.date == datetime.date(2020, 1, 5)).all()

    with pytest.raises(ValueError):
        # noinspection PyUnusedLocal
        split_df = PandasExecutionEngine().get_batch_data(
            RuntimeDataBatchSpec(
                batch_data=test_df,
                splitter_method="_split_on_multi_column_values",
                splitter_kwargs={
                    "column_names": ["I", "dont", "exist"],
                    "batch_identifiers": {
                        "y": 2020,
                        "m": 1,
                        "d": 5,
                    },
                },
            )
        )


def test_get_batch_with_split_on_hashed_column(test_df):
    with pytest.raises(ge_exceptions.ExecutionEngineError):
        # noinspection PyUnusedLocal
        split_df = PandasExecutionEngine().get_batch_data(
            RuntimeDataBatchSpec(
                batch_data=test_df,
                splitter_method="_split_on_hashed_column",
                splitter_kwargs={
                    "column_name": "favorite_color",
                    "hash_digits": 1,
                    "batch_identifiers": {
                        "hash_value": "a",
                    },
                    "hash_function_name": "I_am_not_valid",
                },
            )
        )

    split_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df,
            splitter_method="_split_on_hashed_column",
            splitter_kwargs={
                "column_name": "favorite_color",
                "hash_digits": 1,
                "batch_identifiers": {
                    "hash_value": "a",
                },
                "hash_function_name": "sha256",
            },
        )
    )
    assert split_df.dataframe.shape == (8, 10)
