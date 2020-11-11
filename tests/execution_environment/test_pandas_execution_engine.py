import pytest
import datetime
import random
import os
import pandas as pd
import hashlib
from typing import List

from great_expectations.core.batch import BatchSpec
from great_expectations.execution_environment.types.batch_spec import RuntimeDataBatchSpec, PathBatchSpec
from great_expectations.execution_engine.pandas_execution_engine import PandasExecutionEngine

# def test_basic_setup():
#     df = pd.DataFrame({"x": range(10)})
#     # df = PandasExecutionEngine({"x": range(10)})

#     batch_data, batch_markers = PandasExecutionEngine().load_batch(
#         batch_spec = RuntimeDataBatchSpec({
#             "batch_data": df,
#             "data_asset_name": "TEST_DATA_ASSET",
#         })
#     )
#     print(df)

#     assert False

@pytest.fixture
def test_df(tmp_path_factory):

    def generate_ascending_list_of_dates(
        k,
        start_date=datetime.date(2020,1,1),
        end_date=datetime.date(2020,12,31)
    ):
        days_between_dates = (end_date - start_date).days
        
        date_list = [start_date + datetime.timedelta(days=random.randrange(days_between_dates)) for i in range(k)]
        date_list.sort()
        return date_list

    def generate_ascending_list_of_datetimes(
        k,
        start_date=datetime.date(2020,1,1),
        end_date=datetime.date(2020,12,31)
    ):
        start_time = datetime.datetime(start_date.year, start_date.month, start_date.day)
        days_between_dates = (end_date - start_date).total_seconds()
        
        datetime_list = [start_time + datetime.timedelta(seconds=random.randrange(days_between_dates)) for i in range(k)]
        datetime_list.sort()
        return datetime_list

    k = 120
    random.seed(1)

    timestamp_list = generate_ascending_list_of_datetimes(k, end_date=datetime.date(2020,1,31))
    date_list = [datetime.date(ts.year, ts.month, ts.day) for ts in timestamp_list]

    batch_ids = [random.randint(0,10) for i in range(k)]
    batch_ids.sort()

    session_ids = [random.randint(2,60) for i in range(k)]
    session_ids.sort()
    session_ids = [i-random.randint(0,2) for i in session_ids]

    events_df = pd.DataFrame({
        "id" : range(k),
        "batch_id" : batch_ids,
        "date" : date_list,
        "y" : [d.year for d in date_list],
        "m" : [d.month for d in date_list],
        "d" : [d.day for d in date_list],
        "timestamp" : timestamp_list,
        "session_ids" : session_ids,
        "event_type" : [random.choice(["start", "stop", "continue"]) for i in range(k)],
        "favorite_color" : ["#"+"".join([random.choice(list("0123456789ABCDEF")) for j in range(6)]) for i in range(k)]
    })

    return events_df

def test_get_batch_data(test_df):
    print(test_df.T)
    split_df = PandasExecutionEngine().get_batch_data(RuntimeDataBatchSpec(
        batch_data=test_df,
    ))
    assert split_df.shape == (120, 10)

    # TODO Abe 20201105: We should change RuntimeDataBatchSpec so that this test passes, but that should be a different PR.
    # No dataset passed to RuntimeDataBatchSpec
    # with pytest.raises(ValueError):
    #     PandasExecutionEngine().get_batch_data(RuntimeDataBatchSpec(

    #         # batch_data=test_df,
    #     ))

def test_get_batch_with_split_on_whole_table(test_df):
    print(test_df.T)

    split_df = PandasExecutionEngine().get_batch_data(RuntimeDataBatchSpec(
        batch_data=test_df,
        splitter_method="_split_on_whole_table"
    ))
    assert split_df.shape == (120, 10)


def test_get_batch_with_split_on_whole_table_filesystem(test_folder_connection_path):
    test = PathBatchSpec(
        path=os.path.join(test_folder_connection_path, "test.csv"),
        reader_method="read_csv",
        splitter_method="_split_on_whole_table"
        )
    test_df = PandasExecutionEngine().get_batch_data(test)
    assert test_df.shape == (5, 3)


def test_get_batch_with_split_on_column_value(test_df):

    split_df = PandasExecutionEngine().get_batch_data(RuntimeDataBatchSpec(
        batch_data=test_df,
        splitter_method="_split_on_column_value",
        splitter_kwargs={
            "column_name" : "batch_id",
            "partition_definition" : {
                "batch_id": 2
            }
        }
    ))
    assert split_df.shape == (12, 10)
    assert (split_df.batch_id == 2).all()

    split_df = PandasExecutionEngine().get_batch_data(RuntimeDataBatchSpec(
        batch_data=test_df,
        splitter_method="_split_on_column_value",
        splitter_kwargs={
            "column_name":"date",
            "partition_definition":{
                "date": datetime.date(2020,1,30)
            }
        }
    ))
    assert (split_df).shape == (3, 10)


def test_get_batch_with_split_on_converted_datetime(test_df):
    split_df = PandasExecutionEngine().get_batch_data(RuntimeDataBatchSpec(
        batch_data=test_df,
        splitter_method="_split_on_converted_datetime",
        splitter_kwargs={
            "column_name": "timestamp",
            "partition_definition": {
                "timestamp": "2020-01-30"
            }
        }
    ))
    assert (split_df).shape == (3, 10)

def test_get_batch_with_split_on_divided_integer(test_df):
    split_df = PandasExecutionEngine().get_batch_data(RuntimeDataBatchSpec(
        batch_data=test_df,
        splitter_method="_split_on_divided_integer",
        splitter_kwargs={
            "column_name": "id",
            "divisor": 10,
            "partition_definition": {
                "id": 5
            }
        }
    ))
    assert split_df.shape == (10, 10)
    assert split_df.id.min() == 50
    assert split_df.id.max() == 59

def test_get_batch_with_split_on_mod_integer(test_df):
    split_df = PandasExecutionEngine().get_batch_data(RuntimeDataBatchSpec(
        batch_data=test_df,
        splitter_method="_split_on_mod_integer",
        splitter_kwargs={
            "column_name": "id",
            "mod":10,
            "partition_definition": {
                "id": 5
            }
        }
    ))
    assert split_df.shape == (12, 10)
    assert split_df.id.min() == 5
    assert split_df.id.max() == 115
    
def test_get_batch_with_split_on_multi_column_values(test_df):
    split_df = PandasExecutionEngine().get_batch_data(RuntimeDataBatchSpec(
        batch_data=test_df,
        splitter_method="_split_on_multi_column_values",
        splitter_kwargs={
            "column_names": ["y", "m", "d"],
            "partition_definition": {
                "y": 2020,
                "m": 1,
                "d": 5,
            }
        },
    ))
    assert split_df.shape == (4, 10)
    assert (split_df.date == datetime.date(2020,1,5)).all()

    with pytest.raises(ValueError):
        split_df = PandasExecutionEngine().get_batch_data(RuntimeDataBatchSpec(
            batch_data=test_df,
            splitter_method="_split_on_multi_column_values",
            splitter_kwargs={
                "column_names": ["I", "dont", "exist"],
                "partition_definition": {
                    "y": 2020,
                    "m": 1,
                    "d": 5,
                }
            },
        ))

def test_get_batch_with_split_on_hashed_column(test_df):
    split_df = PandasExecutionEngine().get_batch_data(RuntimeDataBatchSpec(
        batch_data=test_df,
        splitter_method="_split_on_hashed_column",
        splitter_kwargs={
            "column_name":"favorite_color",
            "hash_digits":1,
            "partition_definition":{
                "hash_value": "a",
            }
        }
    ))
    assert split_df.shape == (8, 10)


# ### Sampling methods ###

def test_sample_using_random(test_df):
    random.seed(1)
    sampled_df = PandasExecutionEngine().get_batch_data(RuntimeDataBatchSpec(
        batch_data=test_df,
        sampling_method="_sample_using_random"
    ))
    assert sampled_df.shape == (13, 10)

def test_sample_using_mod(test_df):
    sampled_df = PandasExecutionEngine().get_batch_data(RuntimeDataBatchSpec(
        batch_data=test_df,
        sampling_method="_sample_using_mod",
        sampling_kwargs={
            "column_name":"id",
            "mod":5,
            "value":4,
        }
    ))
    assert sampled_df.shape == (24, 10)

def test_sample_using_a_list(test_df):
    sampled_df = PandasExecutionEngine().get_batch_data(RuntimeDataBatchSpec(
        batch_data=test_df,
        sampling_method="_sample_using_a_list",
        sampling_kwargs={
            "column_name":"id",
            "value_list":[3,5,7,11],
        }
    ))
    assert sampled_df.shape == (4, 10)

def test_sample_using_md5(test_df):
    sampled_df = PandasExecutionEngine().get_batch_data(RuntimeDataBatchSpec(
        batch_data=test_df,
        sampling_method="_sample_using_md5",
        sampling_kwargs={
            "column_name": "date",    
        }
    ))
    assert sampled_df.shape == (10, 10)
    assert sampled_df.date.isin([
        datetime.date(2020,1,15),
        datetime.date(2020,1,29),
    ]).all()
