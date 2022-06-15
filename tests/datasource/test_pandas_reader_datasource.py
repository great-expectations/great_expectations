import datetime

import pandas as pd
from great_expectations.datasource.base_data_asset import BatchSpecPassthrough, DataConnectorQuery, NewConfiguredBatchRequest
import pytest
import sqlalchemy as sa

from great_expectations.data_context.util import file_relative_path
from great_expectations.datasource.pandas_reader_datasource import (
    PandasReaderDatasource,
)
from tests.test_utils import (
    _get_batch_request_from_validator,
    _get_data_from_validator,
)

### Tests for PandasReaderDatasource.read_csv ###
# These are thorough, covering pretty much all of the API surface area for the new read_* methods, including error states

def test_PandasReaderDatasource_read_csv_basic():
    my_datasource = PandasReaderDatasource("my_datasource")
    my_validator = my_datasource.read_csv(
        file_relative_path(__file__, "fixtures/example_1.csv"),
        timestamp=0,
    )

    my_data = _get_data_from_validator(my_validator)
    assert isinstance(my_data, pd.DataFrame)
    assert my_data.to_dict() == {
        "a": {0: 1, 1: 4},
        "b": {0: 2, 1: 5},
        "c": {0: 3, 1: 6},
    }

    my_batch_request = _get_batch_request_from_validator(my_validator)
    assert my_batch_request == NewConfiguredBatchRequest(
        datasource_name= "my_datasource",
        data_asset_name= "default_data_asset",
        data_connector_query= DataConnectorQuery(
            timestamp= 0,
            id_= file_relative_path(__file__, "fixtures/example_1.csv"),
        ),
        batch_spec_passthrough= BatchSpecPassthrough(
            args = [],
            kwargs = {},
        )
    )

def test_PandasReaderDatasource_read_csv_with_real_timestamp():
    my_datasource = PandasReaderDatasource("my_datasource")
    now = datetime.datetime.now()
    my_validator = my_datasource.read_csv(
        file_relative_path(__file__, "fixtures/example_1.csv"),
    )
    my_batch_request = _get_batch_request_from_validator(my_validator)
    time_delta = now - my_batch_request.data_connector_query["timestamp"]
    assert time_delta.total_seconds() < 1


def test_PandasReaderDatasource_read_csv_with_use_primary_arg_as_id__eq__false():
    my_datasource = PandasReaderDatasource("my_datasource")
    my_validator = my_datasource.read_csv(
        file_relative_path(__file__, "fixtures/example_1.csv"),
        use_primary_arg_as_id=False,
    )
    my_batch_request = _get_batch_request_from_validator(my_validator)
    assert my_batch_request.data_connector_query["id_"] == None


def test_PandasReaderDatasource_read_csv_with_use_primary_arg_as_id__eq__false_and_an_id_():
    my_datasource = PandasReaderDatasource("my_datasource")
    my_validator = my_datasource.read_csv(
        file_relative_path(__file__, "fixtures/example_1.csv"),
        use_primary_arg_as_id=False,
        id_="Here's an ID!",
    )
    my_batch_request = _get_batch_request_from_validator(my_validator)
    assert my_batch_request.data_connector_query["id_"] == "Here's an ID!"


def test_PandasReaderDatasource_read_csv_with_use_primary_arg_as_id__eq__true_and_an_id_():
    my_datasource = PandasReaderDatasource("my_datasource")
    with pytest.raises(ValueError):
        my_datasource.read_csv(
            file_relative_path(__file__, "fixtures/example_1.csv"),
            timestamp=0,
            use_primary_arg_as_id=True,
            id_="Here's an ID!",
        )


def test_PandasReaderDatasource_read_csv_with_sep():
    my_datasource = PandasReaderDatasource("my_datasource")
    my_validator = my_datasource.read_csv(
        file_relative_path(__file__, "fixtures/example_2.csv"),
        timestamp=0,
        sep="   ",
    )

    my_data = _get_data_from_validator(my_validator)
    assert isinstance(my_data, pd.DataFrame)
    assert my_data.to_dict() == {
        "a": {0: 1, 1: 4},
        "b": {0: 2, 1: 5},
        "c": {0: 3, 1: 6},
    }

    my_batch_request = _get_batch_request_from_validator(my_validator)
    assert my_batch_request == NewConfiguredBatchRequest(
        datasource_name= "my_datasource",
        data_asset_name= "default_data_asset",
        data_connector_query= DataConnectorQuery(
            timestamp= 0,
            id_= file_relative_path(__file__, "fixtures/example_2.csv"),
        ),
        batch_spec_passthrough= BatchSpecPassthrough(
            args = [],
            kwargs = {"sep": "   "},
        )
    )

def test_PandasReaderDatasource_read_csv_with_sep_as_positional_arg():
    my_datasource = PandasReaderDatasource("my_datasource")
    my_validator = my_datasource.read_csv(
        file_relative_path(__file__, "fixtures/example_2.csv"),
        "   ",
        timestamp=0,
    )

    my_data = _get_data_from_validator(my_validator)
    assert isinstance(my_data, pd.DataFrame)
    assert my_data.to_dict() == {
        "a": {0: 1, 1: 4},
        "b": {0: 2, 1: 5},
        "c": {0: 3, 1: 6},
    }

    my_batch_request = _get_batch_request_from_validator(my_validator)
    assert my_batch_request == NewConfiguredBatchRequest(
        datasource_name= "my_datasource",
        data_asset_name= "default_data_asset",
        data_connector_query= DataConnectorQuery(
            timestamp= 0,
            id_= file_relative_path(__file__, "fixtures/example_2.csv"),
        ),
        batch_spec_passthrough= BatchSpecPassthrough(
            args = ["   "],
            kwargs = {},
        )
    )

def test_PandasReaderDatasource_read_csv_with_buffer():
    my_datasource = PandasReaderDatasource("my_datasource")
    with open(file_relative_path(__file__, "fixtures/example_1.csv")) as file:
        my_validator = my_datasource.read_csv(
            file,
            timestamp=0,
        )

    my_data = _get_data_from_validator(my_validator)
    assert isinstance(my_data, pd.DataFrame)
    assert my_data.to_dict() == {
        "a": {0: 1, 1: 4},
        "b": {0: 2, 1: 5},
        "c": {0: 3, 1: 6},
    }
    # del my_data

    my_batch_request = _get_batch_request_from_validator(my_validator)
    assert my_batch_request == NewConfiguredBatchRequest(
        datasource_name= "my_datasource",
        data_asset_name= "default_data_asset",
        data_connector_query= DataConnectorQuery(
            timestamp= 0,
            id_= None,
        ),
        batch_spec_passthrough= BatchSpecPassthrough(
            args = [],
            kwargs = {},
        )
    )

@pytest.mark.skip(reason="Unsure if this is the behavior that we want.")
def test_PandasReaderDatasource_read_csv_with_buffer_and_use_primary_arg_as_id():
    # !!! Here's what this does currently. I'm not sure if this is the behavior that we want.
    #TypeError: <_io.TextIOWrapper name='/Users/abe/Documents/great_expectations/tests/datasource/fixtures/example_1.csv' mode='r' encoding='UTF-8'> is of type TextIOWrapper which cannot be serialized.

    my_datasource = PandasReaderDatasource("my_datasource")
    with pytest.raises(TypeError):
        with open(file_relative_path(__file__, "fixtures/example_1.csv")) as file:
            my_validator = my_datasource.read_csv(
                file, timestamp=0, use_primary_arg_as_id=True
            )


def test_PandasReaderDatasource_read_csv_with_filepath_or_buffer_argument():
    my_datasource = PandasReaderDatasource("my_datasource")
    my_validator = my_datasource.read_csv(
        filepath_or_buffer=file_relative_path(__file__, "fixtures/example_1.csv"),
    )

    my_data = _get_data_from_validator(my_validator)
    assert isinstance(my_data, pd.DataFrame)
    assert my_data.to_dict() == {
        "a": {0: 1, 1: 4},
        "b": {0: 2, 1: 5},
        "c": {0: 3, 1: 6},
    }


def test_PandasReaderDatasource_read_csv_with_filepath_or_buffer_argument_and_a_positional_argument():
    my_datasource = PandasReaderDatasource("my_datasource")
    with pytest.raises(TypeError) as exc:
        my_validator = my_datasource.read_csv(
            "conflicting argument to filepath_or_buffer",
            filepath_or_buffer=file_relative_path(__file__, "fixtures/example_1.csv"),
        )
    assert (
        str(exc.value)
        == "read_csv() got multiple values for argument filepath_or_buffer"
    )


#!!!
@pytest.mark.skip(reason="This might require deeper surgery on BatchRequest class")
def test_PandasReaderDatasource_read_csv_with_nonserializable_parameter():
    def date_parser(x):
        return x

    my_datasource = PandasReaderDatasource("my_datasource")
    my_validator = my_datasource.read_csv(
        filepath_or_buffer=file_relative_path(__file__, "fixtures/example_1.csv"),
        date_parser=date_parser,
    )

    my_batch_request = _get_batch_request_from_validator(my_validator)
    assert my_batch_request.batch_spec_passthrough["kwargs"] == {
        "date_parser": "<<non-serializable>>"
    }


### Tests of other methods. These don't go into edge cases, because we trust the decorators to cover them.


def test_PandasReaderDatasource_read_json():
    my_datasource = PandasReaderDatasource("my_datasource")
    my_validator = my_datasource.read_json(
        '{"a":[1,4], "b":[2,5], "c":[3,6]}',
        timestamp=0,
    )

    my_data = _get_data_from_validator(my_validator)
    assert isinstance(my_data, pd.DataFrame)
    assert my_data.to_dict() == {
        "a": {0: 1, 1: 4},
        "b": {0: 2, 1: 5},
        "c": {0: 3, 1: 6},
    }

    my_batch_request = _get_batch_request_from_validator(my_validator)
    assert my_batch_request.data_connector_query == {
        "timestamp": 0,
        "id_": None,
    }


def test_PandasReaderDatasource_read_table():
    my_datasource = PandasReaderDatasource("my_datasource")
    my_validator = my_datasource.read_table(
        file_relative_path(__file__, "fixtures/example_3.tsv"),
        delimiter="|",
        skiprows=2,
    )

    my_data = _get_data_from_validator(my_validator)
    assert isinstance(my_data, pd.DataFrame)
    assert my_data.to_dict() == {
        "a": {0: 1, 1: 4},
        "b": {0: 2, 1: 5},
        "c": {0: 3, 1: 6},
    }


@pytest.mark.skip(
    "This test doesn't work on some headless infrastructure, including our CI setup."
)
def test_PandasReaderDatasource_read_clipboard():
    import pyperclip

    old_clipboard_text = pyperclip.paste()

    pyperclip.copy(
        """
	a	b	c
0	1	2	3
1	4	5	6
"""
    )

    my_datasource = PandasReaderDatasource("my_datasource")
    my_validator = my_datasource.read_clipboard(timestamp=0)

    my_data = _get_data_from_validator(my_validator)
    assert isinstance(my_data, pd.DataFrame)
    assert my_data.to_dict() == {
        "a": {0: 1, 1: 4},
        "b": {0: 2, 1: 5},
        "c": {0: 3, 1: 6},
    }

    my_batch_request = _get_batch_request_from_validator(my_validator)
    assert my_batch_request.data_connector_query == {
        "timestamp": 0,
        "id_": None,
    }

    # Restore the old contents of the clipboard
    pyperclip.copy(old_clipboard_text)


@pytest.fixture
def sqlite_engine():
    engine = sa.create_engine("sqlite://")
    df = pd.DataFrame(
        {
            "a": [1, 4],
            "b": [2, 5],
            "c": [3, 6],
        }
    )
    df.to_sql(name="test_table", con=engine, index=False)
    return engine


def test_PandasReaderDatasource_read_sql_table_with_con_as_keyword_arg(sqlite_engine):
    my_datasource = PandasReaderDatasource("my_datasource")
    my_validator = my_datasource.read_sql_table(
        "test_table", con=sqlite_engine, timestamp=0
    )

    my_data = _get_data_from_validator(my_validator)
    assert isinstance(my_data, pd.DataFrame)
    assert my_data.to_dict() == {
        "a": {0: 1, 1: 4},
        "b": {0: 2, 1: 5},
        "c": {0: 3, 1: 6},
    }

    my_batch_request = _get_batch_request_from_validator(my_validator)
    assert my_batch_request.batch_spec_passthrough["args"] == []
    assert my_batch_request.batch_spec_passthrough["kwargs"] == {}
    assert my_batch_request.data_connector_query == {
        "timestamp": 0,
        "id_": "test_table",
    }


def test_PandasReaderDatasource_read_sql_table_with_con_as_positional_arg(
    sqlite_engine,
):
    my_datasource = PandasReaderDatasource("my_datasource")
    my_validator = my_datasource.read_sql_table(
        "test_table", sqlite_engine, timestamp=0
    )

    my_data = _get_data_from_validator(my_validator)    
    assert isinstance(my_data, pd.DataFrame)
    assert my_data.to_dict() == {
        "a": {0: 1, 1: 4},
        "b": {0: 2, 1: 5},
        "c": {0: 3, 1: 6},
    }

    my_batch_request = _get_batch_request_from_validator(my_validator)
    assert my_batch_request.batch_spec_passthrough["args"] == []
    assert my_batch_request.batch_spec_passthrough["kwargs"] == {}
    assert my_batch_request.data_connector_query == {
        "timestamp": 0,
        "id_": "test_table",
    }


def test_PandasReaderDatasource_read_sql_query(sqlite_engine):
    my_datasource = PandasReaderDatasource("my_datasource")
    my_validator = my_datasource.read_sql_query(
        "SELECT * FROM test_table;", con=sqlite_engine, timestamp=0
    )

    my_data = _get_data_from_validator(my_validator)
    assert isinstance(my_data, pd.DataFrame)
    assert my_data.to_dict() == {
        "a": {0: 1, 1: 4},
        "b": {0: 2, 1: 5},
        "c": {0: 3, 1: 6},
    }

    my_batch_request = _get_batch_request_from_validator(my_validator)
    assert my_batch_request.batch_spec_passthrough["args"] == []
    assert my_batch_request.batch_spec_passthrough["kwargs"] == {}
    assert my_batch_request.data_connector_query == {
        "timestamp": 0,
        "id_": None,
    }


def test_PandasReaderDatasource_read_sql_with_query(sqlite_engine):
    my_datasource = PandasReaderDatasource("my_datasource")
    my_validator = my_datasource.read_sql(
        "SELECT * FROM test_table;", con=sqlite_engine, timestamp=0
    )
    my_data = _get_data_from_validator(my_validator)
    assert isinstance(
        my_data, pd.DataFrame
    )
    assert my_data.to_dict() == {
        "a": {0: 1, 1: 4},
        "b": {0: 2, 1: 5},
        "c": {0: 3, 1: 6},
    }


    my_batch_request = _get_batch_request_from_validator(my_validator)
    assert isinstance(my_batch_request, NewConfiguredBatchRequest)
    assert my_batch_request.batch_spec_passthrough["args"] == []
    assert my_batch_request.batch_spec_passthrough["kwargs"] == {}
    assert my_batch_request.data_connector_query == {
        "timestamp": 0,
        "id_": None,
    }


def test_PandasReaderDatasource_read_sql_with_table(sqlite_engine):
    my_datasource = PandasReaderDatasource("my_datasource")
    my_validator = my_datasource.read_sql("test_table", con=sqlite_engine, timestamp=0)
    
    my_data = _get_data_from_validator(my_validator)
    assert isinstance(my_data, pd.DataFrame)
    assert my_data.to_dict() == {
        "a": {0: 1, 1: 4},
        "b": {0: 2, 1: 5},
        "c": {0: 3, 1: 6},
    }
    
    my_batch_request: NewConfiguredBatchRequest = _get_batch_request_from_validator(my_validator)
    assert isinstance(my_batch_request, NewConfiguredBatchRequest)
    assert my_batch_request.batch_spec_passthrough["args"] == []
    assert my_batch_request.batch_spec_passthrough["kwargs"] == {}
    assert my_batch_request.data_connector_query == {
        "timestamp": 0,
        "id_": "test_table",
    }


@pytest.mark.skip("For convenience")
def test_PandasReaderDatasource_read_dataframe():
    my_datasource = PandasReaderDatasource("my_datasource")
    my_df = pd.DataFrame(
        {
            "a": [1, 4],
            "b": [2, 5],
            "c": [3, 6],
        }
    )

    my_validator = my_datasource.from_dataframe(
        my_df,
        timestamp=0,
    )
    my_batch_request = _get_batch_request_from_validator(my_validator)

    assert isinstance(
        my_batch_requestbatch_spec_passthrough["batch_data"], pd.DataFrame
    )

    print("&"*80)
    print(my_batch_request)
    print(my_batch_request.batch_spec_passthrough)
    print(my_data)

    assert my_data.to_dict() == {
        "a": {0: 1, 1: 4},
        "b": {0: 2, 1: 5},
        "c": {0: 3, 1: 6},
    }
    assert my_batch_request.batch_spec_passthrough["args"] == []
    assert my_batch_request.batch_spec_passthrough["kwargs"] == {}
    assert my_batch_requestdata_connector_query == {
        "timestamp": 0,
        "id_": None,
    }
