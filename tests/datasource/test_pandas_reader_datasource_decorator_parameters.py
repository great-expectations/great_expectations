import datetime

import pandas as pd
import pytest
import sqlalchemy as sa

from great_expectations.data_context.util import file_relative_path
from great_expectations.datasource.base_data_asset import (
    BatchSpecPassthrough,
    DataConnectorQuery,
    NewConfiguredBatchRequest,
)
from great_expectations.datasource.pandas_reader_data_asset import PandasReaderDataAsset
from great_expectations.datasource.runtime_pandas_datasource import (
    RuntimePandasDatasource,
)
from tests.test_utils import _get_batch_request_from_validator, _get_data_from_validator

### Tests for RuntimePandasDatasource.read_csv ###
# These are thorough, covering pretty much all of the API surface area for the new read_* methods, including error states


def test_PandasReaderDatasource_read_csv_basic():
    my_datasource = RuntimePandasDatasource("my_datasource")
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
        datasource_name="my_datasource",
        data_asset_name="DEFAULT_DATA_ASSET",
        data_connector_query=DataConnectorQuery(
            timestamp=0,
            id_=file_relative_path(__file__, "fixtures/example_1.csv"),
        ),
        batch_spec_passthrough=BatchSpecPassthrough(
            args=[],
            kwargs={},
        ),
    )


def test_PandasReaderDatasource_read_csv_with_real_timestamp():
    my_datasource = RuntimePandasDatasource("my_datasource")
    now = datetime.datetime.now()
    my_validator = my_datasource.read_csv(
        file_relative_path(__file__, "fixtures/example_1.csv"),
    )
    my_batch_request = _get_batch_request_from_validator(my_validator)
    time_delta = now - my_batch_request.data_connector_query["timestamp"]
    assert time_delta.total_seconds() < 1


def test_PandasReaderDatasource_read_csv_with_use_primary_arg_as_id__eq__false():
    my_datasource = RuntimePandasDatasource("my_datasource")
    my_validator = my_datasource.read_csv(
        file_relative_path(__file__, "fixtures/example_1.csv"),
        use_primary_arg_as_id=False,
    )
    my_batch_request = _get_batch_request_from_validator(my_validator)
    assert my_batch_request.data_connector_query["id_"] == None


def test_PandasReaderDatasource_read_csv_with_use_primary_arg_as_id__eq__false_and_an_id_():
    my_datasource = RuntimePandasDatasource("my_datasource")
    my_validator = my_datasource.read_csv(
        file_relative_path(__file__, "fixtures/example_1.csv"),
        use_primary_arg_as_id=False,
        id_="Here's an ID!",
    )
    my_batch_request = _get_batch_request_from_validator(my_validator)
    assert my_batch_request.data_connector_query["id_"] == "Here's an ID!"


def test_PandasReaderDatasource_read_csv_with_use_primary_arg_as_id__eq__true_and_an_id_():
    my_datasource = RuntimePandasDatasource("my_datasource")
    with pytest.raises(ValueError):
        my_datasource.read_csv(
            file_relative_path(__file__, "fixtures/example_1.csv"),
            timestamp=0,
            use_primary_arg_as_id=True,
            id_="Here's an ID!",
        )


def test_PandasReaderDatasource_read_csv_with_sep():
    my_datasource = RuntimePandasDatasource("my_datasource")
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
        datasource_name="my_datasource",
        data_asset_name="DEFAULT_DATA_ASSET",
        data_connector_query=DataConnectorQuery(
            timestamp=0,
            id_=file_relative_path(__file__, "fixtures/example_2.csv"),
        ),
        batch_spec_passthrough=BatchSpecPassthrough(
            args=[],
            kwargs={"sep": "   "},
        ),
    )


def test_PandasReaderDatasource_read_csv_with_sep_as_positional_arg():
    my_datasource = RuntimePandasDatasource("my_datasource")
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
        datasource_name="my_datasource",
        data_asset_name="DEFAULT_DATA_ASSET",
        data_connector_query=DataConnectorQuery(
            timestamp=0,
            id_=file_relative_path(__file__, "fixtures/example_2.csv"),
        ),
        batch_spec_passthrough=BatchSpecPassthrough(
            args=["   "],
            kwargs={},
        ),
    )


def test_PandasReaderDatasource_read_csv_with_buffer():
    my_datasource = RuntimePandasDatasource("my_datasource")
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
        datasource_name="my_datasource",
        data_asset_name="DEFAULT_DATA_ASSET",
        data_connector_query=DataConnectorQuery(
            timestamp=0,
            id_=None,
        ),
        batch_spec_passthrough=BatchSpecPassthrough(
            args=[],
            kwargs={},
        ),
    )


@pytest.mark.skip(reason="Unsure if this is the behavior that we want.")
def test_PandasReaderDatasource_read_csv_with_buffer_and_use_primary_arg_as_id():
    # !!! Here's what this does currently. I'm not sure if this is the behavior that we want.
    # TypeError: <_io.TextIOWrapper name='/Users/abe/Documents/great_expectations/tests/datasource/fixtures/example_1.csv' mode='r' encoding='UTF-8'> is of type TextIOWrapper which cannot be serialized.

    my_datasource = RuntimePandasDatasource("my_datasource")
    with pytest.raises(TypeError):
        with open(file_relative_path(__file__, "fixtures/example_1.csv")) as file:
            my_validator = my_datasource.read_csv(
                file, timestamp=0, use_primary_arg_as_id=True
            )


def test_PandasReaderDatasource_read_csv_with_filepath_or_buffer_argument():
    my_datasource = RuntimePandasDatasource("my_datasource")
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
    my_datasource = RuntimePandasDatasource("my_datasource")
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

    my_datasource = RuntimePandasDatasource("my_datasource")
    my_validator = my_datasource.read_csv(
        filepath_or_buffer=file_relative_path(__file__, "fixtures/example_1.csv"),
        date_parser=date_parser,
    )

    my_batch_request = _get_batch_request_from_validator(my_validator)
    assert my_batch_request.batch_spec_passthrough["kwargs"] == {
        "date_parser": "<<non-serializable>>"
    }


def test_PandasReaderDatasource_read_csv__with_data_asset_name():
    my_datasource = RuntimePandasDatasource("my_datasource")
    assert my_datasource.list_asset_names() == []

    my_validator = my_datasource.read_csv(
        file_relative_path(__file__, "fixtures/example_1.csv"),
        data_asset_name="my_new_data_asset",
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
        datasource_name="my_datasource",
        data_asset_name="my_new_data_asset",
        data_connector_query=DataConnectorQuery(
            timestamp=0,
            id_=file_relative_path(__file__, "fixtures/example_1.csv"),
        ),
        batch_spec_passthrough=BatchSpecPassthrough(
            args=[],
            kwargs={},
        ),
    )

    # This operation automatically configures a new asset on the datasource
    assert my_datasource.list_asset_names() == ["my_new_data_asset"]
    test_asset = PandasReaderDataAsset(
        datasource=my_datasource,
        name="my_new_data_asset",
        base_directory="",
        batch_identifiers=["filename"],
        method="read_csv",
        regex="(.*)",
    )
    assert my_datasource.assets.my_new_data_asset == test_asset
