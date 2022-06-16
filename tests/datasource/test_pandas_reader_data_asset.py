import pytest

from great_expectations.datasource.pandas_reader_datasource import PandasReaderDatasource
from great_expectations.datasource.base_data_asset import (
    BatchIdentifierException,
    BatchSpecPassthrough,
    DataConnectorQuery,
    NewBatchRequestBase,
    NewConfiguredBatchRequest,
)
from great_expectations.datasource.pandas_reader_data_asset import (
    PandasReaderDataAsset,
)

from tests.datasource.new_fixtures import test_dir_alpha

def test_PandasReaderDataAsset_basic_get_batch_request(test_dir_alpha):

    my_datasource = PandasReaderDatasource("my_datasource")
    my_asset = PandasReaderDataAsset(
        datasource=my_datasource,
        name="test_dir_alpha",
        method="read_csv",
        base_directory=f"{test_dir_alpha}/test_dir_alpha/",
        regex="(*.)\\.csv",
        batch_identifiers=["filename"],
    )

    my_batch_request = my_asset.get_batch_request("A")
    assert isinstance(my_batch_request, NewBatchRequestBase)
    assert my_batch_request == NewConfiguredBatchRequest(
        datasource_name = "my_datasource",
        data_asset_name = "test_dir_alpha",
        data_connector_query=DataConnectorQuery(
            filename= "A"
        ),
        batch_spec_passthrough=BatchSpecPassthrough(),
    )

# @pytest.fixture
# def configured_pandas_reader_data_asset():
#     my_datasource = PandasReaderDatasource("my_datasource")
#     my_asset = PandasReaderDataAsset(
#         datasource=my_datasource,
#         name="test_dir_alpha",
#         method="read_csv",
#         base_directory="some_dir/",
#         regex="(.*)\.(.*)",
#         batch_identifiers=["filename", "file_extension"],
#     )
#     return my_asset

def test_PandasReaderDataAsset__generate_batch_identifiers_from_args_and_kwargs():
    my_datasource = PandasReaderDatasource("my_datasource")
    my_asset = PandasReaderDataAsset(
        datasource=my_datasource,
        name="test_dir_alpha",
        method="read_csv",
        base_directory="some_dir/",
        regex="(.*)\\.(.*)",
        batch_identifiers=["filename", "file_extension"],
    )

    assert my_asset._generate_batch_identifiers_from_args_and_kwargs(
        batch_identifier_args = ["some_file", "csv"],
        batch_identifier_kwargs = {},
    ) == DataConnectorQuery(
        filename="some_file",
        file_extension="csv",
    )

    assert my_asset._generate_batch_identifiers_from_args_and_kwargs(
        batch_identifier_args = ["some_file"],
        batch_identifier_kwargs = {
            "file_extension": "csv"
        },
    ) == DataConnectorQuery(
        filename="some_file",
        file_extension="csv",
    )

    # kwargs have wrong names
    with pytest.raises(BatchIdentifierException):
        my_asset._generate_batch_identifiers_from_args_and_kwargs(
            batch_identifier_args = [],
            batch_identifier_kwargs = {
                "filename": "some_file",
                "wrong_name": "csv",
            },
        )

    # Not enough args and kwargs to fully specify
    with pytest.raises(BatchIdentifierException):
        my_asset._generate_batch_identifiers_from_args_and_kwargs(
            batch_identifier_args = [],
            batch_identifier_kwargs = {},
        )

    # Too many args
    with pytest.raises(BatchIdentifierException):
        my_asset._generate_batch_identifiers_from_args_and_kwargs(
            batch_identifier_args = ["some_file", "csv", "not_a_real_batch_identifier"],
            batch_identifier_kwargs = {},
        )

    # args and kwargs conflict
    with pytest.raises(BatchIdentifierException):
        my_asset._generate_batch_identifiers_from_args_and_kwargs(
            batch_identifier_args = ["some_file", "csv"],
            batch_identifier_kwargs = {
                "filename": "other_filename"
            },
        )

def test_PandasReaderDataAsset_list_batches():
    pass
    # assert my_asset.list_batches() == [
    #     BatchRequest(

    #     )
    # ]