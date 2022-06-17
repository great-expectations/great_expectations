from ast import List
from great_expectations.core.batch import Batch
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

def test_PandasReaderDataAsset__init__(test_dir_alpha):
    my_datasource = PandasReaderDatasource("my_datasource")

    # Smoke test
    my_asset = PandasReaderDataAsset(
        datasource=my_datasource,
        name="test_dir_alpha",
        method="read_csv",
        base_directory=f"{test_dir_alpha}/test_dir_alpha/",
        regex="(*.)\\.csv",
        batch_identifiers=["filename"],
    )

    # Instantiate as a runtime data asset
    my_asset = PandasReaderDataAsset(
        datasource=my_datasource,
        name="test_dir_alpha",
        batch_identifiers=["filename"],
    )

    my_asset = PandasReaderDataAsset(
        datasource=my_datasource,
        name="test_dir_alpha",
        batch_identifiers=["id", "timestamp"],
    )


    # !!! This should throw an error: "Cannot declare method or regex when base_directory=None"
    PandasReaderDataAsset(
        datasource=my_datasource,
        name="test_dir_alpha",
        method="read_csv",
        regex="(*.)\\.csv",
        batch_identifiers=["filename"],
    )

    # !!! If base_directory is not Null, method and regex should be populated as well. defaults are okay.
    # !!! If regex is not Null, the number of groups should be exactly equal to the number of parameters in batch_identifiers


@pytest.fixture
def alpha_test_files_pandas_reader_data_asset(test_dir_alpha) -> PandasReaderDataAsset:
    my_datasource = PandasReaderDatasource("my_datasource")
    return PandasReaderDataAsset(
        datasource=my_datasource,
        name="test_dir_alpha",
        method="read_csv",
        base_directory=test_dir_alpha,
        regex="(*.)\\.csv",
        batch_identifiers=["filename"],
    )

def test_PandasReaderDataAsset_get_batch_request(alpha_test_files_pandas_reader_data_asset):
    my_asset = alpha_test_files_pandas_reader_data_asset

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

@pytest.mark.skip(reason="Doesn't work yet")
def test_PandasReaderDataAsset_list_batches(alpha_test_files_pandas_reader_data_asset):
    my_asset = alpha_test_files_pandas_reader_data_asset
    print(my_asset.list_batches())
    assert len(my_asset.list_batches()) == 4

    sample_batch = my_asset.list_batches()[0]
    assert isinstance(sample_batch, NewBatchRequestBase)

    #!!! Test to make sure that we're getting back real BatchRequests

@pytest.mark.skip(reason="Doesn't work yet")
def test_PandasReaderDataAsset_batches(alpha_test_files_pandas_reader_data_asset):
    my_asset = alpha_test_files_pandas_reader_data_asset
    assert len(my_asset.batches) == 4
    assert isinstance(my_asset.batches[0], Batch)
    assert isinstance(my_asset.batches[:2], List[Batch])


def test_PandasReaderDataAsset_update_configuration():
    #!!!
    pass

def test_PandasReaderDataAsset_get_validator():
    #!!!
    pass

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
