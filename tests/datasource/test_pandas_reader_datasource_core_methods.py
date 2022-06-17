import datetime

import pandas as pd
from great_expectations.datasource.base_data_asset import BatchSpecPassthrough, DataConnectorQuery, NewConfiguredBatchRequest
from great_expectations.datasource.pandas_reader_data_asset import PandasReaderDataAsset
from great_expectations.types import DictDot
import pytest
import sqlalchemy as sa

from great_expectations.data_context.util import file_relative_path
from great_expectations.datasource.pandas_reader_datasource import (
    PandasReaderDatasource,
)
from tests.test_utils import (
    create_files_in_directory,
)

from tests.datasource.new_fixtures import test_dir_alpha

def test_PandasReaderDatasource_method_list():
    my_datasource = PandasReaderDatasource("my_datasource")
    dir_results = dir(my_datasource)
    filtered_dir_results = [ r for r in dir_results if r[0] != "_"]
    print("\n".join(filtered_dir_results))

    assert set(filtered_dir_results) == set({
        # Properties
        "name",
        "assets",

        # Core methods
        "add_asset",
        "rename_asset",
        "get_batch",
        ### "get_batches", #!!! Add this later
        "get_validator",
        "list_asset_names",
        # "self_check",
        
        # read_* methods
        "from_dataframe",
        "read_clipboard",
        "read_csv",
        "read_excel",
        "read_feather",
        "read_fwf",
        "read_gbq",
        "read_hdf",
        "read_html",
        "read_json",
        "read_orc",
        "read_parquet",
        "read_pickle",
        "read_sas",
        "read_spss",
        "read_sql",
        "read_sql_query",
        "read_sql_table",
        "read_stata",
        "read_table",
        "read_xml",
    })


def test_PandasReaderDatasource_add_asset(test_dir_alpha):
    my_datasource = PandasReaderDatasource("my_datasource")
    assert my_datasource.list_asset_names() == []

    my_datasource.add_asset(
        name="test_dir_alpha",
        method="read_csv",
        base_directory=test_dir_alpha,
        regex="(*.)\\.csv",
        batch_identifiers=["filename"],
    )

    assert my_datasource.list_asset_names() == ["test_dir_alpha"]
    assert len(my_datasource.assets) == 1

    # !!! Some of these tests should fail.

    #duplicate asset name
    my_datasource.add_asset(
        name="test_dir_alpha",
        base_directory="test_file_directories/test_dir_alpha/",
    )

    #!!! What if asset names aren't valid python names?
    my_datasource.add_asset(
        name="I'm a horrible name",
        base_directory="test_file_directories/test_dir_alpha/",
    )


    #relative filepath
    my_datasource.add_asset(
        name="test_dir_alpha",
        base_directory="test_file_directories/test_dir_alpha/",
    )

    #absolute filepath
    my_datasource.add_asset(
        name="test_dir_alpha",
        base_directory="test_file_directories/test_dir_alpha/", #!!! Make this absolute
    #     regex="(*.)\.csv", # Regex defaults to the whole filename
    #     batch_identifiers=[("filename_letter")], #batch_identifiers defaults to "filename"
    #     sorter
    #     method_for_loading="read_csv", #method_for_loading defaults to read_csv
    #     other arguments
    )

    #using regex and batch_identifiers
    my_datasource.add_asset(
        name="test_dir_alpha",
        base_directory="test_file_directories/test_dir_alpha/",
        regex="(*.)\\.csv",
        batch_identifiers=["filename_letter"],
    #     method_for_loading="read_csv", #method_for_loading defaults to read_csv
    #     other arguments
    )

    #using custom sorters

    # assert my_datasource.list_asset_names() == ["test_dir_alpha"]

def test_PandasReaderDatasource_rename_asset():
    my_datasource = PandasReaderDatasource("my_datasource")
    assert my_datasource.list_asset_names() == []

    my_datasource.add_asset("A")
    assert my_datasource.list_asset_names() == ["A"]

    with pytest.raises(TypeError):
        # None isn't a valid asset name.
        my_datasource.rename_asset("A", None)
    assert my_datasource.list_asset_names() == ["A"]

    my_datasource.rename_asset("A", "B")
    assert my_datasource.list_asset_names() == ["B"]

    my_datasource.add_asset("A")
    assert my_datasource.list_asset_names() == ["B", "A"]

    with pytest.raises(KeyError):
        # An asset named B already exists
        my_datasource.rename_asset("A", "B")
    assert my_datasource.list_asset_names() == ["B", "A"]



def test_PandasReaderDatasource_asset_property(test_dir_alpha):
    my_datasource = PandasReaderDatasource("my_datasource")
    print(my_datasource.assets)
    assert len(my_datasource.assets) == 0

    my_datasource.add_asset(
        name="test_dir_alpha",
        method="read_csv",
        base_directory=test_dir_alpha,
        regex="(*.)\\.csv",
        batch_identifiers=["filename"],
    )
    assert len(my_datasource.assets) == 1

    new_asset_test_obj = PandasReaderDataAsset(
        datasource=my_datasource,
        name="test_dir_alpha",
        method="read_csv",
        base_directory=test_dir_alpha,
        regex="(*.)\\.csv",
        batch_identifiers=["filename"],
    )

    # assets is accessible through both dot and dict notation
    assert my_datasource.assets.test_dir_alpha == new_asset_test_obj
    assert my_datasource.assets["test_dir_alpha"] == new_asset_test_obj

    # assets supports item assignment (???)
    # !!! I think we want to forbid this, and require users to use .add_asset
    my_datasource.assets["test_dir_beta"] = PandasReaderDataAsset(
        datasource=my_datasource,
        name="some_other_asset",
        method="read_csv",
        base_directory=test_dir_alpha,
        regex="(*.)\\.csv",
        batch_identifiers=["filename"],
    )

def test_PandasReaderDatasource_get_batch(test_dir_alpha):
    my_datasource = PandasReaderDatasource("my_datasource")
    my_datasource.add_asset(
        name="test_dir_alpha",
        method="read_csv",
        base_directory=test_dir_alpha,
        regex="(.*)\\.csv",
        batch_identifiers=["filename"],
    )

    batch_request = NewConfiguredBatchRequest(
        datasource_name="my_datasource",
        data_asset_name="test_dir_alpha",
        data_connector_query=DataConnectorQuery(
            filename="A"
        ),
        batch_spec_passthrough=BatchSpecPassthrough(
            args=[],
            kwargs={},
        ),
    )
    my_datasource.get_batch(batch_request)