import datetime

import pandas as pd
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

def test_PandasReaderDatasource_add_asset(test_dir_alpha):
    my_datasource = PandasReaderDatasource("my_datasource")
    assert my_datasource.list_data_asset_names() == []

    my_datasource.add_asset(
        name="test_dir_alpha",
        method="read_csv",
        base_directory=test_dir_alpha,
        regex="(*.)\.csv",
        batch_identifiers=["filename"],
    )

    assert my_datasource.list_data_asset_names() == ["test_dir_alpha"]


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
        regex="(*.)\.csv",
        batch_identifiers=["filename_letter"],
    #     method_for_loading="read_csv", #method_for_loading defaults to read_csv
    #     other arguments
    )

    #using custom sorters


def test_PandasReaderDatasource_add_asset_from_method_signature():
    """This is scratch code"""
    return

    my_validator = my_datasource.read_csv(my_file_name)
    new_asset = my_datasource.get_asset_from_method_signature(
        'read_json',
        my_file_name,
    )
    assert new_asset == PandasReaderDataAsset(
        name="???",

    )

    my_datasource.add_asset_from_method_signature(
        'read_json',
        my_file_name,
    )


    my_datasource.read_csv(
            file_relative_path(__file__, "fixtures/example_1.csv"),
            use_primary_arg_as_id=False,
        )
