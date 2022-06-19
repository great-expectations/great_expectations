import pytest

from great_expectations.datasource.new_sqlalchemy_datasource import (
    NewSqlAlchemyDatasource,
)
from great_expectations.validator.validator import Validator


def test_NewSqlAlchemyDatasource_instantiation():
    my_datasource = NewSqlAlchemyDatasource(
        name="chinook",
        connection_string="sqlite:///tests/chinook.db",
    )

@pytest.fixture
def chinook_datasource():
    my_datasource = NewSqlAlchemyDatasource(
        name="chinook",
        connection_string="sqlite:///tests/chinook.db",
    )
    return my_datasource


def test_NewSqlAlchemyDatasource_method_list(chinook_datasource):
    dir_results = dir(chinook_datasource)
    filtered_dir_results = [r for r in dir_results if r[0] != "_"]
    print("\n".join(filtered_dir_results))

    assert set(filtered_dir_results) == set(
        {
            # Properties
            "name",
            "assets",

            # Core methods
            "add_asset",
            "rename_asset",
            "get_batch",
            # "get_batches", #!!! Add this later
            "get_validator",
            "list_asset_names",
            # "self_check",

            # Helper methods
            "update_assets",
            "list_tables",
            "get_table",
        }
    )

def test_NewSqlAlchemyDatasource_get_table(chinook_datasource):
    df = chinook_datasource.get_table("albums")
    pass

def test_NewSqlAlchemyDatasource_list_tables(chinook_datasource):

    chinook_datasource.list_tables()

    chinook_datasource.update_assets(
        chinook_datasource.list_tables(return_as="assets")
    )

    assert chinook_datasource.list_asset_names() == [
        "main.albums",
        "main.artists",
        "main.customers",
        "main.employees",
        "main.genres",
        "main.invoice_items",
        "main.invoices",
        "main.media_types",
        "main.playlist_track",
        "main.playlists",
        "main.sqlite_sequence",
        "main.sqlite_stat1",
        "main.tracks",
    ]





