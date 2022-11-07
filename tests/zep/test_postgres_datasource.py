from contextlib import contextmanager
from typing import Callable, Tuple

import pytest

import great_expectations.zep.postgres_datasource as postgres_datasource
from great_expectations.core.batch import BatchData
from great_expectations.core.batch_spec import (
    BatchMarkers,
    SqlAlchemyDatasourceBatchSpec,
)
from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.zep.interfaces import BatchRequestOptions


@contextmanager
def sqlachemy_execution_engine_mock(
    validate_batch_spec: Callable[[SqlAlchemyDatasourceBatchSpec], None]
):
    class MockSqlAlchemyExecutionEngine(SqlAlchemyExecutionEngine):
        def __init__(self, *args, **kwargs):
            pass

        def get_batch_data_and_markers(
            self, batch_spec: SqlAlchemyDatasourceBatchSpec
        ) -> Tuple[BatchData, BatchMarkers]:
            validate_batch_spec(batch_spec)
            return BatchData(self), BatchMarkers(ge_load_time=None)

    original_engine = postgres_datasource.SqlAlchemyExecutionEngine
    try:
        postgres_datasource.SqlAlchemyExecutionEngine = MockSqlAlchemyExecutionEngine
        yield postgres_datasource.SqlAlchemyExecutionEngine
    finally:
        postgres_datasource.SqlAlchemyExecutionEngine = original_engine


def _source() -> postgres_datasource.PostgresDatasource:
    return postgres_datasource.PostgresDatasource(
        name="my_datasource",
        connection_str="postgresql+psycopg2://postgres:@localhost/test_ci",
    )


def test_construct_postgres_datasource():
    with sqlachemy_execution_engine_mock(lambda x: None):
        source = _source()
        assert source.name == "my_datasource"
        assert isinstance(source.execution_engine, SqlAlchemyExecutionEngine)
        assert source.assets == {}


def assert_table_asset(
    asset: postgres_datasource.TableAsset,
    name: str,
    table_name: str,
    source: postgres_datasource.PostgresDatasource,
    batch_request_template: BatchRequestOptions,
):
    assert asset.name == name
    assert asset.table_name == table_name
    assert asset.datasource == source
    assert asset.batch_request_template() == batch_request_template


def assert_batch_request(
    batch_request, source_name: str, asset_name: str, options: BatchRequestOptions
):
    assert batch_request.datasource_name == source_name
    assert batch_request.data_asset_name == asset_name
    assert batch_request.options == options


@pytest.mark.parametrize(
    "config",
    [
        # column_name, splitter_name, batch_request_template, batch_request_options
        (None, None, {}, {}),
        (
            "my_col",
            None,
            {"year": "<value>", "month": "<value>"},
            {"year": 2021, "month": 10},
        ),
        (
            "my_col",
            "",
            {"year": "<value>", "month": "<value>"},
            {"year": 2021, "month": 10},
        ),
        (
            "my_col",
            "mysplitter",
            {"mysplitter": {"year": "<value>", "month": "<value>"}},
            {"mysplitter": {"year": 2021, "month": 10}},
        ),
    ],
)
def test_add_table_asset(config):
    with sqlachemy_execution_engine_mock(lambda x: None):
        source = _source()
        (
            splitter_col,
            splitter_name,
            batch_request_template,
            batch_request_options,
        ) = config
        asset = source.add_table_asset(name="my_asset", table_name="my_table")
        if batch_request_template:
            kwargs = {"column_name": splitter_col}
            if splitter_name is not None:
                kwargs["name"] = splitter_name
            asset.add_year_and_month_splitter(**kwargs)
        assert len(source.assets) == 1
        asset = list(source.assets.values())[0]
        assert_table_asset(
            asset, "my_asset", "my_table", source, batch_request_template
        )
        assert_batch_request(
            asset.get_batch_request(batch_request_options),
            "my_datasource",
            "my_asset",
            batch_request_options,
        )


def test_construct_table_asset_directly_with_no_splitter():
    with sqlachemy_execution_engine_mock(lambda x: None):
        source = _source()
        asset = postgres_datasource.TableAsset(
            name="my_asset", table_name="my_table", datasource=source
        )
        assert_batch_request(asset.get_batch_request(), "my_datasource", "my_asset", {})


def test_construct_table_asset_directly_with_nameless_splitter():
    with sqlachemy_execution_engine_mock(lambda x: None):
        source = _source()
        splitter = postgres_datasource.ColumnSplitter(
            method_name="splitter_method",
            column_name="col",
            template_params=["a", "b"],
            name="",
        )
        asset = postgres_datasource.TableAsset(
            name="my_asset",
            table_name="my_table",
            datasource=source,
            column_splitter=splitter,
        )
        assert_table_asset(
            asset, "my_asset", "my_table", source, {"a": "<value>", "b": "<value>"}
        )
        batch_request_options = {"a": 1, "b": 2}
        assert_batch_request(
            asset.get_batch_request(batch_request_options),
            "my_datasource",
            "my_asset",
            batch_request_options,
        )


def test_construct_table_asset_directly_with_named_splitter():
    with sqlachemy_execution_engine_mock(lambda x: None):
        source = _source()
        splitter = postgres_datasource.ColumnSplitter(
            method_name="splitter_method",
            column_name="col",
            template_params=["a", "b"],
            name="splitter",
        )
        asset = postgres_datasource.TableAsset(
            name="my_asset",
            table_name="my_table",
            datasource=source,
            column_splitter=splitter,
        )
        assert_table_asset(
            asset,
            "my_asset",
            "my_table",
            source,
            {"splitter": {"a": "<value>", "b": "<value>"}},
        )
        batch_request_options = {"splitter": {"a": 1, "b": 2}}
        assert_batch_request(
            asset.get_batch_request(batch_request_options),
            "my_datasource",
            "my_asset",
            batch_request_options,
        )


def test_datasource_gets_batch_list_no_splitter():
    def validate_batch_spec(spec: SqlAlchemyDatasourceBatchSpec) -> None:
        assert spec == {
            "batch_identifiers": {},
            "data_asset_name": "my_asset",
            "table_name": "my_table",
            "type": "table",
        }

    with sqlachemy_execution_engine_mock(validate_batch_spec):
        source = _source()
        asset = source.add_table_asset(name="my_asset", table_name="my_table")
        source.get_batch_list_from_batch_request(asset.get_batch_request())


def test_datasource_gets_batch_list_splitter_no_values():
    def validate_batch_spec(spec: SqlAlchemyDatasourceBatchSpec) -> None:
        assert spec == {
            "batch_identifiers": {"my_col": {"month": None, "year": None}},
            "data_asset_name": "my_asset",
            "splitter_kwargs": {"column_name": "my_col"},
            "splitter_method": "split_on_year_and_month",
            "table_name": "my_table",
            "type": "table",
        }

    with sqlachemy_execution_engine_mock(validate_batch_spec):
        source = _source()
        asset = source.add_table_asset(name="my_asset", table_name="my_table")
        asset.add_year_and_month_splitter(column_name="my_col")
        source.get_batch_list_from_batch_request(asset.get_batch_request())


def test_datasource_gets_batch_list_splitter_some_values():
    def validate_batch_spec(spec: SqlAlchemyDatasourceBatchSpec) -> None:
        assert spec == {
            "batch_identifiers": {"my_col": {"month": None, "year": 2022}},
            "data_asset_name": "my_asset",
            "splitter_kwargs": {"column_name": "my_col"},
            "splitter_method": "split_on_year_and_month",
            "table_name": "my_table",
            "type": "table",
        }

    with sqlachemy_execution_engine_mock(validate_batch_spec):
        source = _source()
        asset = source.add_table_asset(name="my_asset", table_name="my_table")
        asset.add_year_and_month_splitter(column_name="my_col")
        source.get_batch_list_from_batch_request(
            asset.get_batch_request({"year": 2022})
        )


def test_datasource_gets_batch_list_unnamed_splitter():
    def validate_batch_spec(spec: SqlAlchemyDatasourceBatchSpec) -> None:
        assert spec == {
            "batch_identifiers": {"my_col": {"month": 1, "year": 2022}},
            "data_asset_name": "my_asset",
            "splitter_kwargs": {"column_name": "my_col"},
            "splitter_method": "split_on_year_and_month",
            "table_name": "my_table",
            "type": "table",
        }

    with sqlachemy_execution_engine_mock(validate_batch_spec):
        source = _source()
        asset = source.add_table_asset(name="my_asset", table_name="my_table")
        asset.add_year_and_month_splitter(column_name="my_col")
        source.get_batch_list_from_batch_request(
            asset.get_batch_request({"month": 1, "year": 2022})
        )


def test_datasource_gets_batch_list_named_splitter():
    def validate_batch_spec(spec: SqlAlchemyDatasourceBatchSpec) -> None:
        assert spec == {
            "batch_identifiers": {"my_col": {"month": 1, "year": 2022}},
            "data_asset_name": "my_asset",
            "splitter_kwargs": {"column_name": "my_col"},
            "splitter_method": "split_on_year_and_month",
            "table_name": "my_table",
            "type": "table",
        }

    with sqlachemy_execution_engine_mock(validate_batch_spec):
        source = _source()
        asset = source.add_table_asset(name="my_asset", table_name="my_table")
        asset.add_year_and_month_splitter(column_name="my_col", name="my_splitter")
        source.get_batch_list_from_batch_request(
            asset.get_batch_request({"my_splitter": {"month": 1, "year": 2022}})
        )


def test_datasource_gets_batch_list_using_invalid_splitter_name():
    with sqlachemy_execution_engine_mock(lambda x: None):
        source = _source()
        asset = source.add_table_asset(name="my_asset", table_name="my_table")
        asset.add_year_and_month_splitter(column_name="my_col", name="splitter_name")
        assert_table_asset(
            asset,
            "my_asset",
            "my_table",
            source,
            {"splitter_name": {"month": "<value>", "year": "<value>"}},
        )
        with pytest.raises(postgres_datasource.PostgresDatasourceError):
            # This raises because we've named the splitter but we didn't specify the name in the batch request options
            # The batch_request_options should look like {"splitter": {"month": 1, "year": 2}} but instead looks like
            # {"month": 1, "year": 2}
            source.get_batch_list_from_batch_request(
                asset.get_batch_request({"month": 1, "year": 2})
            )


def test_datasource_gets_nonexistent_asset():
    with sqlachemy_execution_engine_mock(lambda x: None):
        source = _source()
        with pytest.raises(postgres_datasource.PostgresDatasourceError):
            source.get_asset("my_asset")
