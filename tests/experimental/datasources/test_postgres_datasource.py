from contextlib import contextmanager
from typing import Callable, ContextManager

import pytest

import great_expectations.experimental.datasources.postgres_datasource as postgres_datasource
from great_expectations.core.batch_spec import SqlAlchemyDatasourceBatchSpec
from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.experimental.datasources.interfaces import (
    BatchRequest,
    BatchRequestOptions,
)
from tests.experimental.datasources.conftest import sqlachemy_execution_engine_mock_cls


@contextmanager
def _source(
    validate_batch_spec: Callable[[SqlAlchemyDatasourceBatchSpec], None]
) -> postgres_datasource.PostgresDatasource:
    execution_eng_cls = sqlachemy_execution_engine_mock_cls(validate_batch_spec)
    original_override = postgres_datasource.PostgresDatasource.execution_engine_override
    try:
        postgres_datasource.PostgresDatasource.execution_engine_override = (
            execution_eng_cls
        )
        yield postgres_datasource.PostgresDatasource(
            name="my_datasource",
            connection_string="postgresql+psycopg2://postgres:@localhost/test_ci",
        )
    finally:
        postgres_datasource.PostgresDatasource.execution_engine_override = (
            original_override
        )


# We may be able parameterize this fixture so we can instantiate _source in the fixture. This
# would reduce the `with ...` boilerplate in the individual tests.
@pytest.fixture
def create_source() -> ContextManager:
    return _source


@pytest.mark.unit
def test_construct_postgres_datasource(create_source):
    with create_source(lambda: None) as source:
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
    assert asset.batch_request_options_template() == batch_request_template


def assert_batch_request(
    batch_request, source_name: str, asset_name: str, options: BatchRequestOptions
):
    assert batch_request.datasource_name == source_name
    assert batch_request.data_asset_name == asset_name
    assert batch_request.options == options


@pytest.mark.unit
def test_add_table_asset_with_splitter(create_source):
    with create_source(lambda: None) as source:
        asset = source.add_table_asset(name="my_asset", table_name="my_table")
        asset.add_year_and_month_splitter("my_column")
        assert len(source.assets) == 1
        assert asset == list(source.assets.values())[0]
        assert_table_asset(
            asset=asset,
            name="my_asset",
            table_name="my_table",
            source=source,
            batch_request_template={"year": None, "month": None},
        )
        assert_batch_request(
            batch_request=asset.get_batch_request({"year": 2021, "month": 10}),
            source_name="my_datasource",
            asset_name="my_asset",
            options={"year": 2021, "month": 10},
        )


@pytest.mark.unit
def test_add_table_asset_with_no_splitter(create_source):
    with create_source(lambda: None) as source:
        asset = source.add_table_asset(name="my_asset", table_name="my_table")
        assert len(source.assets) == 1
        assert asset == list(source.assets.values())[0]
        assert_table_asset(
            asset=asset,
            name="my_asset",
            table_name="my_table",
            source=source,
            batch_request_template={},
        )
        assert_batch_request(
            batch_request=asset.get_batch_request(),
            source_name="my_datasource",
            asset_name="my_asset",
            options={},
        )
        assert_batch_request(
            batch_request=asset.get_batch_request({}),
            source_name="my_datasource",
            asset_name="my_asset",
            options={},
        )


@pytest.mark.unit
def test_construct_table_asset_directly_with_no_splitter(create_source):
    with create_source(lambda: None) as source:
        asset = postgres_datasource.TableAsset(name="my_asset", table_name="my_table")
        asset._datasource = source
        assert_batch_request(asset.get_batch_request(), "my_datasource", "my_asset", {})


@pytest.mark.unit
def test_construct_table_asset_directly_with_splitter(create_source):
    with create_source(lambda: None) as source:
        splitter = postgres_datasource.ColumnSplitter(
            method_name="splitter_method",
            column_name="col",
            param_defaults={"a": [1, 2, 3], "b": range(1, 13)},
        )
        asset = postgres_datasource.TableAsset(
            name="my_asset",
            table_name="my_table",
            column_splitter=splitter,
        )
        # TODO: asset custom init
        asset._datasource = source
        assert_table_asset(
            asset,
            "my_asset",
            "my_table",
            source,
            {"a": None, "b": None},
        )
        batch_request_options = {"a": 1, "b": 2}
        assert_batch_request(
            asset.get_batch_request(batch_request_options),
            "my_datasource",
            "my_asset",
            batch_request_options,
        )


@pytest.mark.unit
def test_datasource_gets_batch_list_no_splitter(create_source):
    def validate_batch_spec(spec: SqlAlchemyDatasourceBatchSpec) -> None:
        assert spec == {
            "batch_identifiers": {},
            "data_asset_name": "my_asset",
            "table_name": "my_table",
            "type": "table",
        }

    with create_source(validate_batch_spec) as source:
        asset = source.add_table_asset(name="my_asset", table_name="my_table")
        source.get_batch_list_from_batch_request(asset.get_batch_request())


def assert_batch_specs_correct_with_year_month_splitter_defaults(batch_specs):
    # We should have 1 batch_spec per (year, month) pair
    expected_batch_spec_num = len(list(postgres_datasource._DEFAULT_YEAR_RANGE)) * len(
        list(postgres_datasource._DEFAULT_MONTH_RANGE)
    )
    assert len(batch_specs) == expected_batch_spec_num
    for year in postgres_datasource._DEFAULT_YEAR_RANGE:
        for month in postgres_datasource._DEFAULT_MONTH_RANGE:
            spec = {
                "type": "table",
                "data_asset_name": "my_asset",
                "table_name": "my_table",
                "batch_identifiers": {"my_col": {"year": year, "month": month}},
                "splitter_method": "split_on_year_and_month",
                "splitter_kwargs": {"column_name": "my_col"},
            }
            assert spec in batch_specs


@pytest.mark.unit
def test_datasource_gets_batch_list_splitter_with_unspecified_batch_request_options(
    create_source,
):
    batch_specs = []

    def collect_batch_spec(spec: SqlAlchemyDatasourceBatchSpec) -> None:
        batch_specs.append(spec)

    with create_source(collect_batch_spec) as source:
        asset = source.add_table_asset(name="my_asset", table_name="my_table")
        asset.add_year_and_month_splitter(column_name="my_col")
        empty_batch_request = asset.get_batch_request()
        assert empty_batch_request.options == {}
        source.get_batch_list_from_batch_request(empty_batch_request)
        assert_batch_specs_correct_with_year_month_splitter_defaults(batch_specs)


@pytest.mark.unit
def test_datasource_gets_batch_list_splitter_with_batch_request_options_set_to_none(
    create_source,
):
    batch_specs = []

    def collect_batch_spec(spec: SqlAlchemyDatasourceBatchSpec) -> None:
        batch_specs.append(spec)

    with create_source(collect_batch_spec) as source:
        asset = source.add_table_asset(name="my_asset", table_name="my_table")
        asset.add_year_and_month_splitter(column_name="my_col")
        batch_request_with_none = asset.get_batch_request(
            asset.batch_request_options_template()
        )
        assert batch_request_with_none.options == {"year": None, "month": None}
        source.get_batch_list_from_batch_request(batch_request_with_none)
        # We should have 1 batch_spec per (year, month) pair
        assert_batch_specs_correct_with_year_month_splitter_defaults(batch_specs)


@pytest.mark.unit
def test_datasource_gets_batch_list_splitter_with_partially_specified_batch_request_options(
    create_source,
):
    batch_specs = []

    def collect_batch_spec(spec: SqlAlchemyDatasourceBatchSpec) -> None:
        batch_specs.append(spec)

    with create_source(collect_batch_spec) as source:
        asset = source.add_table_asset(name="my_asset", table_name="my_table")
        asset.add_year_and_month_splitter(column_name="my_col")
        source.get_batch_list_from_batch_request(
            asset.get_batch_request({"year": 2022})
        )
        assert len(batch_specs) == 12
        for month in postgres_datasource._DEFAULT_MONTH_RANGE:
            spec = {
                "type": "table",
                "data_asset_name": "my_asset",
                "table_name": "my_table",
                "batch_identifiers": {"my_col": {"year": 2022, "month": month}},
                "splitter_method": "split_on_year_and_month",
                "splitter_kwargs": {"column_name": "my_col"},
            }
            assert spec in batch_specs


@pytest.mark.unit
def test_datasource_gets_batch_list_with_fully_specified_batch_request_options(
    create_source,
):
    def validate_batch_spec(spec: SqlAlchemyDatasourceBatchSpec) -> None:
        assert spec == {
            "batch_identifiers": {"my_col": {"month": 1, "year": 2022}},
            "data_asset_name": "my_asset",
            "splitter_kwargs": {"column_name": "my_col"},
            "splitter_method": "split_on_year_and_month",
            "table_name": "my_table",
            "type": "table",
        }

    with create_source(validate_batch_spec) as source:
        asset = source.add_table_asset(name="my_asset", table_name="my_table")
        asset.add_year_and_month_splitter(column_name="my_col")
        source.get_batch_list_from_batch_request(
            asset.get_batch_request({"month": 1, "year": 2022})
        )


@pytest.mark.unit
def test_datasource_gets_nonexistent_asset(create_source):
    with create_source(lambda: None) as source:
        with pytest.raises(LookupError):
            source.get_asset("my_asset")


@pytest.mark.unit
@pytest.mark.parametrize(
    "batch_request_args",
    [
        ("bad", None, None),
        (None, "bad", None),
        (None, None, {"bad": None}),
        ("bad", "bad", None),
    ],
)
def test_bad_batch_request_passed_into_get_batch_list_from_batch_request(
    create_source,
    batch_request_args,
):
    with create_source(lambda: None) as source:
        asset = source.add_table_asset(name="my_asset", table_name="my_table")
        asset.add_year_and_month_splitter(column_name="my_col")

        src, ast, op = batch_request_args
        batch_request = BatchRequest(
            datasource_name=src or source.name,
            data_asset_name=ast or asset.name,
            options=op or {},
        )
        with pytest.raises(
            (
                postgres_datasource.BatchRequestError,
                LookupError,
            )
        ):
            source.get_batch_list_from_batch_request(batch_request)


@pytest.mark.unit
@pytest.mark.parametrize(
    "batch_request_options",
    [{}, {"year": 2021}, {"year": 2021, "month": 10}, {"year": None, "month": 10}],
)
def test_validate_good_batch_request(create_source, batch_request_options):
    with create_source(lambda: None) as source:
        asset = source.add_table_asset(name="my_asset", table_name="my_table")
        asset.add_year_and_month_splitter(column_name="my_col")
        batch_request = BatchRequest(
            datasource_name=source.name,
            data_asset_name=asset.name,
            options=batch_request_options,
        )
        # No exception should get thrown
        asset.validate_batch_request(batch_request)


@pytest.mark.unit
@pytest.mark.parametrize(
    "batch_request_args",
    [
        ("bad", None, None),
        (None, "bad", None),
        (None, None, {"bad": None}),
        ("bad", "bad", None),
    ],
)
def test_validate_malformed_batch_request(create_source, batch_request_args):
    with create_source(lambda: None) as source:
        asset = source.add_table_asset(name="my_asset", table_name="my_table")
        asset.add_year_and_month_splitter(column_name="my_col")
        src, ast, op = batch_request_args
        batch_request = BatchRequest(
            datasource_name=src or source.name,
            data_asset_name=ast or asset.name,
            options=op or {},
        )
        with pytest.raises(postgres_datasource.BatchRequestError):
            asset.validate_batch_request(batch_request)


def test_get_bad_batch_request(create_source):
    with create_source(lambda: None) as source:
        asset = source.add_table_asset(name="my_asset", table_name="my_table")
        asset.add_year_and_month_splitter(column_name="my_col")
        with pytest.raises(postgres_datasource.BatchRequestError):
            asset.get_batch_request({"invalid_key": None})
