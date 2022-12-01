from contextlib import contextmanager
from copy import copy
from typing import Callable, ContextManager

import pytest

from great_expectations.core.batch_spec import SqlAlchemyDatasourceBatchSpec
from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.experimental.datasources.interfaces import (
    BatchRequest,
    BatchRequestOptions,
)
from great_expectations.experimental.datasources.postgres_datasource import (
    _DEFAULT_MONTH_RANGE,
    _DEFAULT_YEAR_RANGE,
    BatchRequestError,
    ColumnSplitter,
    PostgresDatasource,
    TableAsset,
)
from tests.experimental.datasources.conftest import sqlachemy_execution_engine_mock_cls


@contextmanager
def _source(
    validate_batch_spec: Callable[[SqlAlchemyDatasourceBatchSpec], None]
) -> PostgresDatasource:
    execution_eng_cls = sqlachemy_execution_engine_mock_cls(validate_batch_spec)
    original_override = PostgresDatasource.execution_engine_override
    try:
        PostgresDatasource.execution_engine_override = execution_eng_cls
        yield PostgresDatasource(
            name="my_datasource",
            connection_string="postgresql+psycopg2://postgres:@localhost/test_ci",
        )
    finally:
        PostgresDatasource.execution_engine_override = original_override


# We may be able parameterize this fixture so we can instantiate _source in the fixture. This
# would reduce the `with ...` boilerplate in the individual tests.
@pytest.fixture
def create_source() -> ContextManager:
    return _source


@pytest.mark.unit
def test_construct_postgres_datasource(create_source):
    with create_source(lambda _: None) as source:
        assert source.name == "my_datasource"
        assert isinstance(source.execution_engine, SqlAlchemyExecutionEngine)
        assert source.assets == {}


def assert_table_asset(
    asset: TableAsset,
    name: str,
    table_name: str,
    source: PostgresDatasource,
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
    with create_source(lambda _: None) as source:
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
    with create_source(lambda _: None) as source:
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
    with create_source(lambda _: None) as source:
        asset = TableAsset(name="my_asset", table_name="my_table")
        asset._datasource = source
        assert_batch_request(asset.get_batch_request(), "my_datasource", "my_asset", {})


@pytest.mark.unit
def test_construct_table_asset_directly_with_splitter(create_source):
    with create_source(lambda _: None) as source:
        splitter = ColumnSplitter(
            method_name="splitter_method",
            column_name="col",
            param_defaults={"a": [1, 2, 3], "b": range(1, 13)},
        )
        asset = TableAsset(
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
    expected_batch_spec_num = len(list(_DEFAULT_YEAR_RANGE)) * len(
        list(_DEFAULT_MONTH_RANGE)
    )
    assert len(batch_specs) == expected_batch_spec_num
    for year in _DEFAULT_YEAR_RANGE:
        for month in _DEFAULT_MONTH_RANGE:
            spec = {
                "type": "table",
                "data_asset_name": "my_asset",
                "table_name": "my_table",
                "batch_identifiers": {"my_col": {"year": year, "month": month}},
                "splitter_method": "split_on_year_and_month",
                "splitter_kwargs": {"column_name": "my_col"},
            }
            assert spec in batch_specs


def assert_batches_correct_with_year_month_splitter_defaults(batches):
    # We should have 1 batch_spec per (year, month) pair
    expected_batch_spec_num = len(list(_DEFAULT_YEAR_RANGE)) * len(
        list(_DEFAULT_MONTH_RANGE)
    )
    assert len(batches) == expected_batch_spec_num
    metadatas = [batch.metadata for batch in batches]
    for year in _DEFAULT_YEAR_RANGE:
        for month in _DEFAULT_MONTH_RANGE:
            assert {"year": year, "month": month} in metadatas


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
        batches = source.get_batch_list_from_batch_request(empty_batch_request)
        assert_batch_specs_correct_with_year_month_splitter_defaults(batch_specs)
        assert_batches_correct_with_year_month_splitter_defaults(batches)


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
        batches = source.get_batch_list_from_batch_request(batch_request_with_none)
        # We should have 1 batch_spec per (year, month) pair
        assert_batch_specs_correct_with_year_month_splitter_defaults(batch_specs)
        assert_batches_correct_with_year_month_splitter_defaults(batches)


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
        batches = source.get_batch_list_from_batch_request(
            asset.get_batch_request({"year": 2022})
        )
        assert len(batch_specs) == len(_DEFAULT_MONTH_RANGE)
        for month in _DEFAULT_MONTH_RANGE:
            spec = {
                "type": "table",
                "data_asset_name": "my_asset",
                "table_name": "my_table",
                "batch_identifiers": {"my_col": {"year": 2022, "month": month}},
                "splitter_method": "split_on_year_and_month",
                "splitter_kwargs": {"column_name": "my_col"},
            }
            assert spec in batch_specs

        assert len(batches) == len(_DEFAULT_MONTH_RANGE)
        metadatas = [batch.metadata for batch in batches]
        for month in _DEFAULT_MONTH_RANGE:
            expected_metadata = {"month": month, "year": 2022}
            expected_metadata in metadatas


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
        batches = source.get_batch_list_from_batch_request(
            asset.get_batch_request({"month": 1, "year": 2022})
        )
        assert 1 == len(batches)
        assert batches[0].metadata == {"month": 1, "year": 2022}


@pytest.mark.unit
def test_datasource_gets_nonexistent_asset(create_source):
    with create_source(lambda _: None) as source:
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
    with create_source(lambda _: None) as source:
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
                BatchRequestError,
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
    with create_source(lambda _: None) as source:
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
    with create_source(lambda _: None) as source:
        asset = source.add_table_asset(name="my_asset", table_name="my_table")
        asset.add_year_and_month_splitter(column_name="my_col")
        src, ast, op = batch_request_args
        batch_request = BatchRequest(
            datasource_name=src or source.name,
            data_asset_name=ast or asset.name,
            options=op or {},
        )
        with pytest.raises(BatchRequestError):
            asset.validate_batch_request(batch_request)


def test_get_bad_batch_request(create_source):
    with create_source(lambda _: None) as source:
        asset = source.add_table_asset(name="my_asset", table_name="my_table")
        asset.add_year_and_month_splitter(column_name="my_col")
        with pytest.raises(BatchRequestError):
            asset.get_batch_request({"invalid_key": None})


@pytest.mark.unit
@pytest.mark.parametrize(
    "sort_info",
    # Sort info is a list where the first element is the sort keys with an optional prefix and
    # the second element is a range of the values they can take.
    [
        (["year", "month"], [_DEFAULT_YEAR_RANGE, _DEFAULT_MONTH_RANGE]),
        (["+year", "+month"], [_DEFAULT_YEAR_RANGE, _DEFAULT_MONTH_RANGE]),
        (["+year", "month"], [_DEFAULT_YEAR_RANGE, _DEFAULT_MONTH_RANGE]),
        (["year", "+month"], [_DEFAULT_YEAR_RANGE, _DEFAULT_MONTH_RANGE]),
        (["+year", "-month"], [_DEFAULT_YEAR_RANGE, reversed(_DEFAULT_MONTH_RANGE)]),
        (["year", "-month"], [_DEFAULT_YEAR_RANGE, reversed(_DEFAULT_MONTH_RANGE)]),
        (["-year", "month"], [reversed(_DEFAULT_YEAR_RANGE), _DEFAULT_MONTH_RANGE]),
        (["-year", "+month"], [reversed(_DEFAULT_YEAR_RANGE), _DEFAULT_MONTH_RANGE]),
        (
            ["-year", "-month"],
            [reversed(_DEFAULT_YEAR_RANGE), reversed(_DEFAULT_MONTH_RANGE)],
        ),
        (["month", "year"], [_DEFAULT_MONTH_RANGE, _DEFAULT_YEAR_RANGE]),
        (["+month", "+year"], [_DEFAULT_MONTH_RANGE, _DEFAULT_YEAR_RANGE]),
        (["month", "+year"], [_DEFAULT_MONTH_RANGE, _DEFAULT_YEAR_RANGE]),
        (["+month", "year"], [_DEFAULT_MONTH_RANGE, _DEFAULT_YEAR_RANGE]),
        (["-month", "+year"], [reversed(_DEFAULT_MONTH_RANGE), _DEFAULT_YEAR_RANGE]),
        (["-month", "year"], [reversed(_DEFAULT_MONTH_RANGE), _DEFAULT_YEAR_RANGE]),
        (["month", "-year"], [_DEFAULT_MONTH_RANGE, reversed(_DEFAULT_YEAR_RANGE)]),
        (["+month", "-year"], [_DEFAULT_MONTH_RANGE, reversed(_DEFAULT_YEAR_RANGE)]),
        (
            ["-month", "-year"],
            [reversed(_DEFAULT_MONTH_RANGE), reversed(_DEFAULT_YEAR_RANGE)],
        ),
    ],
)
def test_sort_batch_list_by_metadata(sort_info, create_source):
    sort_keys, sort_values = sort_info
    with create_source(lambda _: None) as source:
        asset = source.add_table_asset(name="my_asset", table_name="my_table")
        asset.add_year_and_month_splitter(column_name="my_col").add_sorters(sort_keys)
        batch_request = BatchRequest(
            datasource_name=source.name,
            data_asset_name=asset.name,
            options={},
        )
        batches = source.get_batch_list_from_batch_request(batch_request)
        expected_order = []

        key0 = sort_keys[0].lstrip("+-")
        key1 = sort_keys[1].lstrip("+-")
        for value0 in sort_values[0]:
            for value1 in copy(sort_values[1]):
                # We copy(sort_values[1]) because otherwise we'd exhaust this
                # inner iterator on the first pass of the outer loop.
                expected_order.append({key0: value0, key1: value1})
        assert len(batches) == len(expected_order)
        for i, batch in enumerate(batches):
            assert batch.metadata["year"] == expected_order[i]["year"]
            assert batch.metadata["month"] == expected_order[i]["month"]


def test_sort_batch_list_by_unknown_key(create_source):
    with create_source(lambda _: None) as source:
        asset = source.add_table_asset(name="my_asset", table_name="my_table")
        asset.add_year_and_month_splitter(column_name="my_col").add_sorters(
            ["yr", "month"]
        )
        batch_request = BatchRequest(
            datasource_name=source.name,
            data_asset_name=asset.name,
            options={},
        )
        with pytest.raises(KeyError):
            source.get_batch_list_from_batch_request(batch_request)
