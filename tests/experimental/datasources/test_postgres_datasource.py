from contextlib import contextmanager
from typing import Callable, ContextManager

import pytest

from great_expectations.core.batch_spec import SqlAlchemyDatasourceBatchSpec
from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.experimental.datasources.interfaces import (
    BatchRequest,
    BatchRequestOptions,
)
from great_expectations.experimental.datasources.postgres_datasource import (
    BatchRequestError,
    PostgresDatasource,
    SqlYearMonthSplitter,
    TableAsset,
)
from tests.experimental.datasources.conftest import (
    DEFAULT_MAX_DT,
    DEFAULT_MIN_DT,
    sqlachemy_execution_engine_mock_cls,
)

# We set a default time range that we use for testing. We grab the years from the
# mocks we use in tests. For months, we use every month.
_DEFAULT_TEST_YEARS = list(range(DEFAULT_MIN_DT.year, DEFAULT_MAX_DT.year + 1))
_DEFAULT_TEST_MONTHS = list(range(1, 13))


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
        asset = TableAsset(
            name="my_asset",
            table_name="my_table",
            column_splitter=SqlYearMonthSplitter(column_name="col"),
        )
        # TODO: asset custom init
        asset._datasource = source
        assert_table_asset(
            asset,
            "my_asset",
            "my_table",
            source,
            {"year": None, "month": None},
        )
        batch_request_options = {"year": 2022, "month": 10}
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
    expected_batch_spec_num = len(_DEFAULT_TEST_YEARS) * len(_DEFAULT_TEST_MONTHS)
    assert len(batch_specs) == expected_batch_spec_num
    for year in _DEFAULT_TEST_YEARS:
        for month in _DEFAULT_TEST_MONTHS:
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
    expected_batch_spec_num = len(_DEFAULT_TEST_YEARS) * len(_DEFAULT_TEST_MONTHS)
    assert len(batches) == expected_batch_spec_num
    metadatas = [batch.metadata for batch in batches]
    for year in _DEFAULT_TEST_YEARS:
        for month in _DEFAULT_TEST_MONTHS:
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
        assert len(batch_specs) == len(_DEFAULT_TEST_MONTHS)
        for month in _DEFAULT_TEST_MONTHS:
            spec = {
                "type": "table",
                "data_asset_name": "my_asset",
                "table_name": "my_table",
                "batch_identifiers": {"my_col": {"year": 2022, "month": month}},
                "splitter_method": "split_on_year_and_month",
                "splitter_kwargs": {"column_name": "my_col"},
            }
            assert spec in batch_specs

        assert len(batches) == len(_DEFAULT_TEST_MONTHS)
        metadatas = [batch.metadata for batch in batches]
        for month in _DEFAULT_TEST_MONTHS:
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
def test_get_batch_list_from_batch_request_with_good_batch_request(
    create_source, batch_request_options
):
    with create_source(lambda _: None) as source:
        asset = source.add_table_asset(name="my_asset", table_name="my_table")
        asset.add_year_and_month_splitter(column_name="my_col")
        batch_request = BatchRequest(
            datasource_name=source.name,
            data_asset_name=asset.name,
            options=batch_request_options,
        )
        # No exception should get thrown
        asset.get_batch_list_from_batch_request(batch_request)


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
def test_get_batch_list_from_batch_request_with_malformed_batch_request(
    create_source, batch_request_args
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
        with pytest.raises(BatchRequestError):
            asset.get_batch_list_from_batch_request(batch_request)


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
        (["year", "month"], [_DEFAULT_TEST_YEARS, _DEFAULT_TEST_MONTHS]),
        (["+year", "+month"], [_DEFAULT_TEST_YEARS, _DEFAULT_TEST_MONTHS]),
        (["+year", "month"], [_DEFAULT_TEST_YEARS, _DEFAULT_TEST_MONTHS]),
        (["year", "+month"], [_DEFAULT_TEST_YEARS, _DEFAULT_TEST_MONTHS]),
        (
            ["+year", "-month"],
            [_DEFAULT_TEST_YEARS, list(reversed(_DEFAULT_TEST_MONTHS))],
        ),
        (
            ["year", "-month"],
            [_DEFAULT_TEST_YEARS, list(reversed(_DEFAULT_TEST_MONTHS))],
        ),
        (
            ["-year", "month"],
            [list(reversed(_DEFAULT_TEST_YEARS)), _DEFAULT_TEST_MONTHS],
        ),
        (
            ["-year", "+month"],
            [list(reversed(_DEFAULT_TEST_YEARS)), _DEFAULT_TEST_MONTHS],
        ),
        (
            ["-year", "-month"],
            [list(reversed(_DEFAULT_TEST_YEARS)), list(reversed(_DEFAULT_TEST_MONTHS))],
        ),
        (["month", "year"], [_DEFAULT_TEST_MONTHS, _DEFAULT_TEST_YEARS]),
        (["+month", "+year"], [_DEFAULT_TEST_MONTHS, _DEFAULT_TEST_YEARS]),
        (["month", "+year"], [_DEFAULT_TEST_MONTHS, _DEFAULT_TEST_YEARS]),
        (["+month", "year"], [_DEFAULT_TEST_MONTHS, _DEFAULT_TEST_YEARS]),
        (
            ["-month", "+year"],
            [list(reversed(_DEFAULT_TEST_MONTHS)), _DEFAULT_TEST_YEARS],
        ),
        (
            ["-month", "year"],
            [list(reversed(_DEFAULT_TEST_MONTHS)), _DEFAULT_TEST_YEARS],
        ),
        (
            ["month", "-year"],
            [_DEFAULT_TEST_MONTHS, list(reversed(_DEFAULT_TEST_YEARS))],
        ),
        (
            ["+month", "-year"],
            [_DEFAULT_TEST_MONTHS, list(reversed(_DEFAULT_TEST_YEARS))],
        ),
        (
            ["-month", "-year"],
            [list(reversed(_DEFAULT_TEST_MONTHS)), list(reversed(_DEFAULT_TEST_YEARS))],
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
            for value1 in sort_values[1]:
                expected_order.append({key0: value0, key1: value1})
        assert len(batches) == len(expected_order)
        for i, batch in enumerate(batches):
            assert batch.metadata["year"] == expected_order[i]["year"]
            assert batch.metadata["month"] == expected_order[i]["month"]


@pytest.mark.unit
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


@pytest.mark.unit
def test_data_source_json_has_properties(create_source):
    with create_source(lambda _: None) as source:
        assert isinstance(TableAsset.order_by, property), (
            "This test assumes TableAsset.order_by is a property. If it is not we "
            "should update this test",
        )
        asset = source.add_table_asset(name="my_asset", table_name="my_table")
        asset.add_year_and_month_splitter(column_name="my_col").add_sorters(
            ["year", "month"]
        )
        source_json = source.json()
        assert '"order_by": ' in source_json


@pytest.mark.unit
def test_data_source_str_has_properties(create_source):
    with create_source(lambda _: None) as source:
        assert isinstance(TableAsset.order_by, property), (
            "This test assumes TableAsset.order_by is a property. If it is not we "
            "should update this test",
        )
        asset = source.add_table_asset(name="my_asset", table_name="my_table")
        asset.add_year_and_month_splitter(column_name="my_col").add_sorters(
            ["year", "month"]
        )
        source_str = source.__str__()
        assert "order_by:" in source_str


@pytest.mark.unit
def test_datasource_dict_has_properties(create_source):
    with create_source(lambda _: None) as source:
        assert isinstance(TableAsset.order_by, property), (
            "This test assumes TableAsset.order_by is a property. If it is not we "
            "should update this test",
        )
        asset = source.add_table_asset(name="my_asset", table_name="my_table")
        asset.add_year_and_month_splitter(column_name="my_col").add_sorters(
            ["year", "month"]
        )
        source_dict = source.dict()
        assert isinstance(source_dict["assets"]["my_asset"]["order_by"], list)
