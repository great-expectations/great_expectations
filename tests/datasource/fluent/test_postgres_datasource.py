from __future__ import annotations

import copy
import logging
import pathlib
from contextlib import contextmanager
from pprint import pformat as pf
from pprint import pprint
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ContextManager,
    Dict,
    Generator,
    List,
    Optional,
    Tuple,
)

import pytest
from pydantic import ValidationError

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch_spec import SqlAlchemyDatasourceBatchSpec
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.datasource.fluent.batch_request import (
    BatchRequest,
    BatchRequestOptions,
)
from great_expectations.datasource.fluent.interfaces import (
    Sorter,
    TestConnectionError,
)
from great_expectations.datasource.fluent.postgres_datasource import (
    PostgresDatasource,
)
from great_expectations.datasource.fluent.sql_datasource import (
    Splitter,
    SplitterYearAndMonth,
    TableAsset,
)
from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from tests.datasource.fluent.conftest import sqlachemy_execution_engine_mock_cls
from tests.sqlalchemy_test_doubles import Dialect, MockSaEngine, MockSaInspector

if TYPE_CHECKING:
    from typing_extensions import TypeAlias

    from great_expectations.data_context import AbstractDataContext, FileDataContext
    from great_expectations.datasource.fluent.interfaces import (
        BatchMetadata,
        BatchSlice,
    )

# We set a default time range that we use for testing.
_DEFAULT_TEST_YEARS = list(range(2021, 2022 + 1))
_DEFAULT_TEST_MONTHS = list(range(1, 13))

LOGGER = logging.getLogger(__name__)


@contextmanager
def _source(
    validate_batch_spec: Callable[[SqlAlchemyDatasourceBatchSpec], None],
    dialect: str,
    connection_string: str = "postgresql+psycopg2://postgres:@localhost/test_ci",
    data_context: Optional[AbstractDataContext] = None,
    splitter_query_response: Optional[List[Dict[str, Any]]] = None,
    create_temp_table: bool = True,
) -> Generator[PostgresDatasource, None, None]:
    splitter_response = splitter_query_response or (
        [
            {"year": year, "month": month}
            for year in _DEFAULT_TEST_YEARS
            for month in _DEFAULT_TEST_MONTHS
        ]
    )

    execution_eng_cls = sqlachemy_execution_engine_mock_cls(
        validate_batch_spec=validate_batch_spec,
        dialect=dialect,
        splitter_query_response=splitter_response,
    )
    original_override = PostgresDatasource.execution_engine_override  # type: ignore[misc]
    try:
        PostgresDatasource.execution_engine_override = execution_eng_cls  # type: ignore[misc]
        postgres_datasource = PostgresDatasource(
            name="my_datasource",
            connection_string=connection_string,  # type: ignore[arg-type] # coerced
            create_temp_table=create_temp_table,
        )
        if data_context:
            postgres_datasource._data_context = data_context
        yield postgres_datasource
    finally:
        PostgresDatasource.execution_engine_override = original_override  # type: ignore[misc]


# We may be able to parameterize this fixture so we can instantiate _source in the fixture.
# This would reduce the `with ...` boilerplate in the individual tests.
@pytest.fixture
def create_source() -> ContextManager:
    return _source  # type: ignore[return-value]


CreateSourceFixture: TypeAlias = Callable[..., ContextManager[PostgresDatasource]]


@pytest.fixture
def mock_test_connection(monkeypatch: pytest.MonkeyPatch):
    """Patches the test_connection method of the PostgresDatasource class to return True."""

    def _mock_test_connection(self: PostgresDatasource) -> bool:
        LOGGER.warning(
            f"Mocked {self.__class__.__name__}.test_connection() called and returning True"
        )
        return True

    monkeypatch.setattr(PostgresDatasource, "test_connection", _mock_test_connection)


@pytest.mark.unit
def test_construct_postgres_datasource(create_source: CreateSourceFixture):
    with create_source(
        validate_batch_spec=lambda _: None, dialect="postgresql"
    ) as source:
        assert source.name == "my_datasource"
        assert source.execution_engine_type is SqlAlchemyExecutionEngine
        assert source.assets == []


def assert_table_asset(
    asset: TableAsset,
    name: str,
    table_name: str,
    source: PostgresDatasource,
    batch_request_options: tuple[str, ...],
):
    assert asset.name == name
    assert asset.table_name == table_name
    assert asset.datasource == source
    assert asset.batch_request_options == batch_request_options


def assert_batch_request(
    batch_request, source_name: str, asset_name: str, options: BatchRequestOptions
):
    assert batch_request.datasource_name == source_name
    assert batch_request.data_asset_name == asset_name
    assert batch_request.options == options


@pytest.mark.unit
def test_add_table_asset_with_splitter(mocker, create_source: CreateSourceFixture):
    with create_source(
        validate_batch_spec=lambda _: None, dialect="postgresql"
    ) as source:
        mocker.patch("sqlalchemy.create_engine")
        mocker.patch("sqlalchemy.engine")
        inspect = mocker.patch("sqlalchemy.inspect")
        inspect.return_value = MockSaInspector()
        get_column_names = mocker.patch(
            "tests.sqlalchemy_test_doubles.MockSaInspector.get_columns"
        )
        get_column_names.return_value = [{"name": "my_col"}]
        has_table = mocker.patch(
            "tests.sqlalchemy_test_doubles.MockSaInspector.has_table"
        )
        has_table.return_value = True

        asset = source.add_table_asset(name="my_asset", table_name="my_table")
        asset.add_splitter_year_and_month(column_name="my_col")
        assert len(source.assets) == 1
        assert asset == source.assets[0]
        assert_table_asset(
            asset=asset,
            name="my_asset",
            table_name="my_table",
            source=source,
            batch_request_options=("year", "month"),
        )
        assert_batch_request(
            batch_request=asset.build_batch_request({"year": 2021, "month": 10}),
            source_name="my_datasource",
            asset_name="my_asset",
            options={"year": 2021, "month": 10},
        )


@pytest.mark.unit
def test_add_table_asset_with_no_splitter(mocker, create_source: CreateSourceFixture):
    with create_source(
        validate_batch_spec=lambda _: None, dialect="postgresql"
    ) as source:
        mocker.patch("sqlalchemy.create_engine")
        mocker.patch("sqlalchemy.engine")
        mocker.patch("sqlalchemy.inspect")

        asset = source.add_table_asset(name="my_asset", table_name="my_table")
        assert len(source.assets) == 1
        assert asset == source.assets[0]
        assert_table_asset(
            asset=asset,
            name="my_asset",
            table_name="my_table",
            source=source,
            batch_request_options=tuple(),
        )
        assert_batch_request(
            batch_request=asset.build_batch_request(),
            source_name="my_datasource",
            asset_name="my_asset",
            options={},
        )
        assert_batch_request(
            batch_request=asset.build_batch_request({}),
            source_name="my_datasource",
            asset_name="my_asset",
            options={},
        )


@pytest.mark.unit
def test_construct_table_asset_directly_with_no_splitter(create_source):
    with create_source(
        validate_batch_spec=lambda _: None, dialect="postgresql"
    ) as source:
        asset = TableAsset(name="my_asset", table_name="my_table")
        asset._datasource = source
        assert_batch_request(
            asset.build_batch_request(), "my_datasource", "my_asset", {}
        )


def create_and_add_table_asset_without_testing_connection(
    source: PostgresDatasource,
    name: str,
    table_name: str,
    splitter: Optional[Splitter] = None,
) -> Tuple[PostgresDatasource, TableAsset]:
    table_asset = TableAsset(
        name=name,
        table_name=table_name,
        splitter=splitter,
    )
    # TODO: asset custom init
    table_asset._datasource = source
    source.assets.append(table_asset)
    return source, table_asset


def year_month_splitter(column_name: str) -> SplitterYearAndMonth:
    return SplitterYearAndMonth(
        column_name=column_name,
        method_name="split_on_year_and_month",
    )


@pytest.mark.unit
def test_construct_table_asset_directly_with_splitter(create_source):
    with create_source(
        validate_batch_spec=lambda _: None, dialect="postgresql"
    ) as source:
        (
            source,  # noqa: PLW2901
            asset,
        ) = create_and_add_table_asset_without_testing_connection(
            source=source,
            name="my_asset",
            table_name="my_table",
            splitter=year_month_splitter(column_name="col"),
        )
        assert_table_asset(
            asset,
            "my_asset",
            "my_table",
            source,
            ("year", "month"),
        )
        batch_request_options = {"year": 2022, "month": 10}
        assert_batch_request(
            asset.build_batch_request(batch_request_options),
            "my_datasource",
            "my_asset",
            batch_request_options,
        )


@pytest.mark.unit
def test_datasource_gets_batch_list_no_splitter(empty_data_context, create_source):
    def validate_batch_spec(spec: SqlAlchemyDatasourceBatchSpec) -> None:
        assert spec == {
            "batch_identifiers": {},
            "data_asset_name": "my_asset",
            "table_name": "my_table",
            "schema_name": None,
            "type": "table",
        }

    with create_source(
        validate_batch_spec=validate_batch_spec,
        dialect="postgresql",
        data_context=empty_data_context,
    ) as source:
        (
            source,  # noqa: PLW2901
            asset,
        ) = create_and_add_table_asset_without_testing_connection(
            source=source, name="my_asset", table_name="my_table"
        )
        source.get_batch_list_from_batch_request(asset.build_batch_request())


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
                "schema_name": None,
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
    empty_data_context,
    create_source: CreateSourceFixture,
):
    batch_specs = []

    def collect_batch_spec(spec: SqlAlchemyDatasourceBatchSpec) -> None:
        batch_specs.append(spec)

    with create_source(
        validate_batch_spec=collect_batch_spec,
        dialect="postgresql",
        data_context=empty_data_context,
    ) as source:
        (
            source,  # noqa: PLW2901
            asset,
        ) = create_and_add_table_asset_without_testing_connection(
            source=source, name="my_asset", table_name="my_table"
        )
        asset.splitter = year_month_splitter(column_name="my_col")
        empty_batch_request = asset.build_batch_request()
        assert empty_batch_request.options == {}
        batches = source.get_batch_list_from_batch_request(empty_batch_request)
        assert_batch_specs_correct_with_year_month_splitter_defaults(batch_specs)
        assert_batches_correct_with_year_month_splitter_defaults(batches)


@pytest.mark.unit
def test_datasource_gets_batch_list_splitter_with_batch_request_options_set_to_none(
    empty_data_context,
    create_source: CreateSourceFixture,
):
    batch_specs = []

    def collect_batch_spec(spec: SqlAlchemyDatasourceBatchSpec) -> None:
        batch_specs.append(spec)

    with create_source(
        validate_batch_spec=collect_batch_spec,
        dialect="postgresql",
        data_context=empty_data_context,
    ) as source:
        (
            source,  # noqa: PLW2901
            asset,
        ) = create_and_add_table_asset_without_testing_connection(
            source=source, name="my_asset", table_name="my_table"
        )
        asset.splitter = year_month_splitter(column_name="my_col")
        assert asset.batch_request_options == ("year", "month")
        batch_request_with_none = asset.build_batch_request(
            {"year": None, "month": None}
        )
        assert batch_request_with_none.options == {"year": None, "month": None}
        batches = source.get_batch_list_from_batch_request(batch_request_with_none)
        # We should have 1 batch_spec per (year, month) pair
        assert_batch_specs_correct_with_year_month_splitter_defaults(batch_specs)
        assert_batches_correct_with_year_month_splitter_defaults(batches)


@pytest.mark.unit
def test_datasource_gets_batch_list_splitter_with_partially_specified_batch_request_options(
    empty_data_context,
    create_source: CreateSourceFixture,
):
    batch_specs = []
    year = 2022

    def collect_batch_spec(spec: SqlAlchemyDatasourceBatchSpec) -> None:
        batch_specs.append(spec)

    with create_source(
        validate_batch_spec=collect_batch_spec,
        dialect="postgresql",
        data_context=empty_data_context,
        splitter_query_response=[
            {"year": year, "month": month} for month in list(range(1, 13))
        ],
    ) as source:
        (
            source,  # noqa: PLW2901
            asset,
        ) = create_and_add_table_asset_without_testing_connection(
            source=source, name="my_asset", table_name="my_table"
        )
        asset.splitter = year_month_splitter(column_name="my_col")
        batches = source.get_batch_list_from_batch_request(
            asset.build_batch_request({"year": year})
        )
        assert len(batch_specs) == len(_DEFAULT_TEST_MONTHS)
        for month in _DEFAULT_TEST_MONTHS:
            spec = {
                "type": "table",
                "data_asset_name": "my_asset",
                "table_name": "my_table",
                "schema_name": None,
                "batch_identifiers": {"my_col": {"year": year, "month": month}},
                "splitter_method": "split_on_year_and_month",
                "splitter_kwargs": {"column_name": "my_col"},
            }
            assert spec in batch_specs

        assert len(batches) == len(_DEFAULT_TEST_MONTHS)
        metadatas = [batch.metadata for batch in batches]
        for month in _DEFAULT_TEST_MONTHS:
            expected_metadata = {"month": month, "year": year}
            assert expected_metadata in metadatas


@pytest.mark.unit
def test_datasource_gets_batch_list_with_fully_specified_batch_request_options(
    empty_data_context,
    create_source: CreateSourceFixture,
):
    year = 2022
    month = 1

    def validate_batch_spec(spec: SqlAlchemyDatasourceBatchSpec) -> None:
        assert spec == {
            "batch_identifiers": {"my_col": {"month": 1, "year": 2022}},
            "data_asset_name": "my_asset",
            "splitter_kwargs": {"column_name": "my_col"},
            "splitter_method": "split_on_year_and_month",
            "table_name": "my_table",
            "schema_name": None,
            "type": "table",
        }

    with create_source(
        validate_batch_spec=validate_batch_spec,
        dialect="postgresql",
        data_context=empty_data_context,
        splitter_query_response=[{"month": month, "year": year}],
    ) as source:
        (
            source,  # noqa: PLW2901
            asset,
        ) = create_and_add_table_asset_without_testing_connection(
            source=source, name="my_asset", table_name="my_table"
        )
        asset.splitter = year_month_splitter(column_name="my_col")
        batches = source.get_batch_list_from_batch_request(
            asset.build_batch_request({"month": month, "year": year})
        )
        assert 1 == len(batches)
        assert batches[0].metadata == {"month": month, "year": year}


@pytest.mark.unit
def test_datasource_gets_nonexistent_asset(create_source: CreateSourceFixture):
    with create_source(
        validate_batch_spec=lambda _: None, dialect="postgresql"
    ) as source:
        with pytest.raises(LookupError):
            source.get_asset("my_asset")


@pytest.mark.unit
@pytest.mark.parametrize(
    [
        "add_splitter",
        "add_splitter_kwargs",
        "batch_request_args",
    ],
    [
        # These bad parameters do not occur in the batch request params
        ("add_splitter_year", {"column_name": "my_col"}, ("bad", None, None)),
        ("add_splitter_year", {"column_name": "my_col"}, (None, "bad", None)),
        ("add_splitter_year", {"column_name": "my_col"}, ("bad", "bad", None)),
        # These bad parameters are request option parameters which depend on the splitter.
        ("add_splitter_year", {"column_name": "my_col"}, (None, None, {"bad": None})),
        (
            "add_splitter_year_and_month",
            {"column_name": "my_col"},
            (None, None, {"bad": None}),
        ),
        (
            "add_splitter_year_and_month_and_day",
            {"column_name": "my_col"},
            (None, None, {"bad": None}),
        ),
        (
            "add_splitter_datetime_part",
            {"column_name": "my_col", "datetime_parts": ["year"]},
            (None, None, {"bad": None}),
        ),
        (
            "add_splitter_column_value",
            {"column_name": "my_col"},
            (None, None, {"bad": None}),
        ),
        (
            "add_splitter_divided_integer",
            {"column_name": "my_col", "divisor": 3},
            (None, None, {"bad": None}),
        ),
    ],
)
def test_bad_batch_request_passed_into_get_batch_list_from_batch_request(
    create_source: CreateSourceFixture,
    add_splitter,
    add_splitter_kwargs,
    batch_request_args,
):
    with create_source(
        validate_batch_spec=lambda _: None, dialect="postgresql"
    ) as source:
        # We use a QueryAsset so no database connection checks are done.
        # We are mocking the database so these would fail without monkeypatching the
        # TableAsset
        asset = source.add_query_asset(
            name="query_asset", query="SELECT * FROM my_table"
        )
        getattr(asset, add_splitter)(**add_splitter_kwargs)

        src, ast, op = batch_request_args
        batch_request = BatchRequest(
            datasource_name=src or source.name,
            data_asset_name=ast or asset.name,
            options=op or {},
        )
        with pytest.raises(
            (
                ge_exceptions.InvalidBatchRequestError,
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
    empty_data_context,
    create_source: CreateSourceFixture,
    batch_request_options,
):
    with create_source(
        validate_batch_spec=lambda _: None,
        dialect="postgresql",
        data_context=empty_data_context,
    ) as source:
        (
            source,  # noqa: PLW2901
            asset,
        ) = create_and_add_table_asset_without_testing_connection(
            source=source, name="my_asset", table_name="my_table"
        )
        asset.splitter = year_month_splitter(column_name="my_col")
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
    create_source: CreateSourceFixture, batch_request_args
):
    with create_source(
        validate_batch_spec=lambda _: None, dialect="postgresql"
    ) as source:
        (
            source,  # noqa: PLW2901
            asset,
        ) = create_and_add_table_asset_without_testing_connection(
            source=source, name="my_asset", table_name="my_table"
        )
        asset.splitter = year_month_splitter(column_name="my_col")
        src, ast, op = batch_request_args
        batch_request = BatchRequest(
            datasource_name=src or source.name,
            data_asset_name=ast or asset.name,
            options=op or {},
        )
        with pytest.raises(ge_exceptions.InvalidBatchRequestError):
            asset.get_batch_list_from_batch_request(batch_request)


def test_get_bad_batch_request(create_source: CreateSourceFixture):
    with create_source(
        validate_batch_spec=lambda _: None, dialect="postgresql"
    ) as source:
        (
            source,  # noqa: PLW2901
            asset,
        ) = create_and_add_table_asset_without_testing_connection(
            source=source, name="my_asset", table_name="my_table"
        )
        asset.splitter = year_month_splitter(column_name="my_col")
        with pytest.raises(ge_exceptions.InvalidBatchRequestError):
            asset.build_batch_request({"invalid_key": None})


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
def test_sort_batch_list_by_metadata(
    empty_data_context, sort_info, create_source: CreateSourceFixture
):
    sort_keys, sort_values = sort_info
    with create_source(
        validate_batch_spec=lambda _: None,
        dialect="postgresql",
        data_context=empty_data_context,
    ) as source:
        (
            source,  # noqa: PLW2901
            asset,
        ) = create_and_add_table_asset_without_testing_connection(
            source=source, name="my_asset", table_name="my_table"
        )
        asset.splitter = year_month_splitter(column_name="my_col")
        asset.add_sorters(sort_keys)
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
def test_sort_batch_list_by_unknown_key(
    empty_data_context, create_source: CreateSourceFixture
):
    with create_source(
        validate_batch_spec=lambda _: None,
        dialect="postgresql",
        data_context=empty_data_context,
    ) as source:
        (
            source,  # noqa: PLW2901
            asset,
        ) = create_and_add_table_asset_without_testing_connection(
            source=source, name="my_asset", table_name="my_table"
        )
        asset.splitter = year_month_splitter(column_name="my_col")
        asset.add_sorters(["yr", "month"])
        batch_request = BatchRequest(
            datasource_name=source.name,
            data_asset_name=asset.name,
            options={},
        )
        with pytest.raises(KeyError):
            source.get_batch_list_from_batch_request(batch_request)


@pytest.mark.unit
@pytest.mark.parametrize(
    "order_by",
    [
        ["+year", "-month"],
        [{"key": "year"}, {"key": "month", "reverse": True}],
    ],
)
def test_table_asset_sorter_parsing(order_by: list):
    """Ensure that arguments to `order_by` are parsed correctly regardless if they are lists of dicts or a list of strings"""
    expected_sorters = [
        Sorter(key="year"),
        Sorter(key="month", reverse=True),
    ]

    table_asset = TableAsset(
        name="SorterTest", table_name="SORTER_TEST", order_by=order_by
    )
    print(table_asset)
    pprint(f"\n{table_asset.dict()}")

    assert table_asset.order_by == expected_sorters


@pytest.mark.unit
@pytest.mark.parametrize(
    "batch_slice,expected_batch_count",
    [
        ("[-3:]", 3),
        ("[5:9]", 4),
        ("[:10:2]", 5),
        (slice(-3, None), 3),
        (slice(5, 9), 4),
        (slice(0, 10, 2), 5),
        ("-5", 1),
        ("-1", 1),
        (11, 1),
        (0, 1),
        ([3], 1),
        (None, 12),
        ("", 12),
    ],
)
def test_postgres_slice_batch_count(
    empty_data_context,
    create_source: CreateSourceFixture,
    batch_slice: BatchSlice,
    expected_batch_count: int,
) -> None:
    with create_source(
        validate_batch_spec=lambda _: None,
        dialect="postgresql",
        data_context=empty_data_context,
    ) as source:
        (
            source,  # noqa: PLW2901
            asset,
        ) = create_and_add_table_asset_without_testing_connection(
            source=source, name="my_asset", table_name="my_table"
        )
        asset.splitter = year_month_splitter(column_name="my_col")
        batch_request = asset.build_batch_request(
            options={"year": 2021}, batch_slice=batch_slice
        )
        batches = asset.get_batch_list_from_batch_request(batch_request=batch_request)
        assert len(batches) == expected_batch_count


@pytest.mark.unit
def test_data_source_json_has_properties(create_source: CreateSourceFixture):
    with create_source(
        validate_batch_spec=lambda _: None, dialect="postgresql"
    ) as source:
        (
            source,  # noqa: PLW2901
            asset,
        ) = create_and_add_table_asset_without_testing_connection(
            source=source, name="my_asset", table_name="my_table"
        )
        asset.splitter = year_month_splitter(column_name="my_col")
        asset.add_sorters(["year", "month"])
        source_json = source.json(indent=4, sort_keys=True)
        print(source_json)
        assert '"order_by": ' in source_json
        # type should be in dumped json even if not explicitly set
        assert f'"type": "{asset.type}"'  # noqa: PLW0129


@pytest.mark.unit
def test_data_source_yaml_has_properties(create_source: CreateSourceFixture):
    with create_source(
        validate_batch_spec=lambda _: None, dialect="postgresql"
    ) as source:
        (
            source,  # noqa: PLW2901
            asset,
        ) = create_and_add_table_asset_without_testing_connection(
            source=source, name="my_asset", table_name="my_table"
        )
        asset.splitter = year_month_splitter(column_name="my_col")
        asset.add_sorters(["year", "month"])
        source_str = source.__str__()
        assert "order_by:" in source_str
        # type should be in dumped str even if not explicitly set
        assert f"type: {asset.type}" in source_str


@pytest.mark.unit
def test_datasource_dict_has_properties(create_source):
    with create_source(
        validate_batch_spec=lambda _: None, dialect="postgresql"
    ) as source:
        (
            source,  # noqa: PLW2901
            asset,
        ) = create_and_add_table_asset_without_testing_connection(
            source=source, name="my_asset", table_name="my_table"
        )
        asset.splitter = year_month_splitter(column_name="my_col")
        asset.add_sorters(["year", "month"])
        source_dict = source.dict()
        pprint(source_dict)
        assert isinstance(
            list(
                filter(
                    lambda element: element["name"] == "my_asset",
                    source_dict["assets"],
                )
            )[0]["order_by"],
            list,
        )
        # type should be in dumped dict even if not explicitly set
        assert (
            "type"
            in list(
                filter(
                    lambda element: element["name"] == "my_asset",
                    source_dict["assets"],
                )
            )[0]
        )


@pytest.mark.unit
@pytest.mark.parametrize(
    "connection_string",
    [
        "postgresql://userName:@hostname/dbName",
        "postgresql://userName@hostname",
        "postgresql://userName:password@hostname",
        "postgres://userName:@hostname",
        "postgresql+psycopg2://userName:@hostname",
        "postgresql+pg8000://userName:@hostname",
    ],
)
def test_validate_valid_postgres_connection_string(
    create_source: CreateSourceFixture, connection_string
):
    connection_string = "postgresql://userName:@hostname/dbName"
    with create_source(
        validate_batch_spec=lambda _: None,
        dialect="postgresql",
        connection_string=connection_string,
    ):
        # As long as no exception is thrown we consider this a pass. Pydantic normalizes the underlying
        # connection string so a direct str comparison isn't possible.
        pass


@pytest.mark.unit
@pytest.mark.parametrize(
    "connection_string",
    [
        "postgresql://",
        "postgresql://username",
        "postgresql://username/dbName",
        "postgresql+invalid://",
    ],
)
def test_validate_invalid_postgres_connection_string(
    create_source: CreateSourceFixture, connection_string
):
    with pytest.raises(ValidationError):
        with create_source(
            validate_batch_spec=lambda _: None,
            dialect="postgresql",
            connection_string=connection_string,
        ):
            pass


def bad_connection_string_config() -> tuple[str, str, str]:
    connection_string = "postgresql+psycopg2://postgres:@localhost/bad_database"
    table_name = "good_table"
    schema_name = "good_schema"
    return (
        connection_string,
        table_name,
        schema_name,
    )


def bad_table_name_config() -> tuple[str, str, str]:
    connection_string = "postgresql+psycopg2://postgres:@localhost/test_ci"
    table_name = "bad_table"
    schema_name = "good_schema"
    return (
        connection_string,
        table_name,
        schema_name,
    )


def bad_schema_name_config() -> tuple[str, str, str]:
    connection_string = "postgresql+psycopg2://postgres:@localhost/test_ci"
    table_name = "good_table"
    schema_name = "bad_schema"
    return (
        connection_string,
        table_name,
        schema_name,
    )


@pytest.fixture(
    params=[
        bad_connection_string_config,
        bad_table_name_config,
        bad_schema_name_config,
    ]
)
def bad_configuration_datasource(
    request,
) -> PostgresDatasource:
    (
        connection_string,
        table_name,
        schema_name,
    ) = request.param()
    table_asset = TableAsset(
        name="table_asset",
        table_name=table_name,
        schema_name=schema_name,
    )
    return PostgresDatasource(
        name="postgres_datasource",
        connection_string=connection_string,
        assets=[
            table_asset,
        ],
    )


@pytest.mark.unit
def test_test_connection_failures(
    mocker,
    bad_configuration_datasource: PostgresDatasource,
):
    create_engine = mocker.patch("sqlalchemy.create_engine")
    create_engine.return_value = MockSaEngine(dialect=Dialect("postgresql"))
    mocker.patch("tests.datasource.fluent.conftest.MockSaEngine.connect")
    inspect = mocker.patch("sqlalchemy.inspect")
    inspect.return_value = MockSaInspector()
    get_schema_names = mocker.patch(
        "tests.sqlalchemy_test_doubles.MockSaInspector.get_schema_names"
    )
    get_schema_names.return_value = ["good_schema"]
    has_table = mocker.patch("tests.sqlalchemy_test_doubles.MockSaInspector.has_table")
    has_table.return_value = False

    with pytest.raises(TestConnectionError):
        bad_configuration_datasource.test_connection()


@pytest.mark.unit
def test_query_data_asset(empty_data_context, create_source):
    query = "SELECT * FROM my_table"

    def validate_batch_spec(spec: SqlAlchemyDatasourceBatchSpec) -> None:
        assert spec == {
            "data_asset_name": "query_asset",
            "query": "SELECT * FROM my_table",
            "temp_table_schema_name": None,
            "batch_identifiers": {},
        }

    with create_source(
        validate_batch_spec=validate_batch_spec,
        dialect="postgresql",
        data_context=empty_data_context,
    ) as source:
        asset = source.add_query_asset(
            name="query_asset", query="SELECT * FROM my_table"
        )
        assert asset.name == "query_asset"
        assert asset.query.lower() == query.lower()
        source.get_batch_list_from_batch_request(asset.build_batch_request())


@pytest.mark.unit
def test_non_select_query_data_asset(create_source):
    with create_source(
        validate_batch_spec=lambda _: None, dialect="postgresql"
    ) as source:
        with pytest.raises(ValueError):
            source.add_query_asset(name="query_asset", query="* FROM my_table")


def test_adding_splitter_persists_results(
    empty_data_context: FileDataContext,
    mock_test_connection,
):
    gx_yaml = pathlib.Path(
        empty_data_context.root_directory, "great_expectations.yml"
    ).resolve(strict=True)

    empty_data_context.sources.add_postgres(
        "my_datasource",
        connection_string="postgresql://postgres:@localhost/not_a_real_db",
    ).add_query_asset(
        name="my_asset", query="select * from table", order_by=["year"]
    ).add_splitter_year(
        column_name="my_col"
    )

    final_yaml: dict = YAMLHandler().load(  # type: ignore[assignment]
        gx_yaml.read_text(),
    )["fluent_datasources"]
    print(f"final_yaml:\n{pf(final_yaml, depth=5)}")

    assert final_yaml["my_datasource"]["assets"]["my_asset"]["splitter"]


@pytest.mark.unit
def test_splitter_year(
    empty_data_context,
    create_source: CreateSourceFixture,
):
    years = [2020, 2021]
    batch_specs = []

    def collect_batch_spec(spec: SqlAlchemyDatasourceBatchSpec) -> None:
        batch_specs.append(spec)

    with create_source(
        validate_batch_spec=collect_batch_spec,
        dialect="postgresql",
        data_context=empty_data_context,
        splitter_query_response=[{"year": year} for year in years],
    ) as source:
        # We use a query asset because then we don't have to mock out db connection tests
        # in this unit test.
        asset = source.add_query_asset(
            name="my_asset", query="select * from table", order_by=["year"]
        )
        asset.add_splitter_year(column_name="my_col")
        batches = source.get_batch_list_from_batch_request(asset.build_batch_request())
        assert len(batches) == len(years)
        for i, year in enumerate(years):
            assert "year" in batches[i].metadata
            assert batches[i].metadata["year"] == year

        assert len(batch_specs) == len(years)
        for spec in batch_specs:
            assert "splitter_method" in spec
            assert spec["splitter_method"] == "split_on_year"


@pytest.mark.unit
def test_splitter_year_and_month(
    empty_data_context,
    create_source: CreateSourceFixture,
):
    years = [2020, 2021]
    months = [6, 8, 9]
    batch_specs = []

    def collect_batch_spec(spec: SqlAlchemyDatasourceBatchSpec) -> None:
        batch_specs.append(spec)

    with create_source(
        validate_batch_spec=collect_batch_spec,
        dialect="postgresql",
        data_context=empty_data_context,
        splitter_query_response=[
            {"year": year, "month": month} for year in years for month in months
        ],
    ) as source:
        # We use a query asset because then we don't have to mock out db connection tests
        # in this unit test.
        asset = source.add_query_asset(
            name="my_asset", query="select * from table", order_by=["year", "month"]
        )
        asset.add_splitter_year_and_month(column_name="my_col")
        batches = source.get_batch_list_from_batch_request(asset.build_batch_request())
        assert len(batches) == len(years) * len(months)
        for i, year in enumerate(years):
            for j, month in enumerate(months):
                batch_index = i * len(months) + j
                assert "year" in batches[batch_index].metadata
                assert "month" in batches[batch_index].metadata
                assert batches[batch_index].metadata["year"] == year
                assert batches[batch_index].metadata["month"] == month

        assert len(batch_specs) == len(years) * len(months)
        for spec in batch_specs:
            assert "splitter_method" in spec
            assert spec["splitter_method"] == "split_on_year_and_month"


@pytest.mark.unit
def test_splitter_year_and_month_and_day(
    empty_data_context,
    create_source: CreateSourceFixture,
):
    years = [2020, 2021]
    months = [6, 8, 9]
    days = [1, 23, 24, 30]
    batch_specs = []

    def collect_batch_spec(spec: SqlAlchemyDatasourceBatchSpec) -> None:
        batch_specs.append(spec)

    with create_source(
        validate_batch_spec=collect_batch_spec,
        dialect="postgresql",
        data_context=empty_data_context,
        splitter_query_response=[
            {"year": year, "month": month, "day": day}
            for year in years
            for month in months
            for day in days
        ],
    ) as source:
        # We use a query asset because then we don't have to mock out db connection tests
        # in this unit test.
        asset = source.add_query_asset(
            name="my_asset",
            query="select * from table",
            order_by=["year", "month", "day"],
        )
        asset.add_splitter_year_and_month_and_day(column_name="my_col")
        batches = source.get_batch_list_from_batch_request(asset.build_batch_request())
        assert len(batches) == len(years) * len(months) * len(days)
        for i, year in enumerate(years):
            for j, month in enumerate(months):
                for k, day in enumerate(days):
                    batch_index = i * len(months) * len(days) + j * len(days) + k
                    assert "year" in batches[batch_index].metadata
                    assert "month" in batches[batch_index].metadata
                    assert "day" in batches[batch_index].metadata
                    assert batches[batch_index].metadata["year"] == year
                    assert batches[batch_index].metadata["month"] == month
                    assert batches[batch_index].metadata["day"] == day

        assert len(batch_specs) == len(years) * len(months) * len(days)
        for spec in batch_specs:
            assert "splitter_method" in spec
            assert spec["splitter_method"] == "split_on_year_and_month_and_day"


@pytest.mark.unit
@pytest.mark.parametrize(
    [
        "add_splitter_method",
        "splitter_kwargs",
        "splitter_query_responses",
        "sorter_args",
        "all_batches_cnt",
        "specified_batch_request",
        "specified_batch_cnt",
        "last_specified_batch_metadata",
    ],
    [
        pytest.param(
            "add_splitter_year",
            {"column_name": "pickup_datetime"},
            [{"year": 2020}],
            ["year"],
            1,
            {"year": 2020},
            1,
            {"year": 2020},
            id="year",
        ),
        pytest.param(
            "add_splitter_year_and_month",
            {"column_name": "pickup_datetime"},
            [{"year": 2020, "month": 1}, {"year": 2020, "month": 2}],
            ["year", "month"],
            2,
            {"year": 2020, "month": 1},
            1,
            {"year": 2020, "month": 1},
            id="year_and_month",
        ),
        pytest.param(
            "add_splitter_year_and_month_and_day",
            {"column_name": "pickup_datetime"},
            [
                {"year": 2020, "month": 2, "day": 10},
                {"year": 2020, "month": 2, "day": 12},
            ],
            ["year", "month", "day"],
            2,
            {"year": 2020, "month": 2, "day": 10},
            1,
            {"year": 2020, "month": 2, "day": 10},
            id="year_and_month_and_day",
        ),
        pytest.param(
            "add_splitter_datetime_part",
            {
                "column_name": "pickup_datetime",
                "datetime_parts": ["year", "month", "day"],
            },
            [
                {"year": 2020, "month": 2, "day": 10},
                {"year": 2020, "month": 2, "day": 12},
            ],
            ["year", "month", "day"],
            2,
            {"year": 2020, "month": 2},
            2,
            {"year": 2020, "month": 2, "day": 12},
            id="datetime_part",
        ),
        pytest.param(
            "add_splitter_column_value",
            {"column_name": "passenger_count"},
            [(1,), (None,), (2,)],
            ["passenger_count"],
            3,
            {"passenger_count": 2},
            1,
            {"passenger_count": 2},
            id="column_value",
        ),
        pytest.param(
            "add_splitter_divided_integer",
            {"column_name": "passenger_count", "divisor": 3},
            [(1,), (2,)],
            ["quotient"],
            2,
            {"quotient": 2},
            1,
            {"quotient": 2},
            id="divisor",
        ),
        pytest.param(
            "add_splitter_mod_integer",
            {"column_name": "passenger_count", "mod": 3},
            [(1,), (2,)],
            ["remainder"],
            2,
            {"remainder": 2},
            1,
            {"remainder": 2},
            id="mod_integer",
        ),
        pytest.param(
            "add_splitter_multi_column_values",
            {"column_names": ["passenger_count", "payment_type"]},
            # These types are (passenger_count, payment_type), that is in column_names order.
            # datetime splitters return dicts while all other splitters return tuples.
            [(3, 1), (1, 1), (1, 2)],
            ["passenger_count", "payment_type"],
            3,
            {"passenger_count": 1},
            2,
            {"passenger_count": 1, "payment_type": 2},
            id="multi_column_values",
        ),
    ],
)
def test_splitter(
    empty_data_context,
    create_source: CreateSourceFixture,
    add_splitter_method,
    splitter_kwargs,
    splitter_query_responses,
    sorter_args,
    all_batches_cnt,
    specified_batch_request,
    specified_batch_cnt,
    last_specified_batch_metadata,
):
    with create_source(
        validate_batch_spec=lambda _: None,
        dialect="postgresql",
        data_context=empty_data_context,
        splitter_query_response=[response for response in splitter_query_responses],
    ) as source:
        asset = source.add_query_asset(name="query_asset", query="SELECT * from table")
        getattr(asset, add_splitter_method)(**splitter_kwargs)
        asset.add_sorters(sorter_args)
        # Test getting all batches
        all_batches = asset.get_batch_list_from_batch_request(
            asset.build_batch_request()
        )
        assert len(all_batches) == all_batches_cnt
        # Test getting specified batches
        specified_batches = asset.get_batch_list_from_batch_request(
            asset.build_batch_request(specified_batch_request)
        )
        assert len(specified_batches) == specified_batch_cnt
        assert specified_batches[-1].metadata == last_specified_batch_metadata


@pytest.mark.unit
def test_sorting_none_in_metadata(
    empty_data_context,
    create_source: CreateSourceFixture,
):
    years = [None, 2020, 2021]

    with create_source(
        validate_batch_spec=lambda _: None,
        dialect="postgresql",
        data_context=empty_data_context,
        splitter_query_response=[{"year": year} for year in years],
    ) as source:
        # We use a query asset because then we don't have to mock out db connection tests
        # in this unit test.
        asset = source.add_query_asset(
            name="my_asset", query="select * from table", order_by=["-year"]
        )
        asset.add_splitter_year(column_name="my_col")
        batches = source.get_batch_list_from_batch_request(asset.build_batch_request())
        assert len(batches) == len(years)
        assert batches[-1].metadata["year"] is None


@pytest.mark.unit
def test_create_temp_table(empty_data_context, create_source):
    with create_source(
        validate_batch_spec=lambda _: None,
        dialect="postgresql",
        data_context=empty_data_context,
        create_temp_table=False,
    ) as source:
        assert source.create_temp_table is False
        asset = source.add_query_asset(name="query_asset", query="SELECT * from table")
        _ = asset.get_batch_list_from_batch_request(asset.build_batch_request())
        assert source._execution_engine._create_temp_table is False


@pytest.mark.unit
def test_add_postgres_query_asset_with_batch_metadata(
    empty_data_context,
    create_source: CreateSourceFixture,
):
    my_config_variables = {"pipeline_filename": __file__}
    empty_data_context.config_variables.update(my_config_variables)

    years = [2021, 2022]
    asset_specified_metadata = {
        "pipeline_name": "my_pipeline",
        "no_curly_pipeline_filename": "$pipeline_filename",
        "curly_pipeline_filename": "${pipeline_filename}",
    }

    with create_source(
        validate_batch_spec=lambda _: None,
        dialect="postgresql",
        data_context=empty_data_context,
        splitter_query_response=[{"year": year} for year in years],
    ) as source:
        asset = source.add_query_asset(
            name="query_asset",
            query="SELECT * FROM my_table",
            batch_metadata=asset_specified_metadata,
            order_by=["year"],
        )
        assert asset.batch_metadata == asset_specified_metadata
        asset.add_splitter_year(column_name="col")
        batches = source.get_batch_list_from_batch_request(asset.build_batch_request())
        assert len(batches) == len(years)
        substituted_batch_metadata: BatchMetadata = copy.deepcopy(
            asset_specified_metadata
        )
        substituted_batch_metadata.update(
            {
                "no_curly_pipeline_filename": __file__,
                "curly_pipeline_filename": __file__,
            }
        )
        for i, year in enumerate(years):
            substituted_batch_metadata["year"] = year
            assert batches[i].metadata == substituted_batch_metadata


@pytest.mark.unit
def test_add_postgres_table_asset_with_batch_metadata(
    empty_data_context, create_source: CreateSourceFixture, monkeypatch
):
    my_config_variables = {"pipeline_filename": __file__}
    empty_data_context.config_variables.update(my_config_variables)

    monkeypatch.setattr(TableAsset, "test_connection", lambda _: None)
    monkeypatch.setattr(TableAsset, "test_splitter_connection", lambda _: None)
    years = [2021, 2022]
    asset_specified_metadata = {
        "pipeline_name": "my_pipeline",
        "no_curly_pipeline_filename": "$pipeline_filename",
        "curly_pipeline_filename": "${pipeline_filename}",
    }

    with create_source(
        validate_batch_spec=lambda _: None,
        dialect="postgresql",
        data_context=empty_data_context,
        splitter_query_response=[{"year": year} for year in years],
    ) as source:
        asset = source.add_table_asset(
            name="query_asset",
            table_name="my_table",
            batch_metadata=asset_specified_metadata,
            order_by=["year"],
        )
        assert asset.batch_metadata == asset_specified_metadata
        asset.add_splitter_year(column_name="my_col")
        batches = source.get_batch_list_from_batch_request(asset.build_batch_request())
        assert len(batches) == len(years)
        substituted_batch_metadata: BatchMetadata = copy.deepcopy(
            asset_specified_metadata
        )
        substituted_batch_metadata.update(
            {
                "no_curly_pipeline_filename": __file__,
                "curly_pipeline_filename": __file__,
            }
        )
        for i, year in enumerate(years):
            substituted_batch_metadata["year"] = year
            assert batches[i].metadata == substituted_batch_metadata
