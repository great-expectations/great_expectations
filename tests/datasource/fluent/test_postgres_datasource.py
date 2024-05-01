from __future__ import annotations

import copy
import logging
import pathlib
from typing import (
    TYPE_CHECKING,
    Generator,
    Literal,
    Optional,
    Tuple,
)

import pytest
from sqlalchemy.exc import SQLAlchemyError

import great_expectations.exceptions as ge_exceptions
from great_expectations.compatibility.pydantic import ValidationError
from great_expectations.core.batch_spec import SqlAlchemyDatasourceBatchSpec
from great_expectations.core.partitioners import (
    ColumnPartitioner,
    PartitionerYear,
    PartitionerYearAndMonth,
    PartitionerYearAndMonthAndDay,
)
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from great_expectations.datasource.fluent.batch_request import (
    BatchParameters,
    BatchRequest,
)
from great_expectations.datasource.fluent.interfaces import (
    TestConnectionError,
)
from great_expectations.datasource.fluent.postgres_datasource import (
    PostgresDatasource,
)
from great_expectations.datasource.fluent.sql_datasource import (
    SqlPartitionerYearAndMonth,
    TableAsset,
)
from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from tests.datasource.fluent.conftest import (
    _DEFAULT_TEST_MONTHS,
    _DEFAULT_TEST_YEARS,
    CreateSourceFixture,
)
from tests.sqlalchemy_test_doubles import Dialect, MockSaEngine, MockSaInspector

if TYPE_CHECKING:
    from unittest.mock import Mock  # noqa: TID251

    from pytest_mock import MockFixture

    from great_expectations.datasource.fluent.interfaces import (
        BatchMetadata,
        BatchSlice,
    )

# We set a default time range that we use for testing.

LOGGER = logging.getLogger(__name__)


@pytest.fixture
def mock_test_connection(monkeypatch: pytest.MonkeyPatch):
    """Patches the test_connection method of the PostgresDatasource class to return True."""

    def _mock_test_connection(self: PostgresDatasource) -> bool:
        LOGGER.warning(
            f"Mocked {self.__class__.__name__}.test_connection() called and returning True"
        )
        return True

    monkeypatch.setattr(PostgresDatasource, "test_connection", _mock_test_connection)


@pytest.mark.postgresql
def test_construct_postgres_datasource(create_source: CreateSourceFixture):
    with create_source(validate_batch_spec=lambda _: None, dialect="postgresql") as source:
        assert source.name == "my_datasource"
        assert source.execution_engine_type is SqlAlchemyExecutionEngine
        assert source.assets == []


def assert_table_asset(
    asset: TableAsset,
    name: str,
    table_name: str,
    source: PostgresDatasource,
    batch_parameters: tuple[str, ...],
    partitioner: Optional[ColumnPartitioner] = None,
):
    assert asset.name == name
    assert asset.table_name == table_name
    assert asset.datasource == source
    assert asset.get_batch_parameters_keys(partitioner=partitioner) == batch_parameters


def assert_batch_request(
    batch_request, source_name: str, asset_name: str, options: BatchParameters
):
    assert batch_request.datasource_name == source_name
    assert batch_request.data_asset_name == asset_name
    assert batch_request.options == options


@pytest.mark.postgresql
def test_add_table_asset_with_partitioner(mocker, create_source: CreateSourceFixture):
    with create_source(validate_batch_spec=lambda _: None, dialect="postgresql") as source:
        mocker.patch("sqlalchemy.create_engine")
        mocker.patch("sqlalchemy.engine")
        inspect = mocker.patch("sqlalchemy.inspect")
        inspect.return_value = MockSaInspector()
        get_column_names = mocker.patch("tests.sqlalchemy_test_doubles.MockSaInspector.get_columns")
        get_column_names.return_value = [{"name": "my_col"}]
        has_table = mocker.patch("tests.sqlalchemy_test_doubles.MockSaInspector.has_table")
        has_table.return_value = True

        asset = source.add_table_asset(name="my_asset", table_name="my_table")
        partitioner = PartitionerYearAndMonth(column_name="my_col")
        assert len(source.assets) == 1
        assert asset == source.assets[0]
        assert_table_asset(
            asset=asset,
            name="my_asset",
            table_name="my_table",
            source=source,
            batch_parameters=("year", "month"),
            partitioner=partitioner,
        )
        assert_batch_request(
            batch_request=asset.build_batch_request(
                options={"year": 2021, "month": 10}, partitioner=partitioner
            ),
            source_name="my_datasource",
            asset_name="my_asset",
            options={"year": 2021, "month": 10},
        )


@pytest.mark.postgresql
def test_add_table_asset_with_no_partitioner(mocker, create_source: CreateSourceFixture):
    with create_source(validate_batch_spec=lambda _: None, dialect="postgresql") as source:
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
            batch_parameters=tuple(),
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


@pytest.mark.postgresql
def test_construct_table_asset_directly_with_no_partitioner(create_source):
    with create_source(validate_batch_spec=lambda _: None, dialect="postgresql") as source:
        asset = TableAsset(name="my_asset", table_name="my_table")
        asset._datasource = source
        assert_batch_request(asset.build_batch_request(), "my_datasource", "my_asset", {})


def create_and_add_table_asset_without_testing_connection(
    source: PostgresDatasource,
    name: str,
    table_name: str,
) -> Tuple[PostgresDatasource, TableAsset]:
    table_asset = TableAsset(
        name=name,
        table_name=table_name,
    )
    # TODO: asset custom init
    table_asset._datasource = source
    source.assets.append(table_asset)
    return source, table_asset


def year_month_partitioner(column_name: str) -> SqlPartitionerYearAndMonth:
    return SqlPartitionerYearAndMonth(
        column_name=column_name,
        method_name="partition_on_year_and_month",
    )


@pytest.mark.postgresql
def test_construct_table_asset_directly_with_partitioner(create_source):
    with create_source(validate_batch_spec=lambda _: None, dialect="postgresql") as source:
        (
            source,  # noqa: PLW2901
            asset,
        ) = create_and_add_table_asset_without_testing_connection(
            source=source,
            name="my_asset",
            table_name="my_table",
        )
        partitioner = PartitionerYearAndMonth(column_name="col")
        assert_table_asset(
            asset,
            "my_asset",
            "my_table",
            source,
            ("year", "month"),
            partitioner=partitioner,
        )
        batch_parameters = {"year": 2022, "month": 10}
        assert_batch_request(
            asset.build_batch_request(options=batch_parameters, partitioner=partitioner),
            "my_datasource",
            "my_asset",
            batch_parameters,
        )


@pytest.mark.postgresql
def test_datasource_gets_batch_list_no_partitioner(empty_data_context, create_source):
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


def assert_batch_specs_correct_with_year_month_partitioner_defaults(batch_specs):
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
                "partitioner_method": "partition_on_year_and_month",
                "partitioner_kwargs": {"column_name": "my_col"},
            }
            assert spec in batch_specs


def assert_batches_correct_with_year_month_partitioner_defaults(batches):
    # We should have 1 batch_spec per (year, month) pair
    expected_batch_spec_num = len(_DEFAULT_TEST_YEARS) * len(_DEFAULT_TEST_MONTHS)
    assert len(batches) == expected_batch_spec_num
    metadatas = [batch.metadata for batch in batches]
    for year in _DEFAULT_TEST_YEARS:
        for month in _DEFAULT_TEST_MONTHS:
            assert {"year": year, "month": month} in metadatas


@pytest.mark.postgresql
def test_datasource_gets_batch_list_partitioner_with_unspecified_batch_parameters(
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
        partitioner = PartitionerYearAndMonth(column_name="my_col")
        empty_batch_request = asset.build_batch_request(partitioner=partitioner)
        assert empty_batch_request.options == {}
        batches = source.get_batch_list_from_batch_request(empty_batch_request)
        assert_batch_specs_correct_with_year_month_partitioner_defaults(batch_specs)
        assert_batches_correct_with_year_month_partitioner_defaults(batches)


@pytest.mark.postgresql
def test_datasource_gets_batch_list_partitioner_with_batch_parameters_set_to_none(
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
        partitioner = PartitionerYearAndMonth(column_name="my_col")
        assert asset.get_batch_parameters_keys(partitioner=partitioner) == (
            "year",
            "month",
        )
        batch_request_with_none = asset.build_batch_request(
            options={"year": None, "month": None}, partitioner=partitioner
        )
        assert batch_request_with_none.options == {"year": None, "month": None}
        batches = source.get_batch_list_from_batch_request(batch_request_with_none)
        # We should have 1 batch_spec per (year, month) pair
        assert_batch_specs_correct_with_year_month_partitioner_defaults(batch_specs)
        assert_batches_correct_with_year_month_partitioner_defaults(batches)


@pytest.mark.postgresql
def test_datasource_gets_batch_list_partitioner_with_partially_specified_batch_parameters(
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
        partitioner_query_response=[{"year": year, "month": month} for month in list(range(1, 13))],
    ) as source:
        (
            source,  # noqa: PLW2901
            asset,
        ) = create_and_add_table_asset_without_testing_connection(
            source=source, name="my_asset", table_name="my_table"
        )
        partitioner = PartitionerYearAndMonth(column_name="my_col")
        batches = source.get_batch_list_from_batch_request(
            asset.build_batch_request(options={"year": year}, partitioner=partitioner)
        )
        assert len(batch_specs) == len(_DEFAULT_TEST_MONTHS)
        for month in _DEFAULT_TEST_MONTHS:
            spec = {
                "type": "table",
                "data_asset_name": "my_asset",
                "table_name": "my_table",
                "schema_name": None,
                "batch_identifiers": {"my_col": {"year": year, "month": month}},
                "partitioner_method": "partition_on_year_and_month",
                "partitioner_kwargs": {"column_name": "my_col"},
            }
            assert spec in batch_specs

        assert len(batches) == len(_DEFAULT_TEST_MONTHS)
        metadatas = [batch.metadata for batch in batches]
        for month in _DEFAULT_TEST_MONTHS:
            expected_metadata = {"month": month, "year": year}
            assert expected_metadata in metadatas


@pytest.mark.postgresql
def test_datasource_gets_batch_list_with_fully_specified_batch_parameters(
    empty_data_context,
    create_source: CreateSourceFixture,
):
    year = 2022
    month = 1

    def validate_batch_spec(spec: SqlAlchemyDatasourceBatchSpec) -> None:
        assert spec == {
            "batch_identifiers": {"my_col": {"month": 1, "year": 2022}},
            "data_asset_name": "my_asset",
            "partitioner_kwargs": {"column_name": "my_col"},
            "partitioner_method": "partition_on_year_and_month",
            "table_name": "my_table",
            "schema_name": None,
            "type": "table",
        }

    with create_source(
        validate_batch_spec=validate_batch_spec,
        dialect="postgresql",
        data_context=empty_data_context,
        partitioner_query_response=[{"month": month, "year": year}],
    ) as source:
        (
            source,  # noqa: PLW2901
            asset,
        ) = create_and_add_table_asset_without_testing_connection(
            source=source, name="my_asset", table_name="my_table"
        )
        partitioner = PartitionerYearAndMonth(column_name="my_col")
        batches = source.get_batch_list_from_batch_request(
            asset.build_batch_request(
                options={"month": month, "year": year}, partitioner=partitioner
            )
        )
        assert len(batches) == 1
        assert batches[0].metadata == {"month": month, "year": year}


@pytest.mark.postgresql
def test_datasource_gets_nonexistent_asset(create_source: CreateSourceFixture):
    with create_source(validate_batch_spec=lambda _: None, dialect="postgresql") as source:
        with pytest.raises(LookupError):
            source.get_asset("my_asset")


@pytest.mark.postgresql
@pytest.mark.parametrize(
    [
        "PartitionerClass",
        "add_partitioner_kwargs",
        "batch_request_args",
    ],
    [
        # These bad parameters do not occur in the batch request params
        (PartitionerYear, {"column_name": "my_col"}, ("bad", None, None)),
        (PartitionerYear, {"column_name": "my_col"}, (None, "bad", None)),
        (PartitionerYear, {"column_name": "my_col"}, ("bad", "bad", None)),
        # These bad parameters are request option parameters which depend on the partitioner.
        (
            PartitionerYear,
            {"column_name": "my_col"},
            (None, None, {"bad": None}),
        ),
        (
            PartitionerYearAndMonth,
            {"column_name": "my_col"},
            (None, None, {"bad": None}),
        ),
        (
            PartitionerYearAndMonthAndDay,
            {"column_name": "my_col"},
            (None, None, {"bad": None}),
        ),
    ],
)
def test_bad_batch_request_passed_into_get_batch_list_from_batch_request(
    create_source: CreateSourceFixture,
    PartitionerClass,
    add_partitioner_kwargs,
    batch_request_args,
):
    with create_source(validate_batch_spec=lambda _: None, dialect="postgresql") as source:
        # We use a QueryAsset so no database connection checks are done.
        # We are mocking the database so these would fail without monkeypatching the
        # TableAsset
        asset = source.add_query_asset(name="query_asset", query="SELECT * FROM my_table")
        partitioner = PartitionerClass(**add_partitioner_kwargs)

        src, ast, op = batch_request_args
        batch_request = BatchRequest[ColumnPartitioner](
            datasource_name=src or source.name,
            data_asset_name=ast or asset.name,
            options=op or {},
            partitioner=partitioner,
        )
        with pytest.raises(
            (
                ge_exceptions.InvalidBatchRequestError,
                LookupError,
            )
        ):
            source.get_batch_list_from_batch_request(batch_request)


@pytest.mark.postgresql
@pytest.mark.parametrize(
    "batch_parameters",
    [{}, {"year": 2021}, {"year": 2021, "month": 10}, {"year": None, "month": 10}],
)
def test_get_batch_list_from_batch_request_with_good_batch_request(
    empty_data_context,
    create_source: CreateSourceFixture,
    batch_parameters,
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
        partitioner = PartitionerYearAndMonth(column_name="my_col")
        batch_request = BatchRequest(
            datasource_name=source.name,
            data_asset_name=asset.name,
            options=batch_parameters,
            partitioner=partitioner,
        )
        # No exception should get thrown
        asset.get_batch_list_from_batch_request(batch_request)


@pytest.mark.postgresql
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
    with create_source(validate_batch_spec=lambda _: None, dialect="postgresql") as source:
        (
            source,  # noqa: PLW2901
            asset,
        ) = create_and_add_table_asset_without_testing_connection(
            source=source, name="my_asset", table_name="my_table"
        )
        partitioner = PartitionerYearAndMonth(column_name="my_col")
        src, ast, op = batch_request_args
        batch_request = BatchRequest(
            datasource_name=src or source.name,
            data_asset_name=ast or asset.name,
            options=op or {},
            partitioner=partitioner,
        )
        with pytest.raises(ge_exceptions.InvalidBatchRequestError):
            asset.get_batch_list_from_batch_request(batch_request)


@pytest.mark.postgresql
def test_get_bad_batch_request(create_source: CreateSourceFixture):
    with create_source(validate_batch_spec=lambda _: None, dialect="postgresql") as source:
        (
            source,  # noqa: PLW2901
            asset,
        ) = create_and_add_table_asset_without_testing_connection(
            source=source, name="my_asset", table_name="my_table"
        )
        partitioner = PartitionerYearAndMonth(column_name="my_col")
        with pytest.raises(ge_exceptions.InvalidBatchRequestError):
            asset.build_batch_request(options={"invalid_key": None}, partitioner=partitioner)


@pytest.mark.postgresql
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
        partitioner = PartitionerYearAndMonth(column_name="my_col")
        batch_request = asset.build_batch_request(
            options={"year": 2021}, batch_slice=batch_slice, partitioner=partitioner
        )
        batches = asset.get_batch_list_from_batch_request(batch_request=batch_request)
        assert len(batches) == expected_batch_count


@pytest.mark.postgresql
def test_data_source_json_has_properties(create_source: CreateSourceFixture):
    with create_source(validate_batch_spec=lambda _: None, dialect="postgresql") as source:
        (
            source,  # noqa: PLW2901
            asset,
        ) = create_and_add_table_asset_without_testing_connection(
            source=source, name="my_asset", table_name="my_table"
        )
        # type should be in dumped json even if not explicitly set
        assert f'"type": "{asset.type}"'  # noqa: PLW0129


@pytest.mark.postgresql
def test_data_source_yaml_has_properties(create_source: CreateSourceFixture):
    with create_source(validate_batch_spec=lambda _: None, dialect="postgresql") as source:
        (
            source,  # noqa: PLW2901
            asset,
        ) = create_and_add_table_asset_without_testing_connection(
            source=source, name="my_asset", table_name="my_table"
        )
        source_str = source.__str__()
        # type should be in dumped str even if not explicitly set
        assert f"type: {asset.type}" in source_str


@pytest.mark.postgresql
def test_datasource_dict_has_properties(create_source):
    with create_source(validate_batch_spec=lambda _: None, dialect="postgresql") as source:
        (
            source,  # noqa: PLW2901
            _,
        ) = create_and_add_table_asset_without_testing_connection(
            source=source, name="my_asset", table_name="my_table"
        )
        source_dict = source.dict()
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


@pytest.mark.postgresql
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
        # As long as no exception is thrown we consider this a pass. Pydantic normalizes the underlying  # noqa: E501
        # connection string so a direct str comparison isn't possible.
        pass


@pytest.mark.postgresql
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


@pytest.fixture
def mock_create_engine(mocker: MockFixture) -> Mock:
    create_engine = mocker.patch("sqlalchemy.create_engine")
    create_engine.return_value = MockSaEngine(dialect=Dialect("postgresql"))
    return create_engine


@pytest.fixture
def mock_inspector(
    mocker: MockFixture,
) -> Generator[dict[Literal["schema_names", "table_names"], list[str]], None, None]:
    """
    Mock the inspector to return the schema and table names we want.
    Append the desired schema and table names (in tests) to the mock_inspector_returns dict.
    """
    mock_inspector_returns: dict[Literal["schema_names", "table_names"], list[str]] = {
        "schema_names": [],
        "table_names": [],
    }
    mock_inspector = MockSaInspector()

    def get_table_names(schema: str | None = None) -> list[str]:
        LOGGER.info("MockSaInspector.get_table_names() called")
        return mock_inspector_returns["table_names"]

    def get_schema_names() -> list[str]:
        LOGGER.info("MockSaInspector.get_schema_names() called")
        return mock_inspector_returns["schema_names"]

    def has_table(table_name: str, schema: str | None = None) -> bool:
        LOGGER.info("MockSaInspector.has_table() called")
        return table_name in mock_inspector_returns["table_names"]

    # directly patching the instance rather then using mocker.patch
    mock_inspector.get_schema_names = get_schema_names  # type: ignore[method-assign]
    mock_inspector.get_table_names = get_table_names  # type: ignore[method-assign]
    mock_inspector.has_table = has_table  # type: ignore[method-assign]

    inspect = mocker.patch("sqlalchemy.inspect")
    inspect.return_value = mock_inspector

    yield mock_inspector_returns


@pytest.fixture
def mock_connection_execute_failure(mocker: MockFixture) -> Mock:
    """
    Engine execute should raise an exception when called.
    """
    execute = mocker.patch("tests.sqlalchemy_test_doubles._MockConnection.execute")

    def execute_side_effect(*args, **kwargs):
        LOGGER.info("MockSaEngine.execute() called")
        raise SQLAlchemyError("Mocked exception")

    execute.side_effect = execute_side_effect
    return execute


@pytest.mark.postgresql
def test_test_connection_failures(
    mock_create_engine: Mock,
    mock_connection_execute_failure: Mock,
    mock_inspector: dict[Literal["schema_names", "table_names"], list[str]],
    bad_configuration_datasource: PostgresDatasource,
):
    mock_inspector["schema_names"].extend(["good_schema"])
    mock_inspector["table_names"].extend(["good_table", "bad_table"])

    with pytest.raises(TestConnectionError):
        bad_configuration_datasource.test_connection()


@pytest.mark.filesystem
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
        asset = source.add_query_asset(name="query_asset", query="SELECT * FROM my_table")
        assert asset.name == "query_asset"
        assert asset.query.lower() == query.lower()
        source.get_batch_list_from_batch_request(asset.build_batch_request())


@pytest.mark.postgresql
def test_non_select_query_data_asset(create_source):
    with create_source(validate_batch_spec=lambda _: None, dialect="postgresql") as source:
        with pytest.raises(ValueError):
            source.add_query_asset(name="query_asset", query="* FROM my_table")


@pytest.mark.filesystem
def test_adding_partitioner_persists_results(
    empty_data_context: FileDataContext,
    mock_test_connection,
):
    gx_yaml = pathlib.Path(empty_data_context.root_directory, FileDataContext.GX_YML).resolve(
        strict=True
    )

    empty_data_context.data_sources.add_postgres(
        name="my_datasource",
        connection_string="postgresql://postgres:@localhost/not_a_real_db",
    ).add_query_asset(
        name="my_asset", query="select * from table", order_by=["year"]
    ).add_batch_definition(
        name="my_batch_definition", partitioner=PartitionerYear(column_name="my_col")
    )

    final_yaml: dict = YAMLHandler().load(  # type: ignore[assignment]
        gx_yaml.read_text(),
    )["fluent_datasources"]

    assert final_yaml["my_datasource"]["assets"]["my_asset"]["batch_definitions"][
        "my_batch_definition"
    ]["partitioner"]


@pytest.mark.postgresql
def test_partitioner_year(
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
        partitioner_query_response=[{"year": year} for year in years],
    ) as source:
        # We use a query asset because then we don't have to mock out db connection tests
        # in this unit test.
        asset = source.add_query_asset(
            name="my_asset", query="select * from table", order_by=["year"]
        )
        partitioner = PartitionerYear(column_name="my_col")
        batches = source.get_batch_list_from_batch_request(
            asset.build_batch_request(partitioner=partitioner)
        )
        assert len(batches) == len(years)
        for i, year in enumerate(years):
            assert "year" in batches[i].metadata
            assert batches[i].metadata["year"] == year

        assert len(batch_specs) == len(years)
        for spec in batch_specs:
            assert "partitioner_method" in spec
            assert spec["partitioner_method"] == "partition_on_year"


@pytest.mark.postgresql
def test_partitioner_year_and_month(
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
        partitioner_query_response=[
            {"year": year, "month": month} for year in years for month in months
        ],
    ) as source:
        # We use a query asset because then we don't have to mock out db connection tests
        # in this unit test.
        asset = source.add_query_asset(
            name="my_asset", query="select * from table", order_by=["year", "month"]
        )
        partitioner = PartitionerYearAndMonth(column_name="my_col")
        batches = source.get_batch_list_from_batch_request(
            asset.build_batch_request(partitioner=partitioner)
        )
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
            assert "partitioner_method" in spec
            assert spec["partitioner_method"] == "partition_on_year_and_month"


@pytest.mark.postgresql
def test_partitioner_year_and_month_and_day(
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
        partitioner_query_response=[
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
        partitioner = PartitionerYearAndMonthAndDay(column_name="my_col")
        batches = source.get_batch_list_from_batch_request(
            asset.build_batch_request(partitioner=partitioner)
        )
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
            assert "partitioner_method" in spec
            assert spec["partitioner_method"] == "partition_on_year_and_month_and_day"


@pytest.mark.postgresql
@pytest.mark.parametrize(
    [
        "PartitionerClass",
        "partitioner_kwargs",
        "partitioner_query_responses",
        "all_batches_cnt",
        "specified_batch_request",
        "specified_batch_cnt",
        "last_specified_batch_metadata",
    ],
    [
        pytest.param(
            PartitionerYear,
            {"column_name": "pickup_datetime"},
            [{"year": 2020}],
            1,
            {"year": 2020},
            1,
            {"year": 2020},
            id="year",
        ),
        pytest.param(
            PartitionerYearAndMonth,
            {"column_name": "pickup_datetime"},
            [{"year": 2020, "month": 1}, {"year": 2020, "month": 2}],
            2,
            {"year": 2020, "month": 1},
            1,
            {"year": 2020, "month": 1},
            id="year_and_month",
        ),
        pytest.param(
            PartitionerYearAndMonthAndDay,
            {"column_name": "pickup_datetime"},
            [
                {"year": 2020, "month": 2, "day": 10},
                {"year": 2020, "month": 2, "day": 12},
            ],
            2,
            {"year": 2020, "month": 2, "day": 10},
            1,
            {"year": 2020, "month": 2, "day": 10},
            id="year_and_month_and_day",
        ),
    ],
)
def test_partitioner(
    empty_data_context,
    create_source: CreateSourceFixture,
    PartitionerClass,
    partitioner_kwargs,
    partitioner_query_responses,
    all_batches_cnt,
    specified_batch_request,
    specified_batch_cnt,
    last_specified_batch_metadata,
):
    with create_source(
        validate_batch_spec=lambda _: None,
        dialect="postgresql",
        data_context=empty_data_context,
        partitioner_query_response=[response for response in partitioner_query_responses],
    ) as source:
        asset = source.add_query_asset(name="query_asset", query="SELECT * from table")
        partitioner = PartitionerClass(**partitioner_kwargs)
        # Test getting all batches
        all_batches = asset.get_batch_list_from_batch_request(
            asset.build_batch_request(partitioner=partitioner)
        )
        assert len(all_batches) == all_batches_cnt
        # Test getting specified batches
        specified_batches = asset.get_batch_list_from_batch_request(
            asset.build_batch_request(specified_batch_request, partitioner=partitioner)
        )
        assert len(specified_batches) == specified_batch_cnt
        assert specified_batches[-1].metadata == last_specified_batch_metadata


@pytest.mark.postgresql
def test_sorting_none_in_metadata(
    empty_data_context,
    create_source: CreateSourceFixture,
):
    years = [None, 2020, 2021]

    with create_source(
        validate_batch_spec=lambda _: None,
        dialect="postgresql",
        data_context=empty_data_context,
        partitioner_query_response=[{"year": year} for year in years],
    ) as source:
        # We use a query asset because then we don't have to mock out db connection tests
        # in this unit test.
        asset = source.add_query_asset(name="my_asset", query="select * from table")
        partitioner = PartitionerYear(column_name="my_col", sort_ascending=False)
        batches = source.get_batch_list_from_batch_request(
            asset.build_batch_request(partitioner=partitioner)
        )
        assert len(batches) == len(years)
        assert batches[-1].metadata["year"] is None


@pytest.mark.postgresql
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


@pytest.mark.postgresql
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
        partitioner_query_response=[{"year": year} for year in years],
    ) as source:
        asset = source.add_query_asset(
            name="query_asset",
            query="SELECT * FROM my_table",
            batch_metadata=asset_specified_metadata,
            order_by=["year"],
        )
        assert asset.batch_metadata == asset_specified_metadata
        partitioner = PartitionerYear(column_name="col")
        batches = source.get_batch_list_from_batch_request(
            asset.build_batch_request(partitioner=partitioner)
        )
        assert len(batches) == len(years)
        substituted_batch_metadata: BatchMetadata = copy.deepcopy(asset_specified_metadata)
        substituted_batch_metadata.update(
            {
                "no_curly_pipeline_filename": __file__,
                "curly_pipeline_filename": __file__,
            }
        )
        for i, year in enumerate(years):
            substituted_batch_metadata["year"] = year
            assert batches[i].metadata == substituted_batch_metadata


@pytest.mark.postgresql
def test_add_postgres_table_asset_with_batch_metadata(
    empty_data_context, create_source: CreateSourceFixture, monkeypatch
):
    my_config_variables = {"pipeline_filename": __file__}
    empty_data_context.config_variables.update(my_config_variables)

    monkeypatch.setattr(TableAsset, "test_connection", lambda _: None)
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
        partitioner_query_response=[{"year": year} for year in years],
    ) as source:
        asset = source.add_table_asset(
            name="query_asset",
            table_name="my_table",
            batch_metadata=asset_specified_metadata,
            order_by=["year"],
        )
        assert asset.batch_metadata == asset_specified_metadata
        partitioner = PartitionerYear(column_name="my_col")
        batches = source.get_batch_list_from_batch_request(
            asset.build_batch_request(partitioner=partitioner)
        )
        assert len(batches) == len(years)
        substituted_batch_metadata: BatchMetadata = copy.deepcopy(asset_specified_metadata)
        substituted_batch_metadata.update(
            {
                "no_curly_pipeline_filename": __file__,
                "curly_pipeline_filename": __file__,
            }
        )
        for i, year in enumerate(years):
            substituted_batch_metadata["year"] = year
            assert batches[i].metadata == substituted_batch_metadata
