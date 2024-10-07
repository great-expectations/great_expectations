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
from unittest.mock import ANY

import pytest
from sqlalchemy.exc import SQLAlchemyError

import great_expectations.exceptions as ge_exceptions
from great_expectations.compatibility.pydantic import ValidationError
from great_expectations.core.batch_spec import SqlAlchemyDatasourceBatchSpec
from great_expectations.core.id_dict import IDDict
from great_expectations.core.partitioners import (
    ColumnPartitioner,
    ColumnPartitionerDaily,
    ColumnPartitionerMonthly,
    ColumnPartitionerYearly,
    PartitionerColumnValue,
    PartitionerDatetimePart,
    PartitionerDividedInteger,
    PartitionerModInteger,
    PartitionerMultiColumnValue,
)
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from great_expectations.datasource.fluent.batch_request import (
    BatchParameters,
    BatchRequest,
)
from great_expectations.datasource.fluent.interfaces import TestConnectionError
from great_expectations.datasource.fluent.postgres_datasource import (
    PostgresDatasource,
)
from great_expectations.datasource.fluent.sql_datasource import (
    SqlPartitionerYearAndMonth,
    TableAsset,
)
from great_expectations.exceptions.exceptions import NoAvailableBatchesError
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
        partitioner = ColumnPartitionerMonthly(column_name="my_col")
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
        partitioner = ColumnPartitionerMonthly(column_name="col")
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
        source.get_batch_identifiers_list(asset.build_batch_request())


def assert_batch_specs_correct_with_year_month_partitioner_defaults(batch_specs):
    # We should have only collected 1 batch_spec
    assert len(batch_specs) == 1
    for year in _DEFAULT_TEST_YEARS:
        for month in _DEFAULT_TEST_MONTHS:
            spec = {
                "type": "table",
                "data_asset_name": "my_asset",
                "table_name": "my_table",
                "schema_name": None,
                "batch_identifiers": {"my_col": {"year": ANY, "month": ANY}},
                "partitioner_method": "partition_on_year_and_month",
                "partitioner_kwargs": {"column_name": "my_col"},
            }
            assert spec in batch_specs


def assert_batch_identifiers_correct_with_year_month_partitioner_defaults(
    batch_identifiers_list: list[dict],
):
    # We should have 1 batch_spec per (year, month) pair
    expected_batch_spec_num = len(_DEFAULT_TEST_YEARS) * len(_DEFAULT_TEST_MONTHS)
    assert len(batch_identifiers_list) == expected_batch_spec_num
    for year in _DEFAULT_TEST_YEARS:
        for month in _DEFAULT_TEST_MONTHS:
            assert {"year": year, "month": month} in batch_identifiers_list


@pytest.mark.postgresql
def test_datasource_gets_batch_partitioner_with_unspecified_batch_parameters(
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
        partitioner = ColumnPartitionerMonthly(column_name="my_col")
        empty_batch_request = asset.build_batch_request(partitioner=partitioner)
        assert empty_batch_request.options == {}
        batch_identifiers_list = source.get_batch_identifiers_list(empty_batch_request)
        batch = source.get_batch(empty_batch_request)

        assert batch.metadata == {"month": 12, "year": 2022}
        assert_batch_specs_correct_with_year_month_partitioner_defaults(batch_specs)
        assert_batch_identifiers_correct_with_year_month_partitioner_defaults(
            [IDDict(bi) for bi in batch_identifiers_list]
        )


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
        partitioner = ColumnPartitionerMonthly(column_name="my_col")
        assert asset.get_batch_parameters_keys(partitioner=partitioner) == (
            "year",
            "month",
        )
        batch_request_with_none = asset.build_batch_request(
            options={"year": None, "month": None}, partitioner=partitioner
        )
        assert batch_request_with_none.options == {"year": None, "month": None}
        batch_identifiers_list = source.get_batch_identifiers_list(batch_request_with_none)
        batch = source.get_batch(batch_request_with_none)

        # We should have 1 batch_identifier per (year, month) pair

        assert batch.metadata == {"month": 12, "year": 2022}
        assert_batch_specs_correct_with_year_month_partitioner_defaults(batch_specs)
        assert_batch_identifiers_correct_with_year_month_partitioner_defaults(
            [IDDict(bi) for bi in batch_identifiers_list]
        )


@pytest.mark.postgresql
def test_datasource_gets_batch_partitioner_with_partially_specified_batch_parameters(
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
        partitioner = ColumnPartitionerMonthly(column_name="my_col")
        batch_request = asset.build_batch_request(options={"year": year}, partitioner=partitioner)
        identifiers_list = source.get_batch_identifiers_list(batch_request)
        batch = source.get_batch(batch_request)
        assert len(batch_specs) == 1
        assert batch_specs[0] == {
            "type": "table",
            "data_asset_name": "my_asset",
            "table_name": "my_table",
            "schema_name": None,
            "batch_identifiers": {"my_col": {"year": 2022, "month": 12}},
            "partitioner_method": "partition_on_year_and_month",
            "partitioner_kwargs": {"column_name": "my_col"},
        }

        assert len(identifiers_list) == len(_DEFAULT_TEST_MONTHS)
        for month in _DEFAULT_TEST_MONTHS:
            expected_metadata = IDDict({"month": month, "year": year})
            assert expected_metadata in identifiers_list
        assert batch.metadata == {"month": 12, "year": 2022}


@pytest.mark.postgresql
def test_datasource_gets_batch_with_fully_specified_batch_parameters(
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
        partitioner = ColumnPartitionerMonthly(column_name="my_col")
        batches = source.get_batch(
            asset.build_batch_request(
                options={"month": month, "year": year}, partitioner=partitioner
            )
        )
        assert batches.metadata == {"month": month, "year": year}


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
        (ColumnPartitionerYearly, {"column_name": "my_col"}, ("bad", None, None)),
        (ColumnPartitionerYearly, {"column_name": "my_col"}, (None, "bad", None)),
        (ColumnPartitionerYearly, {"column_name": "my_col"}, ("bad", "bad", None)),
        # These bad parameters are request option parameters which depend on the partitioner.
        (
            ColumnPartitionerYearly,
            {"column_name": "my_col"},
            (None, None, {"bad": None}),
        ),
        (
            ColumnPartitionerMonthly,
            {"column_name": "my_col"},
            (None, None, {"bad": None}),
        ),
        (
            ColumnPartitionerDaily,
            {"column_name": "my_col"},
            (None, None, {"bad": None}),
        ),
        (
            PartitionerDatetimePart,
            {"column_name": "my_col", "datetime_parts": ["year"]},
            (None, None, {"bad": None}),
        ),
        (
            PartitionerColumnValue,
            {"column_name": "my_col"},
            (None, None, {"bad": None}),
        ),
        (
            PartitionerDividedInteger,
            {"column_name": "my_col", "divisor": 3},
            (None, None, {"bad": None}),
        ),
    ],
)
def test_bad_batch_request_passed_into_get_batch(
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
            source.get_batch(batch_request)


@pytest.mark.postgresql
@pytest.mark.parametrize(
    "batch_parameters",
    [{}, {"year": 2021}, {"year": 2021, "month": 10}, {"year": None, "month": 10}],
)
def test_get_batch_with_good_batch_request(
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
        partitioner = ColumnPartitionerMonthly(column_name="my_col")
        batch_request = BatchRequest(
            datasource_name=source.name,
            data_asset_name=asset.name,
            options=batch_parameters,
            partitioner=partitioner,
        )
        # No exception should get thrown
        asset.get_batch(batch_request)


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
def test_get_batch_with_malformed_batch_request(
    create_source: CreateSourceFixture, batch_request_args
):
    with create_source(validate_batch_spec=lambda _: None, dialect="postgresql") as source:
        (
            source,  # noqa: PLW2901
            asset,
        ) = create_and_add_table_asset_without_testing_connection(
            source=source, name="my_asset", table_name="my_table"
        )
        partitioner = ColumnPartitionerMonthly(column_name="my_col")
        src, ast, op = batch_request_args
        batch_request = BatchRequest(
            datasource_name=src or source.name,
            data_asset_name=ast or asset.name,
            options=op or {},
            partitioner=partitioner,
        )
        with pytest.raises(ge_exceptions.InvalidBatchRequestError):
            asset.get_batch(batch_request)


@pytest.mark.postgresql
def test_get_bad_batch_request(create_source: CreateSourceFixture):
    with create_source(validate_batch_spec=lambda _: None, dialect="postgresql") as source:
        (
            source,  # noqa: PLW2901
            asset,
        ) = create_and_add_table_asset_without_testing_connection(
            source=source, name="my_asset", table_name="my_table"
        )
        partitioner = ColumnPartitionerMonthly(column_name="my_col")
        with pytest.raises(ge_exceptions.InvalidBatchRequestError):
            asset.build_batch_request(options={"invalid_key": None}, partitioner=partitioner)


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
        source.get_batch(asset.build_batch_request())


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
    ).add_query_asset(name="my_asset", query="select * from table").add_batch_definition(
        name="my_batch_definition", partitioner=ColumnPartitionerYearly(column_name="my_col")
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
        asset = source.add_query_asset(name="my_asset", query="select * from table")
        partitioner = ColumnPartitionerYearly(column_name="my_col")
        batch_request = asset.build_batch_request(partitioner=partitioner)
        batches = source.get_batch_identifiers_list(batch_request)
        batch = source.get_batch(batch_request)
        assert len(batches) == len(years)
        for i, year in enumerate(years):
            assert "year" in batches[i]
            assert batches[i]["year"] == year

        assert len(batch_specs) == 1
        for spec in batch_specs:
            assert "partitioner_method" in spec
            assert spec["partitioner_method"] == "partition_on_year"
        assert batch.metadata == {"year": 2021}


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
        asset = source.add_query_asset(name="my_asset", query="select * from table")
        partitioner = ColumnPartitionerMonthly(column_name="my_col")
        batch_request = asset.build_batch_request(partitioner=partitioner)
        batch_identifiers = source.get_batch_identifiers_list(batch_request)
        batch = source.get_batch(batch_request)
        assert len(batch_identifiers) == len(years) * len(months)
        for i, year in enumerate(years):
            for j, month in enumerate(months):
                batch_index = i * len(months) + j
                assert "year" in batch_identifiers[batch_index]
                assert "month" in batch_identifiers[batch_index]
                assert batch_identifiers[batch_index]["year"] == year
                assert batch_identifiers[batch_index]["month"] == month

        assert len(batch_identifiers) == len(years) * len(months)
        assert len(batch_specs) == 1
        assert "partitioner_method" in batch_specs[0]
        assert batch_specs[0]["partitioner_method"] == "partition_on_year_and_month"
        assert batch.metadata == {"year": 2021, "month": 9}


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
        )
        partitioner = ColumnPartitionerDaily(column_name="my_col")
        batch_request = asset.build_batch_request(partitioner=partitioner)
        batches = source.get_batch_identifiers_list(batch_request)
        batch = source.get_batch(batch_request)
        assert len(batches) == len(years) * len(months) * len(days)
        for i, year in enumerate(years):
            for j, month in enumerate(months):
                for k, day in enumerate(days):
                    batch_index = i * len(months) * len(days) + j * len(days) + k
                    assert "year" in batches[batch_index]
                    assert "month" in batches[batch_index]
                    assert "day" in batches[batch_index]
                    assert batches[batch_index]["year"] == year
                    assert batches[batch_index]["month"] == month
                    assert batches[batch_index]["day"] == day

        assert len(batch_specs) == 1
        assert "partitioner_method" in batch_specs[0]
        assert batch_specs[0]["partitioner_method"] == "partition_on_year_and_month_and_day"
        assert batch.metadata == {"year": 2021, "month": 9, "day": 30}


@pytest.mark.parametrize(
    ("sort_ascending", "expected_metadata"), [(True, {"year": 2021}), (False, {"year": 2020})]
)
@pytest.mark.postgresql
def test_get_batch_partitioner__sort_ascending_respected(
    empty_data_context,
    create_source: CreateSourceFixture,
    sort_ascending: bool,
    expected_metadata: dict,
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
            name="my_asset",
            query="select * from table",
        )
        partitioner = ColumnPartitionerYearly(column_name="my_col", sort_ascending=sort_ascending)
        batch_request = asset.build_batch_request(partitioner=partitioner)
        batch = source.get_batch(batch_request)
        assert batch.metadata == expected_metadata


@pytest.mark.postgresql
def test_get_batch_raises_if_no_batches_available(
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
        partitioner_query_response=[],
    ) as source:
        # We use a query asset because then we don't have to mock out db connection tests
        # in this unit test.
        asset = source.add_query_asset(
            name="my_asset",
            query="select * from table",
        )
        partitioner = ColumnPartitionerYearly(column_name="my_col")
        batch_request = asset.build_batch_request(partitioner=partitioner, options={"year": 1995})
        with pytest.raises(NoAvailableBatchesError):
            source.get_batch(batch_request)


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
            ColumnPartitionerYearly,
            {"column_name": "pickup_datetime"},
            [{"year": 2020}],
            1,
            {"year": 2020},
            1,
            {"year": 2020},
            id="year",
        ),
        pytest.param(
            ColumnPartitionerMonthly,
            {"column_name": "pickup_datetime"},
            [{"year": 2020, "month": 1}, {"year": 2020, "month": 2}],
            2,
            {"year": 2020, "month": 1},
            1,
            {"year": 2020, "month": 1},
            id="year_and_month",
        ),
        pytest.param(
            ColumnPartitionerDaily,
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
        pytest.param(
            PartitionerDatetimePart,
            {
                "column_name": "pickup_datetime",
                "datetime_parts": ["year", "month", "day"],
            },
            [
                {"year": 2020, "month": 2, "day": 10},
                {"year": 2020, "month": 2, "day": 12},
            ],
            2,
            {"year": 2020, "month": 2},
            2,
            {"year": 2020, "month": 2, "day": 12},
            id="datetime_part",
        ),
        pytest.param(
            PartitionerColumnValue,
            {"column_name": "passenger_count"},
            [(1,), (None,), (2,)],
            3,
            {"passenger_count": 2},
            1,
            {"passenger_count": 2},
            id="column_value",
        ),
        pytest.param(
            PartitionerDividedInteger,
            {"column_name": "passenger_count", "divisor": 3},
            [(1,), (2,)],
            2,
            {"quotient": 2},
            1,
            {"quotient": 2},
            id="divisor",
        ),
        pytest.param(
            PartitionerModInteger,
            {"column_name": "passenger_count", "mod": 3},
            [(1,), (2,)],
            2,
            {"remainder": 2},
            1,
            {"remainder": 2},
            id="mod_integer",
        ),
        pytest.param(
            PartitionerMultiColumnValue,
            {"column_names": ["passenger_count", "payment_type"]},
            # These types are (passenger_count, payment_type), that is in column_names order.
            # datetime partitioners return dicts while all other partitioners return tuples.
            [(3, 1), (1, 1), (1, 2)],
            3,
            {"passenger_count": 1},
            2,
            {"passenger_count": 1, "payment_type": 2},
            id="multi_column_values",
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
        # Test getting all batch itentifiers
        all_batches = asset.get_batch_identifiers_list(
            asset.build_batch_request(partitioner=partitioner)
        )
        assert len(all_batches) == all_batches_cnt
        # Test getting specified batches
        batch_request = asset.build_batch_request(specified_batch_request, partitioner=partitioner)
        specified_batches = asset.get_batch_identifiers_list(batch_request)
        assert len(specified_batches) == specified_batch_cnt
        assert asset.get_batch(batch_request).metadata == last_specified_batch_metadata


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
        partitioner = ColumnPartitionerYearly(column_name="my_col", sort_ascending=False)
        batch_request = asset.build_batch_request(partitioner=partitioner)
        batches = source.get_batch_identifiers_list(batch_request)
        assert len(batches) == len(years)
        assert asset.get_batch(batch_request).metadata["year"] is None


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
        _ = asset.get_batch(asset.build_batch_request())
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
        )
        assert asset.batch_metadata == asset_specified_metadata
        partitioner = ColumnPartitionerYearly(column_name="col")
        batches = source.get_batch_identifiers_list(
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
            assert batches[i] == substituted_batch_metadata


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
        )
        assert asset.batch_metadata == asset_specified_metadata
        partitioner = ColumnPartitionerYearly(column_name="my_col")
        batches = source.get_batch_identifiers_list(
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
            assert batches[i] == substituted_batch_metadata
