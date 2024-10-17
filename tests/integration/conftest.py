from __future__ import annotations

import random
import string
from abc import ABC, abstractmethod
from collections.abc import Generator, Mapping
from dataclasses import dataclass
from functools import cached_property
from typing import Any, Callable, Generic, Optional, Sequence, TypeVar

import pandas as pd
import pytest

import great_expectations as gx
from great_expectations.compatibility.typing_extensions import override
from great_expectations.data_context.data_context.abstract_data_context import AbstractDataContext
from great_expectations.datasource.fluent.interfaces import Batch

_F = TypeVar("_F", bound=Callable)


def parameterize_batch_for_data_sources(
    data_source_configs: Sequence[DataSourceConfig],
    data: pd.DataFrame,
    description: Optional[str] = None,
) -> Callable[[_F], _F]:
    """Test decorator that parametrizes a test function with batches for various data sources.

    This injects a `batch_for_datasource` parameter into the test function for each data source
    type.

    example use:
        @parameterize_batch_for_data_sources(
            data_source_configs=[DataSourceType.FOO, DataSourceType.BAR],
            data=[1, 2],
            # description="test_stuff",
        )
        def test_stuff(batch_for_datasource) -> None:
            ...
    """

    def decorator(func: _F) -> _F:
        pytest_params = [
            pytest.param(
                (data, t),
                id=t.get_test_id(description),
                marks=[_data_source_config_to_mark(t)],
            )
            for t in data_source_configs
        ]
        parameterize_decorator = pytest.mark.parametrize(
            batch_for_datasource.__name__,
            pytest_params,
            indirect=True,
        )
        return parameterize_decorator(func)

    return decorator


@pytest.fixture
def batch_for_datasource(request: pytest.FixtureRequest) -> Generator[Batch, None, None]:
    """Fixture that yields a batch for a specific data source type.

    This must be used in conjunction with `indirect=True` to defer execution
    """
    data, data_source_config = request.param
    assert isinstance(data, pd.DataFrame)
    assert isinstance(data_source_config, DataSourceConfig)

    batch_setup = data_source_config.create_batch_setup(data)

    batch_setup.setup()
    yield batch_setup.make_batch()
    batch_setup.teardown()


def _data_source_config_to_mark(data_source_config: DataSourceConfig) -> pytest.MarkDecorator:
    """Get the appropriate mark for a data source type."""
    if isinstance(data_source_config, PandasDataFrameDatasource):
        return pytest.mark.unit
    elif isinstance(data_source_config, PandasFilesystemDatasource):
        return pytest.mark.filesystem
    elif isinstance(data_source_config, PostgresDataSource):
        return pytest.mark.postgresql
    elif isinstance(data_source_config, SnowflakeDataSource):
        return pytest.mark.snowflake
    else:
        assert False


# === DataSource config section ===
@dataclass(frozen=True)
class DataSourceConfig(ABC):
    name: str | None = None

    @property
    @abstractmethod
    def label(self) -> str:
        """Label that will show up in test name."""
        ...

    @abstractmethod
    def create_batch_setup(self, data: list) -> BatchSetup:
        """Create a batch setup object for this data source."""

    def get_test_id(self, test_description: str | None) -> str:
        parts: list[Optional[str]] = [test_description, self.label, self.name]
        non_null_parts = [p for p in parts if p is not None]

        return "-".join(non_null_parts)


@dataclass(frozen=True)
class _SqlDataSourceConfig(DataSourceConfig):
    columns: Mapping[str, Any] | None = None  # TODO: obvs lock down the value type here


class PandasDataFrameDatasource(DataSourceConfig):
    @property
    @override
    def label(self) -> str:
        return "PandasDataFrameDatasource"

    @override
    def create_batch_setup(self, data: list) -> BatchSetup:
        return PandasDataFrameBatchSetup(data=data, config=self)


class PandasFilesystemDatasource(DataSourceConfig):
    @property
    @override
    def label(self) -> str:
        return "PandasFilesystemDatasource"

    @override
    def create_batch_setup(self, data: list) -> BatchSetup:
        return NotImplemented


class PostgresDataSource(_SqlDataSourceConfig):
    @property
    @override
    def label(self) -> str:
        return "PostgresDatasource"

    @override
    def create_batch_setup(self, data: list) -> BatchSetup:
        return PostgresBatchSetup(data=data, config=self)


class SnowflakeDataSource(_SqlDataSourceConfig):
    @property
    @override
    def label(self) -> str:
        return "SnowflakeDatasource"

    @override
    def create_batch_setup(self, data: list) -> BatchSetup:
        return NotImplemented


# === Batch Setup Classes section ===
_ConfigT = TypeVar("_ConfigT", bound=DataSourceConfig)


class BatchSetup(ABC, Generic[_ConfigT]):
    """ABC for classes that set up and tear down batches."""

    def __init__(self, config: _ConfigT, data: list) -> None:
        self.config = config
        self.data = data

    @abstractmethod
    def make_batch(self) -> Batch: ...

    @abstractmethod
    def setup(self) -> None: ...

    @abstractmethod
    def teardown(self) -> None:
        pass

    @staticmethod
    def _random_resource_name() -> str:
        return "".join(random.choices(string.ascii_lowercase, k=10))


_foo_cache: dict[int, pd.DataFrame] = {}


class PandasDataFrameBatchSetup(BatchSetup[PandasDataFrameDatasource]):
    @override
    def make_batch(self) -> Batch:
        name = self._random_resource_name()
        return (
            self._context.data_sources.add_pandas(name)
            .add_dataframe_asset(name)
            .add_batch_definition_whole_dataframe(name)
            .get_batch(batch_parameters={"dataframe": self.data})
        )

    @override
    def setup(self) -> None: ...

    @override
    def teardown(self) -> None: ...

    @cached_property
    def _context(self) -> AbstractDataContext:
        return gx.get_context(mode="ephemeral")


class PostgresBatchSetup(BatchSetup[PostgresDataSource]):
    @override
    def make_batch(self) -> Batch:
        explicit_column_types = self.config.columns  # noqa: F841
        inferred_column_types = self._inferred_column_types  # noqa: F841

        # *waves hands* set schema based on ^
        # *waves hands* load data

        raise NotImplementedError

    @override
    def setup(self) -> None: ...

    @override
    def teardown(self) -> None: ...

    @property
    def _inferred_column_types(self) -> Mapping[str, Any]:
        # *waves hands* infers column types from data
        return {}
