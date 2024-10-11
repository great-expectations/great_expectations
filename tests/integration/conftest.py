import enum
import random
import string
from abc import ABC, abstractmethod
from collections.abc import Generator, Mapping
from functools import cached_property
from typing import Callable, Optional, TypeVar

import pandas as pd
import pytest

import great_expectations as gx
from great_expectations.compatibility.typing_extensions import override
from great_expectations.data_context.data_context.abstract_data_context import AbstractDataContext
from great_expectations.datasource.fluent.interfaces import Batch

F = TypeVar("F", bound=Callable)


class DataSourceType(str, enum.Enum):
    FOO = "foo"
    BAR = "bar"


def parameterize_batch_for_data_sources(
    types: list[DataSourceType], data: pd.DataFrame, description: Optional[str] = None
) -> Callable[[F], F]:
    """Test decorator that parametrizes a test function with batches for various data sources.

    This injects a `batch_for_datasource` parameter into the test function for each data source
    type.

    example use:
        @parameterize_batch_for_data_sources(
            types=[DataSourceType.FOO, DataSourceType.BAR],
            data=[1, 2],
            # description="test_stuff",
        )
        def test_stuff(batch_for_datasource) -> None:
            ...
    """

    def decorator(func: F) -> F:
        pytest_params = [
            pytest.param(
                (data, t),
                id=f"{description}-{t.value}" if description else t,
                marks=[_data_source_type_to_mark(t)],
            )
            for t in types
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
    data, data_source_type = request.param
    batch_setup_cls = data_source_to_batch_setup[data_source_type]
    batch_setup = batch_setup_cls(data)

    batch_setup.setup()
    yield batch_setup.make_batch()
    batch_setup.teardown()


def _data_source_type_to_mark(type: DataSourceType) -> pytest.MarkDecorator:
    """Get the appropriate mark for a data source type."""
    if type in {DataSourceType.FOO}:
        return pytest.mark.unit
    elif type in {DataSourceType.BAR}:
        return pytest.mark.cloud
    else:
        assert False


class BatchSetup(ABC):
    def __init__(self, data: list) -> None:
        self.data = data

    @abstractmethod
    def make_batch(self) -> Batch: ...

    @abstractmethod
    def setup(self) -> None: ...

    @abstractmethod
    def teardown(self) -> None:
        pass


_foo_cache: dict[int, pd.DataFrame] = {}


class FooBatchSetup(BatchSetup):
    @override
    def make_batch(self) -> Batch:
        name = _random_resource_name()
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


class BarBatchSetup(BatchSetup):
    @override
    def make_batch(self) -> Batch:
        raise NotImplementedError

    @override
    def setup(self) -> None: ...

    @override
    def teardown(self) -> None: ...


data_source_to_batch_setup: Mapping[DataSourceType, type[BatchSetup]] = {
    DataSourceType.FOO: FooBatchSetup,
    DataSourceType.BAR: BarBatchSetup,
}


def _random_resource_name() -> str:
    return "".join(random.choices(string.ascii_lowercase, k=10))
