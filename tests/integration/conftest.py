import enum
from abc import ABC, abstractmethod
from collections.abc import Generator, Mapping
from typing import Any, Callable, Optional, TypeVar

import pytest

from great_expectations.compatibility.typing_extensions import override

F = TypeVar("F", bound=Callable)


class DataSourceType(str, enum.Enum):
    FOO = "foo"
    BAR = "bar"


def parameterize_batch_for_data_sources(
    types: list[DataSourceType], data: list[int], description: Optional[str] = None
) -> Callable[[F], F]:
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
def batch_for_datasource(request: pytest.FixtureRequest) -> Generator[Any, None, None]:
    data, data_source_type = request.param
    batch_setup_cls = data_source_to_batch_setup[data_source_type]
    batch_setup = batch_setup_cls(data)

    batch_setup.setup()
    yield batch_setup.make_batch()
    batch_setup.teardown()


def _data_source_type_to_mark(type: DataSourceType) -> pytest.MarkDecorator:
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
    def make_batch(self) -> list:  # TODO: return Batch
        ...

    @abstractmethod
    def setup(self) -> None: ...

    @abstractmethod
    def teardown(self) -> None:
        pass


class FooBatchSetup(BatchSetup):
    @override
    def make_batch(self) -> list:
        return [*self.data, 3]

    @override
    def setup(self) -> None: ...

    @override
    def teardown(self) -> None: ...


class BarBatchSetup(BatchSetup):
    @override
    def make_batch(self) -> list:
        return [*self.data, 3, 4]

    @override
    def setup(self) -> None: ...

    @override
    def teardown(self) -> None: ...


data_source_to_batch_setup: Mapping[DataSourceType, type[BatchSetup]] = {
    DataSourceType.FOO: FooBatchSetup,
    DataSourceType.BAR: BarBatchSetup,
}
