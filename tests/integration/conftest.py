from typing import Callable, Generator, Sequence, TypeVar

import pandas as pd
import pytest

from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.test_utils.data_source_config import DataSourceTestConfig

_F = TypeVar("_F", bound=Callable)


def parameterize_batch_for_data_sources(
    data_source_configs: Sequence[DataSourceTestConfig],
    data: pd.DataFrame,
) -> Callable[[_F], _F]:
    """Test decorator that parametrizes a test function with batches for various data sources.
    This injects a `batch_for_datasource` parameter into the test function for each data source
    type.
    example use:
        @parameterize_batch_for_data_sources(
            data_source_configs=[DataSourceType.FOO, DataSourceType.BAR],
            data=pd.DataFrame{"col_name": [1, 2]},
            # description="test_stuff",
        )
        def test_stuff(batch_for_datasource) -> None:
            ...
    """

    def decorator(func: _F) -> _F:
        pytest_params = [
            pytest.param(
                (data, t),
                id=t.test_id,
                marks=[t.pytest_mark],
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
    assert isinstance(data_source_config, DataSourceTestConfig)

    batch_setup = data_source_config.create_batch_setup(data)

    batch_setup.setup()
    yield batch_setup.make_batch()
    batch_setup.teardown()
