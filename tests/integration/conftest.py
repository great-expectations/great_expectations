import logging
import pprint
from dataclasses import dataclass
from typing import Callable, Generator, Mapping, Optional, Sequence, TypeVar, Union
from uuid import UUID

import pandas as pd
import pytest
from _pytest.mark import MarkDecorator

import great_expectations as gx
from great_expectations.compatibility.typing_extensions import override
from great_expectations.data_context.data_context.context_factory import set_context
from great_expectations.datasource.fluent.interfaces import Batch, DataAsset
from tests.integration.sql_session_manager import SessionSQLEngineManager
from tests.integration.test_utils.data_source_config import DataSourceTestConfig
from tests.integration.test_utils.data_source_config.base import (
    BatchTestSetup,
    dict_to_tuple,
    hash_data_frame,
)
from tests.integration.test_utils.data_source_config.sql import (
    SQLBatchTestSetup,
)

logger = logging.getLogger(__name__)

_F = TypeVar("_F", bound=Callable)


@dataclass(frozen=True)
class TestConfig:
    data_source_config: DataSourceTestConfig
    data: pd.DataFrame
    extra_data: Mapping[str, pd.DataFrame]
    secondary_source_config: Union[DataSourceTestConfig, None] = None
    secondary_data: Union[pd.DataFrame, None] = None

    @override
    def __hash__(self) -> int:
        if self.secondary_data is None and self.secondary_source_config is not None:
            raise ValueError(
                "secondary_data cannot be None when secondary_source_config is provided"
            )
        return hash(
            (
                self.__class__,
                hash_data_frame(self.data),
                dict_to_tuple(
                    {k: hash_data_frame(self.extra_data[k]) for k in sorted(self.extra_data)}
                ),
                hash_data_frame(self.secondary_data) if self.secondary_data is not None else None,
            )
        )

    @override
    def __eq__(self, value: object) -> bool:
        # We need to implement this ourselves to call `.equals` on dataframes.`
        if not isinstance(value, TestConfig):
            return False
        return all(
            [
                self.data_source_config == value.data_source_config,
                self.data.equals(value.data),
                self.extra_data.keys() == value.extra_data.keys(),
                all(self.extra_data[k].equals(value.extra_data[k]) for k in self.extra_data),
                self.secondary_source_config == value.secondary_source_config,
                (self.secondary_data is None and value.secondary_data is None)
                or (
                    self.secondary_data is not None
                    and value.secondary_data is not None
                    and self.secondary_data.equals(value.secondary_data)
                ),
            ]
        )


def parameterize_batch_for_data_sources(
    data_source_configs: Sequence[DataSourceTestConfig],
    data: pd.DataFrame,
    extra_data: Optional[Mapping[str, pd.DataFrame]] = None,
) -> Callable[[_F], _F]:
    """Test decorator that parametrizes a test function with batches for various data sources.
    This injects a `batch_for_datasource` parameter into the test function for each data source
    type.

    Args:
        data_source_configs: The data source configurations to test.
        data: Data to load into the asset
        extra_data: Mapping of {asset_label: data} to load into other assets. Only relevant for SQL
                    multi-table expectations. NOTE: This is NOT the table name. The label is used to
                    correlate the data with the types passed to
                    DataSourceTestConfig.extra_column_types.


    example use:
        @parameterize_batch_for_data_sources(
            data_source_configs=[DataSourceType.FOO, DataSourceType.BAR],
            data=pd.DataFrame{"col_name": [1, 2]},
            # description="test_stuff",
        )
        def test_stuff(batch_for_datasource) -> None:
            ...
    """
    if len(data_source_configs) == 0:
        raise ValueError("At least one data source configuration must be provided.")

    def decorator(func: _F) -> _F:
        pytest_params = [
            pytest.param(
                TestConfig(
                    data_source_config=config,
                    data=data,
                    extra_data=extra_data or {},
                ),
                id=config.test_id,
                marks=[config.pytest_mark],
            )
            for config in data_source_configs
        ]
        parameterize_decorator = pytest.mark.parametrize(
            _batch_setup_for_datasource.__name__,
            pytest_params,
            indirect=True,
        )
        return parameterize_decorator(func)

    return decorator


# NOTE on performance setup/teardown:
# When we get equivalent TestConfigs, we only instantiate one BatchTestSetup for all of them, and
# only perform its setup/teardown once. batch_for_datasource instantiate the BatchTestSetup
# immediately before the first test that needs it and store it in cached_test_configs. Subsequent
# tests that use the same TestConfig will reuse the same BatchTestSetup. At the end of the test
# session, _cleanup will clean up all the BatchTestSetups.


@pytest.fixture(scope="session")
def _cached_test_configs() -> dict[TestConfig, BatchTestSetup]:
    """Fixture to hold cached test configurations across tests."""
    cached_test_configs: dict[TestConfig, BatchTestSetup] = {}

    return cached_test_configs


@pytest.fixture(scope="session")
def _cached_secondary_test_configs() -> dict[UUID, BatchTestSetup]:
    """Fixture to hold secondary test configurations across tests."""
    cached_test_configs: dict[UUID, BatchTestSetup] = {}
    return cached_test_configs


@pytest.fixture(scope="session")
def _cleanup(
    _cached_test_configs: Mapping[TestConfig, BatchTestSetup],
    _cached_secondary_test_configs: Mapping[TestConfig, BatchTestSetup],
    # While not explicitly used, we want the session_sql_engine_manager to
    # be torn down after we clean up. Adding it as a dependeny will ensure
    # this.
    session_sql_engine_manager: SessionSQLEngineManager,
) -> Generator[None, None, None]:
    """Fixture to do all teardown at the end of the test session."""
    yield
    # Attempt every teardown even when one fails: stopping at the first failure would leave
    # every remaining setup's resources (e.g. schemas) behind on the backend.
    failures: list[BaseException] = []
    for batch_setup in [
        *_cached_test_configs.values(),
        *_cached_secondary_test_configs.values(),
    ]:
        try:
            batch_setup.teardown()
        except Exception as e:
            logger.exception(f"Teardown failed for {type(batch_setup).__name__}")
            failures.append(e)
    if failures:
        raise RuntimeError(f"{len(failures)} batch setup teardown(s) failed") from failures[0]


@pytest.fixture
def _batch_setup_for_datasource(
    request: pytest.FixtureRequest,
    _cached_test_configs: dict[TestConfig, BatchTestSetup],
    _cached_secondary_test_configs: dict[UUID, BatchTestSetup],
    session_sql_engine_manager: SessionSQLEngineManager,
    # _cleanup is not called directly. It is a session scoped fixture
    # which will cleanup created db resources such as schemas.
    _cleanup: Callable[[], None],
) -> Generator[BatchTestSetup, None, None]:
    """Fixture that yields a BatchSetup for a specific data source type.
    This must be used in conjunction with `indirect=True` to defer execution
    """
    config = request.param
    assert isinstance(config, TestConfig)

    if config not in _cached_test_configs:
        batch_setup = config.data_source_config.create_batch_setup(
            request=request,
            data=config.data,
            extra_data=config.extra_data,
            context=gx.get_context(mode="ephemeral"),
            engine_manager=session_sql_engine_manager,
        )
        _cached_test_configs[config] = batch_setup
        batch_setup.setup()
        if config.secondary_source_config:
            assert config.secondary_data is not None, (
                "Secondary data is required when secondary config is provided."
            )
            secondary_batch_setup = config.secondary_source_config.create_batch_setup(
                request=request,
                data=config.secondary_data,
                extra_data={},
                context=batch_setup.context,
                engine_manager=session_sql_engine_manager,
            )
            _cached_secondary_test_configs[batch_setup.id] = secondary_batch_setup
            secondary_batch_setup.setup()

    yield _cached_test_configs[config]


@pytest.fixture
def batch_for_datasource(
    _batch_setup_for_datasource: BatchTestSetup,
) -> Generator[Batch, None, None]:
    """Fixture that yields a batch for a specific data source type.
    This must be used in conjunction with `indirect=True` to defer execution
    """
    set_context(_batch_setup_for_datasource.context)
    yield _batch_setup_for_datasource.make_batch()


@pytest.fixture
def asset_for_datasource(
    _batch_setup_for_datasource: BatchTestSetup,
) -> Generator[DataAsset, None, None]:
    """Fixture that yields an asset for a specific data source type.
    This must be used in conjunction with `indirect=True` to defer execution
    """
    set_context(_batch_setup_for_datasource.context)
    yield _batch_setup_for_datasource.make_asset()


@dataclass(frozen=True)
class MultiSourceBatch:
    base_batch: Batch
    comparison_data_source_name: str
    comparison_table_name: str


@pytest.fixture
def multi_source_batch(
    _batch_setup_for_datasource: BatchTestSetup,
    _cached_secondary_test_configs: dict[UUID, BatchTestSetup],
) -> Generator[MultiSourceBatch, None, None]:
    """Fixture that sets up multiple sources in a single data context."""
    secondary_batch_setup = _cached_secondary_test_configs[_batch_setup_for_datasource.id]
    assert isinstance(secondary_batch_setup, SQLBatchTestSetup), (
        "MultiSourceBatch requires SQLBatchTestSetup"
    )
    # we need a data source, so we use the make_asset API:
    set_context(_batch_setup_for_datasource.context)
    secondary_asset = secondary_batch_setup.make_asset()
    yield MultiSourceBatch(
        base_batch=_batch_setup_for_datasource.make_batch(),
        comparison_data_source_name=secondary_asset.datasource.name,
        comparison_table_name=secondary_batch_setup.table_name,
    )


@pytest.fixture
def schema_name_for_datasource(
    _batch_setup_for_datasource: BatchTestSetup,
) -> Generator[str | None, None, None]:
    """Fixture that yields extra table names"""
    assert isinstance(_batch_setup_for_datasource, SQLBatchTestSetup)
    yield _batch_setup_for_datasource.schema


@pytest.fixture
def extra_table_names_for_datasource(
    _batch_setup_for_datasource: BatchTestSetup,
) -> Generator[Mapping[str, str], None, None]:
    assert isinstance(_batch_setup_for_datasource, SQLBatchTestSetup)
    yield {key: t.name for key, t in _batch_setup_for_datasource.extra_table_data.items()}


@pytest.fixture
def fully_qualified_extra_table_names_for_datasource(
    extra_table_names_for_datasource: Mapping[str, str],
    schema_name_for_datasource: str | None,
) -> Generator[Mapping[str, str], None, None]:
    """Fixture that yields extra table names"""
    if schema_name_for_datasource:
        yield {
            key: f"{schema_name_for_datasource}.{table_name}"
            for key, table_name in extra_table_names_for_datasource.items()
        }
    else:
        yield extra_table_names_for_datasource


@pytest.fixture(scope="session")
def _base_to_comparison_map() -> Mapping[UUID, UUID]:
    """Get a comparison BatchTestSetup ID by its base BatchTestSetup ID."""
    return {}


@dataclass(frozen=True)
class MultiSourceTestConfig:
    comparison: DataSourceTestConfig
    base: DataSourceTestConfig


def multi_source_batch_setup(
    multi_source_test_configs: list[MultiSourceTestConfig],
    base_data: pd.DataFrame,
    comparison_data: pd.DataFrame,
) -> Callable[[_F], _F]:
    def decorator(func: _F) -> _F:
        pytest_params = []
        for multi_source_test_config in multi_source_test_configs:
            pytest_params.append(
                pytest.param(
                    TestConfig(
                        data_source_config=multi_source_test_config.base,
                        data=base_data,
                        extra_data={},
                        secondary_source_config=multi_source_test_config.comparison,
                        secondary_data=comparison_data,
                    ),
                    id=f"{multi_source_test_config.comparison.test_id}->{multi_source_test_config.base.test_id}",
                    marks=_get_multi_source_marks(multi_source_test_config),
                )
            )
        parameterize_decorator = pytest.mark.parametrize(
            _batch_setup_for_datasource.__name__,
            pytest_params,
            indirect=True,
        )
        return parameterize_decorator(func)

    return decorator


def _get_multi_source_marks(multi_source_test_config: MultiSourceTestConfig) -> list[MarkDecorator]:
    if multi_source_test_config.base.pytest_mark == multi_source_test_config.comparison.pytest_mark:
        return [multi_source_test_config.base.pytest_mark]
    # our test setup restricts us to testing a single backend at a time.
    # sqlite doesn't require any extra setup, so it's an exception.
    marks = [
        mark
        for mark in [
            multi_source_test_config.comparison.pytest_mark,
            multi_source_test_config.base.pytest_mark,
        ]
        if mark != pytest.mark.sqlite
    ]
    if len(marks) == 1:
        return marks
    elif not marks:
        return [pytest.mark.sqlite]
    else:
        raise ValueError(
            "MultiSourceBatch tests must either use the same backend or include sqlite."
        )


@pytest.fixture(scope="session")
def session_sql_engine_manager():
    logger.info("SessionSqlEngineManager: Starting setup.")
    manager = SessionSQLEngineManager()
    yield manager

    logger.info("SessionSqlEngineManager: Starting teardown.")
    pre_cleanup_stats = manager.get_all_pool_statistics()
    # We temporarily log a warning so we can see this in the pytest output without turning on info
    # logging across the whole test run.
    logger.warning(
        "SessionSqlEngineManager: Pool statistics before explicit cleanup:\n"
        f"{pprint.pformat(pre_cleanup_stats)}"
    )
    # Check for any immediately obvious issues before cleanup
    for key, stat in pre_cleanup_stats.items():
        if "error" not in stat and stat.get("checked_out", 0) > 0:
            logger.warning(
                f"SessionSqlEngineManager: Engine {key} has {stat['checked_out']} connections "
                "still checked out BEFORE manager disposal."
            )
    manager.dispose_all_engines()
    logger.info("SessionSqlEngineManager: All engines disposed by manager.")
    assert not manager._engine_cache, "Engine cache should be empty after dispose_all_engines."
    logger.info("SessionSqlEngineManager: Teardown complete.")
