import logging
from typing import Tuple, Type, Union

import pytest
from pytest import MonkeyPatch

from great_expectations.core.batch import BatchData
from great_expectations.core.batch_spec import BatchMarkers
from great_expectations.execution_engine import (
    ExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.zep.sources import _SourceFactories

LOGGER = logging.getLogger(__name__)


class ExecutionEngineDouble:
    def __init__(self, *args, **kwargs):
        pass

    def get_batch_data_and_markers(self, batch_spec) -> Tuple[BatchData, BatchMarkers]:
        return BatchData(self), BatchMarkers(ge_load_time=None)


@pytest.fixture
def inject_engine_lookup_double(monkeypatch: MonkeyPatch) -> ExecutionEngineDouble:  # type: ignore[misc]
    """
    Inject an execution engine test double into the _SourcesFactory.engine_lookup
    so that all Datasources use the execution engine double.
    Dynamically create a new subclass so that runtime type validation does not fail.
    """
    LOGGER.info(
        f"Patching `_SourceFactories.engine_lookup` to return {ExecutionEngineDouble.__name__}"
    )
    key: Union[str, Type[ExecutionEngine]]
    value: Type[ExecutionEngine]
    for key, value in _SourceFactories.engine_lookup.items():
        if isinstance(key, str):
            engine_double_cls = type(  # TODO: make sure order of bases is correct
                f"{key.capitalize()}ExecutionEngineDouble",
                (ExecutionEngineDouble, value),
                {},
            )
            monkeypatch.setitem(
                _SourceFactories.engine_lookup.data, key, engine_double_cls
            )
            LOGGER.info(
                f"patched '{key}' -BEFORE-> {value.__name__} -AFTER-> {engine_double_cls.__name__}"
            )
    yield ExecutionEngineDouble
