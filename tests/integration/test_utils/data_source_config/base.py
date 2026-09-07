from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass, field
from functools import cached_property
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Generator,
    Generic,
    Hashable,
    Mapping,
    Optional,
    TypeVar,
)
from uuid import UUID, uuid4

import pandas as pd

from great_expectations.compatibility.typing_extensions import override
from great_expectations.datasource.fluent.interfaces import Batch, DataAsset

if TYPE_CHECKING:
    import pytest
    from pytest import FixtureRequest

    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )
    from tests.integration.test_utils.data_source_config.data_source_spec import DataSourceSpec
    from tests.integration.test_utils.data_source_config.sql import SessionSQLEngineManager


_ColumnTypes = TypeVar("_ColumnTypes")


@dataclass(frozen=True)
class DataSourceTestConfig(ABC, Generic[_ColumnTypes]):
    name: Optional[str] = None
    table_name: Optional[str] = None  # Overrides random table name generation
    schema_name: Optional[str] = None  # Overrides random schema name generation
    column_types: Optional[Mapping[str, _ColumnTypes]] = None
    extra_column_types: Mapping[str, Mapping[str, _ColumnTypes]] = field(default_factory=dict)

    DATA_SOURCE_SPEC: ClassVar[DataSourceSpec]
    """The declaration a concrete config states once, describing what its data source is.

    Every data source states it the same way, whether or not it is a SQL backend, so the identity
    a test run sees is derived from one declaration rather than restated by hand next to it.

    Annotated `ClassVar` so the dataclass machinery treats it as a plain class attribute rather
    than a frozen-dataclass field: it takes no part in equality or hashing and is not an
    `__init__` parameter, which is what lets it be added without re-declaring any config or
    changing any config's constructor, equality, or hash.

    `ClassVar` must stay a runtime import in this module for that to hold. The dataclass
    machinery recognizes a class variable by resolving the name `ClassVar` in the defining
    module's namespace as the module runs; behind `TYPE_CHECKING` that name is absent there, the
    annotation reads as an ordinary field, and the declaration joins every config's constructor
    and generated field set. Here that surfaces as a `TypeError` at class creation, because the
    slot follows defaulted fields - loud in this arrangement, but it is the field set changing
    that matters, and a different field order would let the same mistake through quietly.
    """

    @property
    def data_source_spec(self) -> DataSourceSpec:
        """The declaration governing this instance.

        Resolution is a property rather than a direct read of the class attribute at each call
        site so that a config whose declaration varies per instance can override the resolution
        without this base carrying the seam. Only one config needs that variation - the
        connection-string escape hatch, whose identity depends on a value supplied at
        construction - and giving this base a per-instance override field for it would add a
        parameter to every config's constructor and force three further classes to re-declare
        themselves as dataclasses. A property costs an overriding subclass one method and costs
        every other subclass nothing.
        """
        return self.DATA_SOURCE_SPEC

    @property
    def label(self) -> str:
        """Label that will show up in test name."""
        return self.data_source_spec.label

    @property
    def pytest_mark(self) -> pytest.MarkDecorator:
        """Mark for pytest"""
        return self.data_source_spec.pytest_mark

    @abstractmethod
    def create_batch_setup(
        self,
        request: FixtureRequest,
        data: pd.DataFrame,
        extra_data: Mapping[str, pd.DataFrame],
        context: AbstractDataContext,
        # This violates the interface segration principle (the I in SOLID) since we now make
        # non-SQL datasources rely on an argument that only SQL datasources are need.
        # However, this is simpler than adding an additional layer to decouple this interface.
        # If the SQL and non-SQL test interfaces diverge more significantly we should consider
        # refactoring these tests.
        # One possible fix is to remove this method from this class and create a sql and
        # non-sql subclass. We'd like need to update _ConfigT to be bounded by a union of
        # these subclasses and update callers of create_batch_setup.
        engine_manager: Optional[SessionSQLEngineManager] = None,
    ) -> BatchTestSetup:
        """Create a batch setup object for this data source."""

    @property
    def test_id(self) -> str:
        parts: list[Optional[str]] = [self.label, self.name]
        non_null_parts = [p for p in parts if p is not None]

        return "-".join(non_null_parts)

    @override
    def __eq__(self, value: object) -> bool:
        if not isinstance(value, DataSourceTestConfig):
            return False
        return all(
            [
                super().__eq__(value),
                self.label == value.label,
                self.pytest_mark == value.pytest_mark,
            ]
        )

    @override
    def __hash__(self) -> int:
        hashable_col_types = dict_to_tuple(self.column_types) if self.column_types else None
        hashable_extra_col_types = dict_to_tuple(
            {k: dict_to_tuple(self.extra_column_types[k]) for k in sorted(self.extra_column_types)}
        )
        return hash(
            (
                self.__class__.name,
                self.test_id,
                hashable_col_types,
                hashable_extra_col_types,
            )
        )


_ConfigT = TypeVar("_ConfigT", bound=DataSourceTestConfig)
_AssetT = TypeVar("_AssetT", bound=DataAsset)


class BatchTestSetup(ABC, Generic[_ConfigT, _AssetT]):
    """ABC for classes that set up and tear down batches."""

    def __init__(self, config: _ConfigT, data: pd.DataFrame, context: AbstractDataContext) -> None:
        self.config = config
        self.data = data
        self.context = context

    @abstractmethod
    def make_asset(self) -> _AssetT: ...

    @abstractmethod
    def make_batch(self) -> Batch: ...

    @abstractmethod
    def setup(self) -> None: ...

    @abstractmethod
    def teardown(self) -> None: ...

    @contextmanager
    def data_context_test_context(self) -> Generator[AbstractDataContext, None, None]:
        """Receive a DataContext and ensure proper setup and teardown regardless of errors."""
        try:
            self.setup()
            yield self.context
        finally:
            self.teardown()

    @contextmanager
    def asset_test_context(self) -> Generator[_AssetT, None, None]:
        """Receive an Asset and ensure proper setup and teardown regardless of errors."""
        try:
            self.setup()
            yield self.make_asset()
        finally:
            self.teardown()

    @contextmanager
    def batch_test_context(self) -> Generator[Batch, None, None]:
        """Receive a Batch and ensure proper setup and teardown regardless of errors."""
        try:
            self.setup()
            yield self.make_batch()
        finally:
            self.teardown()

    @staticmethod
    def _random_resource_name() -> str:
        # Use uuid4 rather than the global `random` module: tests/conftest.py and
        # tests/execution_engine/conftest.py reseed `random` with a fixed seed,
        # which makes "random" names deterministic and prone to in-session collisions.
        return uuid4().hex[:10]

    @cached_property
    def id(self) -> UUID:
        return uuid4()


def dict_to_tuple(d: Mapping[str, Hashable]) -> tuple[tuple[str, Hashable], ...]:
    return tuple((key, d[key]) for key in sorted(d))


def hash_data_frame(df: pd.DataFrame) -> int:
    return hash(tuple(pd.util.hash_pandas_object(df).array))
