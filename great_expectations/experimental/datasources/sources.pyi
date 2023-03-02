from typing import Any, ClassVar, List, NamedTuple, Type, Union

from _typeshed import Incomplete
from typing_extensions import Final

from great_expectations.data_context import (
    AbstractDataContext as GXDataContext,  # noqa: TCH001
)
from great_expectations.datasource import (
    BaseDatasource as BaseDatasource,
)
from great_expectations.datasource import (
    LegacyDatasource as LegacyDatasource,
)
from great_expectations.experimental.context import DataContext as DataContext
from great_expectations.experimental.datasources import (
    PandasDatasource as PandasDatasource,
)
from great_expectations.experimental.datasources.interfaces import (
    DataAsset as DataAsset,
)
from great_expectations.experimental.datasources.interfaces import (
    Datasource as Datasource,
)
from great_expectations.experimental.datasources.type_lookup import (
    TypeLookup as TypeLookup,
)
from great_expectations.validator.validator import Validator as Validator

SourceFactoryFn: Incomplete
logger: Incomplete
DEFAULT_PANDAS_DATASOURCE_NAME: Final[str]
DEFAULT_PANDAS_DATA_ASSET_NAME: Final[str]

class DefaultPandasDatasourceError(Exception): ...
class TypeRegistrationError(TypeError): ...

class _FieldDetails(NamedTuple):
    default_value: Any
    type_annotation: Type

class _SourceFactories:
    type_lookup: ClassVar
    def __init__(self, data_context: Union[DataContext, GXDataContext]) -> None: ...
    @classmethod
    def register_types_and_ds_factory(
        cls, ds_type: Type[Datasource], factory_fn: SourceFactoryFn
    ) -> None: ...
    @property
    def pandas_default(self) -> PandasDatasource: ...
    @property
    def factories(self) -> List[str]: ...
    def __getattr__(self, attr_name: str): ...
    def __dir__(self) -> List[str]: ...
