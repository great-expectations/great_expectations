from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, List, NamedTuple, Type, Union

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

if TYPE_CHECKING:
    import pathlib

    from pydantic.networks import PostgresDsn

    from great_expectations.experimental.datasources import (
        PandasAzureBlobStorageDatasource,
        PandasFilesystemDatasource,
        PandasGoogleCloudStorageDatasource,
        PandasS3Datasource,
        PostgresDatasource,
        SparkAzureBlobStorageDatasource,
        SparkFilesystemDatasource,
        SparkGoogleCloudStorageDatasource,
        SparkS3Datasource,
        SQLDatasource,
        SqliteDatasource,
    )
    from great_expectations.experimental.datasources.sqlite_datasource import SqliteDsn

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
    def add_pandas(self, name: str) -> PandasDatasource: ...
    def add_pandas_abs(
        self, name: str, *, azure_options: dict[str, Any] | None = None
    ) -> PandasAzureBlobStorageDatasource: ...
    def add_pandas_filesystem(
        self,
        name: str,
        *,
        base_directory: pathlib.Path,
        data_context_root_directory: pathlib.Path | None = None,
    ) -> PandasFilesystemDatasource: ...
    def add_pandas_gcs(
        self,
        name: str,
        *,
        bucket_or_name: str,
        gcs_options: dict[str, Any] | None = None,
    ) -> PandasGoogleCloudStorageDatasource: ...
    def add_pandas_s3(
        self, name: str, *, bucket: str, boto3_options: dict[str, Any] | None = None
    ) -> PandasS3Datasource: ...
    def add_postgres(
        self, name: str, *, connection_string: PostgresDsn
    ) -> PostgresDatasource: ...
    def add_spark_abs(
        self, name: str, *, azure_options: dict[str, Any] | None = None
    ) -> SparkAzureBlobStorageDatasource: ...
    def add_spark_filesystem(
        self,
        name: str,
        *,
        base_directory: pathlib.Path,
        data_context_root_directory: pathlib.Path | None = None,
    ) -> SparkFilesystemDatasource: ...
    def add_spark_gcs(
        self,
        name: str,
        *,
        bucket_or_name: str,
        gcs_options: dict[str, Any] | None = None,
    ) -> SparkGoogleCloudStorageDatasource: ...
    def add_spark_s3(
        self, name: str, *, bucket: str, boto3_options: dict[str, Any] | None = None
    ) -> SparkS3Datasource: ...
    def add_sql(self, name: str, *, connection_string: str) -> SQLDatasource: ...
    def add_sqlite(
        self, name: str, *, connection_string: SqliteDsn
    ) -> SqliteDatasource: ...
