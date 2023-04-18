"""Utilities to handle optional imports and related warnings e.g. sqlalchemy.

Great Expectations contains support for datasources and data stores that are
not included in the core package by default. Support requires install of
additional packages. To ensure these code paths are not executed when supporting
libraries are not installed, we check for existence of the associated library.

We also consolidate logic for warning based on version number in this module.
"""
from __future__ import annotations

import warnings
from typing import Any

from packaging.version import Version


class NotImported:
    def __init__(self, message: str):
        self.__dict__["gx_error_message"] = message

    def __getattr__(self, attr: str) -> Any:
        raise ModuleNotFoundError(self.__dict__["gx_error_message"])

    def __setattr__(self, key: str, value: Any) -> None:
        raise ModuleNotFoundError(self.__dict__["gx_error_message"])

    def __call__(self, *args, **kwargs) -> Any:
        raise ModuleNotFoundError(self.__dict__["gx_error_message"])

    def __str__(self) -> str:
        return self.__dict__["gx_error_message"]

    def __bool__(self):
        return False


def sqlalchemy_version_check(version: str | Version) -> None:
    """Check if the sqlalchemy version is supported or warn if not.

    Args:
        version: sqlalchemy version as a string or Version.
    """
    if isinstance(version, str):
        version = Version(version)

    if version >= Version("2.0.0"):
        warnings.warn(
            "SQLAlchemy v2.0.0 or later is not yet supported by Great Expectations.",
            UserWarning,
        )


def is_version_greater_or_equal(
    version: str | Version, compare_version: str | Version
) -> bool:
    """Check if the version is greater or equal to the compare_version.

    Args:
        version: Current version.
        compare_version: Version to compare to.

    Returns:
        Boolean indicating if the version is greater or equal to the compare version.
    """
    if isinstance(version, str):
        version = Version(version)
    if isinstance(compare_version, str):
        compare_version = Version(compare_version)

    return version >= compare_version


def is_version_less_than(
    version: str | Version, compare_version: str | Version
) -> bool:
    """Check if the version is less than the compare_version.

    Args:
        version: Current version.
        compare_version: Version to compare to.

    Returns:
        Boolean indicating if the version is less than the compare version.
    """
    if isinstance(version, str):
        version = Version(version)
    if isinstance(compare_version, str):
        compare_version = Version(compare_version)

    return version < compare_version


# GX optional imports
SQLALCHEMY_NOT_IMPORTED = NotImported(
    "sqlalchemy is not installed, please 'pip install sqlalchemy'"
)

try:
    import sqlalchemy

    sqlalchemy_version_check(sqlalchemy.__version__)
except (ImportError, AttributeError):
    sqlalchemy = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.dialects import registry as sqlalchemy_dialects_registry
except (ImportError, AttributeError):
    sqlalchemy_dialects_registry = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.engine import (
        Dialect as sqlalchemy_engine_Dialect,
    )
except (ImportError, AttributeError):
    sqlalchemy_engine_Dialect = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.engine import (
        Inspector as sqlalchemy_engine_Inspector,
    )
except (ImportError, AttributeError):
    sqlalchemy_engine_Inspector = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.engine import (
        reflection as sqlalchemy_reflection,
    )
except (ImportError, AttributeError):
    sqlalchemy_reflection = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.engine import (
        Connection as sqlalchemy_engine_Connection,
    )
except (ImportError, AttributeError):
    sqlalchemy_engine_Connection = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.engine import (
        Engine as sqlalchemy_engine_Engine,
    )
except (ImportError, AttributeError):
    sqlalchemy_engine_Engine = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.engine import (
        Row as sqlalchemy_engine_Row,
    )
except (ImportError, AttributeError):
    sqlalchemy_engine_Row = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.engine.default import (
        DefaultDialect as sqlalchemy_engine_DefaultDialect,
    )
except (ImportError, AttributeError):
    sqlalchemy_engine_DefaultDialect = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.exc import (
        DatabaseError as sqlalchemy_DatabaseError,
    )
except (ImportError, AttributeError):
    sqlalchemy_DatabaseError = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.exc import (
        IntegrityError as sqlalchemy_IntegrityError,
    )
except (ImportError, AttributeError):
    sqlalchemy_IntegrityError = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.exc import (
        NoSuchTableError as sqlalchemy_NoSuchTableError,
    )
except (ImportError, AttributeError):
    sqlalchemy_NoSuchTableError = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.exc import (
        OperationalError as sqlalchemy_OperationalError,
    )
except (ImportError, AttributeError):
    sqlalchemy_OperationalError = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.exc import (
        ProgrammingError as sqlalchemy_ProgrammingError,
    )
except (ImportError, AttributeError):
    sqlalchemy_ProgrammingError = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.exc import (
        SQLAlchemyError,
    )
except (ImportError, AttributeError):
    SQLAlchemyError = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql import Insert as sa_sql_Insert
except (ImportError, AttributeError):
    sa_sql_Insert = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.elements import (
        literal as sqlalchemy_literal,
    )
except (ImportError, AttributeError):
    sqlalchemy_literal = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.elements import (
        TextClause as sqlalchemy_TextClause,
    )
except (ImportError, AttributeError):
    sqlalchemy_TextClause = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.elements import (
        quoted_name,
    )
except (ImportError, AttributeError):
    quoted_name = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.expression import (
        CTE as sa_sql_expression_CTE,  # noqa N812
    )
except (ImportError, AttributeError):
    sa_sql_expression_CTE = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.expression import (
        BinaryExpression as sa_sql_expression_BinaryExpression,
    )
except (ImportError, AttributeError):
    sa_sql_expression_BinaryExpression = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.expression import (
        BooleanClauseList as sa_sql_expression_BooleanClauseList,
    )
except (ImportError, AttributeError):
    sa_sql_expression_BooleanClauseList = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.expression import (
        ColumnClause as sa_sql_expression_ColumnClause,
    )
except (ImportError, AttributeError):
    sa_sql_expression_ColumnClause = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.expression import (
        Label as sa_sql_expression_Label,
    )
except (ImportError, AttributeError):
    sa_sql_expression_Label = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.expression import (
        Select as sa_sql_expression_Select,
    )
except (ImportError, AttributeError):
    sa_sql_expression_Select = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.expression import (
        Selectable as sa_sql_expression_Selectable,
    )
except (ImportError, AttributeError):
    sa_sql_expression_Selectable = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.expression import (
        TableClause as sa_sql_expression_TableClause,
    )
except (ImportError, AttributeError):
    sa_sql_expression_TableClause = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.expression import (
        TextualSelect as sa_sql_expression_TextualSelect,
    )
except (ImportError, AttributeError):
    sa_sql_expression_TextualSelect = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.expression import (
        WithinGroup as sa_sql_expression_WithinGroup,
    )
except (ImportError, AttributeError):
    sa_sql_expression_WithinGroup = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.operators import custom_op as sqlalchemy_custom_op
except (ImportError, AttributeError):
    sqlalchemy_custom_op = SQLALCHEMY_NOT_IMPORTED


SPARK_NOT_IMPORTED = NotImported(
    "pyspark is not installed, please 'pip install pyspark'"
)

try:
    import pyspark as pyspark
except (ImportError, AttributeError):
    pyspark = SPARK_NOT_IMPORTED  # type: ignore[assignment]

try:
    import pyspark.sql.functions as F  # noqa N801
except (ImportError, AttributeError):
    F = SPARK_NOT_IMPORTED  # type: ignore[assignment]

try:
    import pyspark.sql.types as sparktypes
except (ImportError, AttributeError):
    sparktypes = SPARK_NOT_IMPORTED  # type: ignore[assignment]

try:
    from pyspark import SparkContext as pyspark_SparkContext
except (ImportError, AttributeError):
    pyspark_SparkContext = SPARK_NOT_IMPORTED  # type: ignore[assignment,misc]

try:
    from pyspark.ml.feature import Bucketizer as pyspark_ml_Bucketizer
except (ImportError, AttributeError):
    pyspark_ml_Bucketizer = SPARK_NOT_IMPORTED  # type: ignore[assignment,misc]

try:
    from pyspark.sql import (
        Column as pyspark_sql_Column,
    )
except (ImportError, AttributeError):
    pyspark_sql_Column = SPARK_NOT_IMPORTED  # type: ignore[assignment,misc]

try:
    from pyspark.sql import (
        DataFrame as pyspark_sql_DataFrame,
    )
except (ImportError, AttributeError):
    pyspark_sql_DataFrame = SPARK_NOT_IMPORTED  # type: ignore[assignment,misc]

try:
    from pyspark.sql import (
        Row as pyspark_sql_Row,
    )
except (ImportError, AttributeError):
    pyspark_sql_Row = SPARK_NOT_IMPORTED  # type: ignore[assignment,misc]

try:
    from pyspark.sql import (
        SparkSession as pyspark_sql_SparkSession,
    )
except (ImportError, AttributeError):
    pyspark_sql_SparkSession = SPARK_NOT_IMPORTED  # type: ignore[assignment,misc]

try:
    from pyspark.sql import (
        SQLContext as pyspark_SQLContext,
    )
except (ImportError, AttributeError):
    pyspark_SQLContext = SPARK_NOT_IMPORTED  # type: ignore[assignment,misc]

try:
    from pyspark.sql import (
        Window as pyspark_sql_Window,
    )
except (ImportError, AttributeError):
    pyspark_sql_Window = SPARK_NOT_IMPORTED  # type: ignore[assignment,misc]

try:
    from pyspark.sql.readwriter import DataFrameReader as pyspark_DataFrameReader
except (ImportError, AttributeError):
    pyspark_DataFrameReader = SPARK_NOT_IMPORTED  # type: ignore[assignment,misc]

try:
    from pyspark.sql.utils import (
        AnalysisException as pyspark_sql_utils_AnalysisException,
    )
except (ImportError, AttributeError):
    pyspark_sql_utils_AnalysisException = SPARK_NOT_IMPORTED  # type: ignore[assignment,misc]


GOOGLE_CLOUD_STORAGE_NOT_IMPORTED = NotImported(
    "google cloud storage components are not installed, please 'pip install google-cloud-storage google-cloud-secret-manager'"
)

try:
    from google.api_core.exceptions import GoogleAPIError  # noqa N801
except (ImportError, AttributeError):
    GoogleAPIError = GOOGLE_CLOUD_STORAGE_NOT_IMPORTED  # type: ignore[assignment,misc]

try:
    from google.auth.exceptions import DefaultCredentialsError  # noqa N801
except (ImportError, AttributeError):
    DefaultCredentialsError = GOOGLE_CLOUD_STORAGE_NOT_IMPORTED

try:
    from google.cloud import storage as google_cloud_storage
except (ImportError, AttributeError):
    google_cloud_storage = GOOGLE_CLOUD_STORAGE_NOT_IMPORTED

try:
    from google.cloud.storage import Client as GoogleCloudStorageClient
except (ImportError, AttributeError):
    GoogleCloudStorageClient = GOOGLE_CLOUD_STORAGE_NOT_IMPORTED

try:
    from google.oauth2 import service_account as google_service_account
except (ImportError, AttributeError):
    google_service_account = GOOGLE_CLOUD_STORAGE_NOT_IMPORTED

try:
    from google.oauth2.service_account import (
        Credentials as GoogleServiceAccountCredentials,
    )
except (ImportError, AttributeError):
    GoogleServiceAccountCredentials = GOOGLE_CLOUD_STORAGE_NOT_IMPORTED


AZURE_BLOB_STORAGE_NOT_IMPORTED = NotImported(
    "azure blob storage components are not installed, please 'pip install azure-storage-blob azure-identity azure-keyvault-secrets'"
)

try:
    from azure import storage as azure_storage
except (ImportError, AttributeError):
    azure_storage = AZURE_BLOB_STORAGE_NOT_IMPORTED

try:
    from azure.storage.blob import (
        BlobPrefix,
    )
except (ImportError, AttributeError):
    BlobPrefix = AZURE_BLOB_STORAGE_NOT_IMPORTED

try:
    from azure.storage.blob import (
        BlobServiceClient,
    )
except (ImportError, AttributeError):
    BlobServiceClient = AZURE_BLOB_STORAGE_NOT_IMPORTED

try:
    from azure.storage.blob import (
        ContainerClient,
    )
except (ImportError, AttributeError):
    ContainerClient = AZURE_BLOB_STORAGE_NOT_IMPORTED


PYARROW_NOT_IMPORTED = NotImported(
    "pyarrow is not installed, please 'pip install pyarrow'"
)

try:
    import pyarrow as pyarrow
except (ImportError, AttributeError):
    pyarrow = PYARROW_NOT_IMPORTED
