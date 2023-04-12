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

    from sqlalchemy.dialects import registry as sqlalchemy_dialects_registry
    from sqlalchemy.engine import (
        Dialect as sqlalchemy_engine_Dialect,
    )
    from sqlalchemy.engine import (
        Engine as sqlalchemy_engine_Engine,
    )
    from sqlalchemy.engine import (
        Inspector as sqlalchemy_engine_Inspector,
    )
    from sqlalchemy.engine import (
        Row as sqlalchemy_engine_Row,
    )
    from sqlalchemy.engine import (
        reflection as sqlalchemy_reflection,
    )
    from sqlalchemy.exc import (
        IntegrityError as sqlalchemy_IntegrityError,
    )
    from sqlalchemy.exc import (
        NoSuchTableError as sqlalchemy_NoSuchTableError,
    )
    from sqlalchemy.exc import (
        ProgrammingError as sqlalchemy_ProgrammingError,
    )
    from sqlalchemy.exc import (
        SQLAlchemyError,
    )
    from sqlalchemy.sql.elements import (
        BinaryExpression,
        BooleanClauseList,
        Label,
        TextClause,
        quoted_name,
    )
    from sqlalchemy.sql.expression import (
        CTE,
    )
    from sqlalchemy.sql.expression import (
        ColumnClause as sa_sql_expression_ColumnClause,
    )
    from sqlalchemy.sql.expression import (
        Select as sa_sql_expression_Select,
    )
    from sqlalchemy.sql.expression import (
        Selectable as sa_sql_expression_Selectable,
    )
    from sqlalchemy.sql.expression import (
        WithinGroup as sa_sql_expression_WithinGroup,
    )
except ImportError:
    sqlalchemy = SQLALCHEMY_NOT_IMPORTED
    sqlalchemy_dialects_registry = SQLALCHEMY_NOT_IMPORTED
    BinaryExpression = SQLALCHEMY_NOT_IMPORTED
    sqlalchemy_ProgrammingError = SQLALCHEMY_NOT_IMPORTED
    sa_sql_expression_WithinGroup = SQLALCHEMY_NOT_IMPORTED
    CTE = SQLALCHEMY_NOT_IMPORTED
    sqlalchemy_NoSuchTableError = SQLALCHEMY_NOT_IMPORTED
    sqlalchemy_IntegrityError = SQLALCHEMY_NOT_IMPORTED
    SQLAlchemyError = SQLALCHEMY_NOT_IMPORTED
    sqlalchemy_engine_Engine = SQLALCHEMY_NOT_IMPORTED
    sqlalchemy_engine_Dialect = SQLALCHEMY_NOT_IMPORTED
    sqlalchemy_engine_Row = SQLALCHEMY_NOT_IMPORTED
    BooleanClauseList = SQLALCHEMY_NOT_IMPORTED
    Label = SQLALCHEMY_NOT_IMPORTED
    sqlalchemy_reflection = SQLALCHEMY_NOT_IMPORTED
    sa_sql_expression_ColumnClause = SQLALCHEMY_NOT_IMPORTED
    sa_sql_expression_Select = SQLALCHEMY_NOT_IMPORTED
    sa_sql_expression_Selectable = SQLALCHEMY_NOT_IMPORTED
    quoted_name = SQLALCHEMY_NOT_IMPORTED
    TextClause = SQLALCHEMY_NOT_IMPORTED
    sqlalchemy_engine_Inspector = SQLALCHEMY_NOT_IMPORTED


SPARK_NOT_IMPORTED = NotImported(
    "pyspark is not installed, please 'pip install pyspark'"
)
try:
    import pyspark as pyspark
    import pyspark.sql.functions as F  # noqa N801
    import pyspark.sql.types as sparktypes
    from pyspark import SparkContext
    from pyspark.ml.feature import Bucketizer
    from pyspark.sql import (
        Column as pyspark_sql_Column,
    )
    from pyspark.sql import (
        DataFrame as pyspark_sql_DataFrame,
    )
    from pyspark.sql import (
        Row as pyspark_sql_Row,
    )
    from pyspark.sql import (
        SparkSession as pyspark_sql_SparkSession,
    )
    from pyspark.sql import (
        SQLContext as pyspark_SQLContext,
    )
    from pyspark.sql import (
        Window as pyspark_sql_Window,
    )
    from pyspark.sql.readwriter import DataFrameReader as pyspark_DataFrameReader
    from pyspark.sql.utils import (
        AnalysisException as pyspark_sql_utils_AnalysisException,
    )
except ImportError:
    pyspark_DataFrameReader = SPARK_NOT_IMPORTED
    pyspark = SPARK_NOT_IMPORTED  # type: ignore[assignment]
    pyspark_sql_Row = SPARK_NOT_IMPORTED
    F = SPARK_NOT_IMPORTED
    pyspark_sql_utils_AnalysisException = SPARK_NOT_IMPORTED
    sparktypes = SPARK_NOT_IMPORTED
    pyspark_sql_DataFrame = SPARK_NOT_IMPORTED
    pyspark_sql_Column = SPARK_NOT_IMPORTED
    SparkContext = SPARK_NOT_IMPORTED
    pyspark_sql_SparkSession = SPARK_NOT_IMPORTED
    pyspark_SQLContext = SPARK_NOT_IMPORTED
    pyspark_sql_Window = SPARK_NOT_IMPORTED
    Bucketizer = SPARK_NOT_IMPORTED

GOOGLE_CLOUD_STORAGE_NOT_IMPORTED = NotImported(
    "google cloud storage components are not installed, please 'pip install google-cloud-storage google-cloud-secret-manager'"
)
try:
    from google.api_core.exceptions import GoogleAPIError  # noqa N801
    from google.auth.exceptions import DefaultCredentialsError  # noqa N801
    from google.cloud import storage as google_cloud_storage
    from google.cloud.storage import Client as GoogleCloudStorageClient
    from google.oauth2 import service_account as google_service_account
    from google.oauth2.service_account import (
        Credentials as GoogleServiceAccountCredentials,
    )
except ImportError:
    google_cloud_storage = GOOGLE_CLOUD_STORAGE_NOT_IMPORTED
    google_service_account = GoogleServiceAccountCredentials
    GoogleServiceAccountCredentials = GOOGLE_CLOUD_STORAGE_NOT_IMPORTED
    GoogleCloudStorageClient = GOOGLE_CLOUD_STORAGE_NOT_IMPORTED
    GoogleAPIError = GOOGLE_CLOUD_STORAGE_NOT_IMPORTED
    DefaultCredentialsError = GOOGLE_CLOUD_STORAGE_NOT_IMPORTED


AZURE_BLOB_STORAGE_NOT_IMPORTED = NotImported(
    "azure blob storage components are not installed, please 'pip install azure-storage-blob azure-identity azure-keyvault-secrets'"
)
try:
    from azure import storage as azure_storage
    from azure.storage.blob import (
        BlobPrefix,
        BlobServiceClient,
        ContainerClient,
    )
except ImportError:
    azure_storage = AZURE_BLOB_STORAGE_NOT_IMPORTED
    BlobPrefix = AZURE_BLOB_STORAGE_NOT_IMPORTED
    BlobServiceClient = AZURE_BLOB_STORAGE_NOT_IMPORTED
    ContainerClient = AZURE_BLOB_STORAGE_NOT_IMPORTED
