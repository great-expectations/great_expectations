from __future__ import annotations

import warnings

from great_expectations.compatibility.not_imported import NotImported

SPARK_NOT_IMPORTED = NotImported(
    "pyspark is not installed, please 'pip install pyspark'"
)

with warnings.catch_warnings():
    # DeprecationWarning: typing.io is deprecated, import directly from typing instead. typing.io will be removed in Python 3.12.
    warnings.simplefilter(action="ignore", category=DeprecationWarning)
    try:
        import pyspark
    except ImportError:
        pyspark = SPARK_NOT_IMPORTED  # type: ignore[assignment]

try:
    from pyspark.sql import functions
except (ImportError, AttributeError):
    functions = SPARK_NOT_IMPORTED  # type: ignore[assignment]

try:
    from pyspark.sql import types
except (ImportError, AttributeError):
    types = SPARK_NOT_IMPORTED  # type: ignore[assignment]

try:
    from pyspark import SparkContext
except ImportError:
    SparkContext = SPARK_NOT_IMPORTED  # type: ignore[assignment,misc]

try:
    from pyspark.ml.feature import Bucketizer
except (ImportError, AttributeError):
    Bucketizer = SPARK_NOT_IMPORTED  # type: ignore[assignment,misc]

try:
    from pyspark.sql import Column
except (ImportError, AttributeError):
    Column = SPARK_NOT_IMPORTED  # type: ignore[assignment,misc]

try:
    from pyspark.sql import DataFrame
except (ImportError, AttributeError):
    DataFrame = SPARK_NOT_IMPORTED  # type: ignore[assignment,misc]

try:
    from pyspark.sql import Row
except (ImportError, AttributeError):
    Row = SPARK_NOT_IMPORTED  # type: ignore[assignment,misc]

try:
    from pyspark.sql import SparkSession
except (ImportError, AttributeError):
    SparkSession = SPARK_NOT_IMPORTED  # type: ignore[assignment,misc]

try:
    from pyspark.sql import SQLContext
except (ImportError, AttributeError):
    SQLContext = SPARK_NOT_IMPORTED  # type: ignore[assignment,misc]

try:
    from pyspark.sql import Window
except (ImportError, AttributeError):
    Window = SPARK_NOT_IMPORTED  # type: ignore[assignment,misc]

try:
    from pyspark.sql.readwriter import DataFrameReader
except (ImportError, AttributeError):
    DataFrameReader = SPARK_NOT_IMPORTED  # type: ignore[assignment,misc]

try:
    from pyspark.sql.utils import AnalysisException
except (ImportError, AttributeError):
    AnalysisException = SPARK_NOT_IMPORTED  # type: ignore[assignment,misc]
