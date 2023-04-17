from __future__ import annotations

from great_expectations.compatibility.not_imported import NotImported

SPARK_NOT_IMPORTED = NotImported(
    "pyspark is not installed, please 'pip install pyspark'"
)

try:
    import pyspark  # noqa: TID251
except ImportError:
    pyspark = SPARK_NOT_IMPORTED  # type: ignore[assignment]

try:
    from pyspark.sql import functions  # noqa: TID251
except (ImportError, AttributeError):
    functions = SPARK_NOT_IMPORTED  # type: ignore[assignment]

try:
    from pyspark.sql import types  # noqa: TID251
except (ImportError, AttributeError):
    types = SPARK_NOT_IMPORTED  # type: ignore[assignment]

try:
    from pyspark import SparkContext  # noqa: TID251
except ImportError:
    SparkContext = SPARK_NOT_IMPORTED  # type: ignore[assignment,misc]

try:
    from pyspark.ml.feature import Bucketizer  # noqa: TID251
except (ImportError, AttributeError):
    Bucketizer = SPARK_NOT_IMPORTED  # type: ignore[assignment,misc]

try:
    from pyspark.sql import Column  # noqa: TID251
except (ImportError, AttributeError):
    Column = SPARK_NOT_IMPORTED  # type: ignore[assignment,misc]

try:
    from pyspark.sql import DataFrame  # noqa: TID251
except (ImportError, AttributeError):
    DataFrame = SPARK_NOT_IMPORTED  # type: ignore[assignment,misc]

try:
    from pyspark.sql import Row  # noqa: TID251
except (ImportError, AttributeError):
    Row = SPARK_NOT_IMPORTED  # type: ignore[assignment,misc]

try:
    from pyspark.sql import SparkSession  # noqa: TID251
except (ImportError, AttributeError):
    SparkSession = SPARK_NOT_IMPORTED  # type: ignore[assignment,misc]

try:
    from pyspark.sql import SQLContext  # noqa: TID251
except (ImportError, AttributeError):
    SQLContext = SPARK_NOT_IMPORTED  # type: ignore[assignment,misc]

try:
    from pyspark.sql import Window  # noqa: TID251
except (ImportError, AttributeError):
    Window = SPARK_NOT_IMPORTED  # type: ignore[assignment,misc]

try:
    from pyspark.sql.readwriter import DataFrameReader  # noqa: TID251
except (ImportError, AttributeError):
    DataFrameReader = SPARK_NOT_IMPORTED  # type: ignore[assignment,misc]

try:
    from pyspark.sql.utils import AnalysisException  # noqa: TID251
except (ImportError, AttributeError):
    AnalysisException = SPARK_NOT_IMPORTED  # type: ignore[assignment,misc]
