"""
This file manages common global-level imports for which we want to centralize error handling
"""
import logging

logger = logging.getLogger(__name__)
sa_import_warning_required = False
spark_import_warning_required = False

try:
    import sqlalchemy as sa
except ImportError:
    logger.debug("No SqlAlchemy module available.")
    sa = None

try:
    from sqlalchemy.engine import Engine as sqlalchemy_engine_Engine
    from sqlalchemy.engine import Row as sqlalchemy_engine_Row
    from sqlalchemy.engine import reflection
except ImportError:
    logger.debug("No SqlAlchemy.engine module available.")
    reflection = None
    sqlalchemy_engine_Engine = None
    sqlalchemy_engine_Row = None

try:
    import sqlalchemy.func.count as sa_func_count
except ImportError:
    logger.debug("No SqlAlchemy.func module available.")
    sa_func_count = None

try:
    import sqlalchemy.sql.expression.ColumnClause as sa_sql_expression_ColumnClause
    import sqlalchemy.sql.expression.Select as sa_sql_expression_Select
    import sqlalchemy.sql.expression.Selectable as sa_sql_expression_Selectable
except ImportError:
    logger.debug("No SqlAlchemy.sql.expression module available.")
    sa_sql_expression_ColumnClause = None
    sa_sql_expression_Select = None
    sa_sql_expression_Selectable = None

try:
    import pyspark.sql.functions as F
    import pyspark.sql.types as sparktypes
except ImportError:
    logger.debug("No spark functions module available.")
    sparktypes = None
    F = None

try:
    from pyspark.ml.feature import Bucketizer
except ImportError:
    logger.debug("No spark Bucketizer available.")
    Bucketizer = None

try:
    from pyspark.sql import Window
except ImportError:
    logger.debug("No spark Window function available.")
    Window = None

try:
    from pyspark.sql import Column as pyspark_sql_Column
    from pyspark.sql import DataFrame as pyspark_sql_DataFrame
    from pyspark.sql import Row as pyspark_sql_Row
    from pyspark.sql import SparkSession as pyspark_sql_SparkSession
    from pyspark.sql import SQLContext
except ImportError:
    logger.debug("No spark SQLContext available.")
    SQLContext = None
    pyspark_sql_Column = None
    pyspark_sql_DataFrame = None
    pyspark_sql_Row = None
    pyspark_sql_SparkSession = None
