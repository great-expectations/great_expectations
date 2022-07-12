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
    import sqlalchemy.engine as sqlalchemy_engine
    from sqlalchemy.engine import reflection
except ImportError:
    logger.debug("No SqlAlchemy module available.")
    sqlalchemy_engine = None
    reflection = None

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
    import pyspark.sql as pyspark_sql
    from pyspark.sql import SQLContext
except ImportError:
    logger.debug("No spark SQLContext available.")
    pyspark_sql = None
    SQLContext = None
