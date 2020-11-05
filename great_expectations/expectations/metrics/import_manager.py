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
    import pyspark.sql.functions as F
except ImportError:
    logger.debug("No spark functions module available.")
    F = None
