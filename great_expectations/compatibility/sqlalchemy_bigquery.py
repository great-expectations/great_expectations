from __future__ import annotations

from great_expectations.compatibility.not_imported import NotImported

SQLALCHEMY_BIGQUERY_NOT_IMPORTED = NotImported(
    "sqlalchemy-bigquery is not installed, please 'pip install sqlalchemy-bigquery'"
)
_BIGQUERY_MODULE_NAME = "sqlalchemy_bigquery"
BIGQUERY_GEO_SUPPORT = False

bigquery_types_tuple = None


try:
    import sqlalchemy_bigquery
except (ImportError, AttributeError):
    sqlalchemy_bigquery = SQLALCHEMY_BIGQUERY_NOT_IMPORTED

try:
    from sqlalchemy_bigquery import GEOGRAPHY
except (ImportError, AttributeError):
    GEOGRAPHY = SQLALCHEMY_BIGQUERY_NOT_IMPORTED

if sqlalchemy_bigquery:
    BIGQUERY_TYPES = {
        "INTEGER": sqlalchemy_bigquery.INTEGER,
        "NUMERIC": sqlalchemy_bigquery.NUMERIC,
        "STRING": sqlalchemy_bigquery.STRING,
        "BIGNUMERIC": sqlalchemy_bigquery.BIGNUMERIC,
        "BYTES": sqlalchemy_bigquery.BYTES,
        "BOOL": sqlalchemy_bigquery.BOOL,
        "BOOLEAN": sqlalchemy_bigquery.BOOLEAN,
        "TIMESTAMP": sqlalchemy_bigquery.TIMESTAMP,
        "TIME": sqlalchemy_bigquery.TIME,
        "FLOAT": sqlalchemy_bigquery.FLOAT,
        "DATE": sqlalchemy_bigquery.DATE,
        "DATETIME": sqlalchemy_bigquery.DATETIME,
    }
else:
    BIGQUERY_TYPES = {}
