from great_expectations.compatibility.not_imported import NotImported

DATABRICKS_CONNECT_NOT_IMPORTED = NotImported(
    "databricks-connect is not installed, please 'pip install databricks-connect'"
)

try:
    from databricks import connect
except ImportError:
    connect = DATABRICKS_CONNECT_NOT_IMPORTED
