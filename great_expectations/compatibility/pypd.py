from great_expectations.compatibility.not_imported import NotImported

PYPD_NOT_IMPORTED = NotImported(
    "PagerDuty dependencies are not installed, please 'pip install pypd'"
)

try:
    import pypd
except ImportError:
    pypd = PYPD_NOT_IMPORTED
