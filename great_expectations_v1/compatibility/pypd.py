from great_expectations_v1.compatibility.not_imported import NotImported

PYPD_NOT_IMPORTED = NotImported(
    "PagerDuty dependencies are not installed, please 'pip install pypd'"
)

try:
    import pypd
except ImportError:
    pypd = PYPD_NOT_IMPORTED
