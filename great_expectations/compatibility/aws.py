from __future__ import annotations

from great_expectations.compatibility.not_imported import NotImported

AWS_NOT_IMPORTED = NotImported(
    "Unable to load AWS connection object; please 'pip install boto3 botocore'"
)

try:
    import boto3
except ImportError:
    boto3 = AWS_NOT_IMPORTED

try:
    import botocore
except ImportError:
    botocore = AWS_NOT_IMPORTED

try:
    from botocore.exceptions import ClientError

except (ImportError, AttributeError):
    ClientError = AWS_NOT_IMPORTED

try:
    from botocore.exceptions import ParamValidationError

except (ImportError, AttributeError):
    ParamValidationError = AWS_NOT_IMPORTED
