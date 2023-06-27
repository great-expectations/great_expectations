from __future__ import annotations

from great_expectations.compatibility.not_imported import NotImported

AWS_NOT_IMPORTED = NotImported(
    "AWS connection components are not installed, please 'pip install boto3 botocore'"
)

try:
    import boto3  # noqa TID251
except ImportError:
    boto3 = AWS_NOT_IMPORTED

try:
    import botocore  # TID251
except ImportError:
    botocore = AWS_NOT_IMPORTED

try:
    from botocore import exceptions
except ImportError:
    exceptions = AWS_NOT_IMPORTED
