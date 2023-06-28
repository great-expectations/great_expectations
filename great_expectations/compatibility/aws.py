from __future__ import annotations

from great_expectations.compatibility.not_imported import NotImported

AWS_NOT_IMPORTED = NotImported(
    "AWS connection components are not installed, please 'pip install boto3 botocore sqlalchemy_redshift'"
)

try:
    import boto3  # noqa TID251
except ImportError:
    boto3 = AWS_NOT_IMPORTED

try:
    import botocore
except ImportError:
    botocore = AWS_NOT_IMPORTED

try:
    from botocore.client import Config
except ImportError:
    Config = AWS_NOT_IMPORTED

try:
    from botocore import exceptions
except ImportError:
    exceptions = AWS_NOT_IMPORTED

try:
    import sqlalchemy_redshift
except ImportError:
    sqlalchemy_redshift = AWS_NOT_IMPORTED

try:
    from sqlalchemy_redshift import dialect as redshiftdialect
except (ImportError, AttributeError):
    redshiftdialect = AWS_NOT_IMPORTED
