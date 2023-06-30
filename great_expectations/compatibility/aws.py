from __future__ import annotations

from great_expectations.compatibility.not_imported import NotImported

BOTO_NOT_IMPORTED = NotImported(
    "AWS S3 connection components are not installed, please 'pip install boto3 botocore'"
)
REDSHIFT_NOT_IMPORTED = NotImported(
    "AWS Redshift connection component is not installed, please 'pip install sqlalchemy_redshift'"
)
ATHENA_NOT_IMPORTED = NotImported(
    "AWS Athena connection component is not installed, please 'pip install pyathena[SQLAlchemy]>=2.0.0,<3'"
)

try:
    import boto3  # noqa TID251
except ImportError:
    boto3 = BOTO_NOT_IMPORTED

try:
    import botocore
except ImportError:
    botocore = BOTO_NOT_IMPORTED

try:
    from botocore.client import Config
except ImportError:
    Config = BOTO_NOT_IMPORTED

try:
    from botocore import exceptions
except ImportError:
    exceptions = BOTO_NOT_IMPORTED

try:
    import sqlalchemy_redshift
except ImportError:
    sqlalchemy_redshift = REDSHIFT_NOT_IMPORTED

try:
    from sqlalchemy_redshift import dialect as redshiftdialect
except (ImportError, AttributeError):
    redshiftdialect = REDSHIFT_NOT_IMPORTED

try:
    import pyathena
except ImportError:
    pyathena = ATHENA_NOT_IMPORTED  # type: ignore[assignment]

try:
    from pyathena import sqlalchemy_athena
except (ImportError, AttributeError):
    sqlalchemy_athena = ATHENA_NOT_IMPORTED  # type: ignore[assignment]

try:
    from pyathena.sqlalchemy_athena import types as athenatypes
except (ImportError, AttributeError):
    athenatypes = ATHENA_NOT_IMPORTED
