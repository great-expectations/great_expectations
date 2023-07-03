from __future__ import annotations

import warnings

from great_expectations.compatibility.not_imported import NotImported

GOOGLE_CLOUD_STORAGE_NOT_IMPORTED = NotImported(
    "google cloud storage components are not installed, please 'pip install google-cloud-storage google-cloud-secret-manager'"
)

with warnings.catch_warnings():
    # DeprecationWarning: pkg_resources is deprecated as an API
    warnings.simplefilter(action="ignore", category=DeprecationWarning)
    try:
        from google.cloud import secretmanager
    except (ImportError, AttributeError):
        secretmanager = GOOGLE_CLOUD_STORAGE_NOT_IMPORTED  # type: ignore[assignment]

try:
    from google.api_core.exceptions import GoogleAPIError
except (ImportError, AttributeError):
    GoogleAPIError = GOOGLE_CLOUD_STORAGE_NOT_IMPORTED  # type: ignore[assignment,misc]

try:
    from google.auth.exceptions import DefaultCredentialsError
except (ImportError, AttributeError):
    DefaultCredentialsError = GOOGLE_CLOUD_STORAGE_NOT_IMPORTED

try:
    from google.cloud.exceptions import NotFound
except (ImportError, AttributeError):
    NotFound = GOOGLE_CLOUD_STORAGE_NOT_IMPORTED  # type: ignore[assignment,misc]

try:
    from google.cloud import storage
except (ImportError, AttributeError):
    storage = GOOGLE_CLOUD_STORAGE_NOT_IMPORTED

try:
    from google.cloud import bigquery as python_bigquery
except (ImportError, AttributeError):
    python_bigquery = GOOGLE_CLOUD_STORAGE_NOT_IMPORTED  # type: ignore[assignment]
try:
    from google.cloud.storage import Client
except (ImportError, AttributeError):
    Client = GOOGLE_CLOUD_STORAGE_NOT_IMPORTED

try:
    from google.oauth2 import service_account
except (ImportError, AttributeError):
    service_account = GOOGLE_CLOUD_STORAGE_NOT_IMPORTED

try:
    from google.oauth2.service_account import Credentials
except (ImportError, AttributeError):
    Credentials = GOOGLE_CLOUD_STORAGE_NOT_IMPORTED
