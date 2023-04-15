from __future__ import annotations

from great_expectations.compatibility.not_imported import NotImported

GOOGLE_CLOUD_STORAGE_NOT_IMPORTED = NotImported(
    "google cloud storage components are not installed, please 'pip install google-cloud-storage google-cloud-secret-manager'"
)

try:
    from google.api_core.exceptions import GoogleAPIError  # noqa: TID251
except (ImportError, AttributeError):
    GoogleAPIError = GOOGLE_CLOUD_STORAGE_NOT_IMPORTED  # type: ignore[assignment,misc]

try:
    from google.auth.exceptions import DefaultCredentialsError  # noqa: TID251
except (ImportError, AttributeError):
    DefaultCredentialsError = GOOGLE_CLOUD_STORAGE_NOT_IMPORTED

try:
    from google.cloud import storage  # noqa: TID251
except (ImportError, AttributeError):
    storage = GOOGLE_CLOUD_STORAGE_NOT_IMPORTED

try:
    from google.cloud.storage import Client  # noqa: TID251
except (ImportError, AttributeError):
    Client = GOOGLE_CLOUD_STORAGE_NOT_IMPORTED

try:
    from google.oauth2 import service_account  # noqa: TID251
except (ImportError, AttributeError):
    service_account = GOOGLE_CLOUD_STORAGE_NOT_IMPORTED

try:
    from google.oauth2.service_account import Credentials  # noqa: TID251
except (ImportError, AttributeError):
    Credentials = GOOGLE_CLOUD_STORAGE_NOT_IMPORTED
