from __future__ import annotations

from types import ModuleType
from typing import TYPE_CHECKING, Type

from great_expectations.compatibility.not_imported import NotImported

if TYPE_CHECKING:
    from azure.keyvault import secrets
    from azure.storage import blob

AZURE_BLOB_STORAGE_NOT_IMPORTED = NotImported(
    "azure blob storage components are not installed, please 'pip install azure-storage-blob azure-identity azure-keyvault-secrets'"
)

try:
    storage: NotImported | ModuleType
    from azure import storage
except ImportError:
    storage = AZURE_BLOB_STORAGE_NOT_IMPORTED

try:
    pass
except ImportError:
    DefaultAzureCredentia: NotImported | type = AZURE_BLOB_STORAGE_NOT_IMPORTED

try:
    SecretClient: NotImported | Type[secrets.SecretClient]
    from azure.keyvault.secrets import SecretClient
except (ImportError, AttributeError):
    SecretClient = AZURE_BLOB_STORAGE_NOT_IMPORTED

try:
    ContentSettings: NotImported | Type[blob.ContentSettings]
    from azure.storage.blob import ContentSettings
except (ImportError, AttributeError):
    ContentSettings = AZURE_BLOB_STORAGE_NOT_IMPORTED
try:
    BlobPrefix: NotImported | Type[blob.BlobPrefix]
    from azure.storage.blob import BlobPrefix
except (ImportError, AttributeError):
    BlobPrefix = AZURE_BLOB_STORAGE_NOT_IMPORTED

try:
    BlobServiceClient: NotImported | Type[blob.BlobServiceClient]
    from azure.storage.blob import BlobServiceClient
except (ImportError, AttributeError):
    BlobServiceClient = AZURE_BLOB_STORAGE_NOT_IMPORTED

try:
    ContainerClient: NotImported | Type[blob.ContainerClient]
    from azure.storage.blob import ContainerClient
except (ImportError, AttributeError):
    ContainerClient = AZURE_BLOB_STORAGE_NOT_IMPORTED
