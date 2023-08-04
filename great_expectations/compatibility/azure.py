from __future__ import annotations

from great_expectations.compatibility.not_imported import NotImported

AZURE_BLOB_STORAGE_NOT_IMPORTED = NotImported(
    "azure blob storage components are not installed, please 'pip install azure-storage-blob azure-identity azure-keyvault-secrets'"
)

try:
    from azure import storage
except ImportError:
    storage = AZURE_BLOB_STORAGE_NOT_IMPORTED

try:
    from azure.identity import DefaultAzureCredential
except ImportError:
    DefaultAzureCredential = AZURE_BLOB_STORAGE_NOT_IMPORTED  # type: ignore[misc] # assigning to type

try:
    from azure.keyvault.secrets import SecretClient
except (ImportError, AttributeError):
    SecretClient = AZURE_BLOB_STORAGE_NOT_IMPORTED  # type: ignore[misc] # assigning to type

try:
    from azure.storage.blob import ContentSettings
except (ImportError, AttributeError):
    ContentSettings = AZURE_BLOB_STORAGE_NOT_IMPORTED  # type: ignore[misc] # assigning to type
try:
    from azure.storage.blob import BlobPrefix
except (ImportError, AttributeError):
    BlobPrefix = AZURE_BLOB_STORAGE_NOT_IMPORTED  # type: ignore[misc] # assigning to type

try:
    from azure.storage.blob import BlobServiceClient
except (ImportError, AttributeError):
    BlobServiceClient = AZURE_BLOB_STORAGE_NOT_IMPORTED  # type: ignore[misc] # assigning to type

try:
    from azure.storage.blob import ContainerClient
except (ImportError, AttributeError):
    ContainerClient = AZURE_BLOB_STORAGE_NOT_IMPORTED  # type: ignore[misc] # assigning to type
