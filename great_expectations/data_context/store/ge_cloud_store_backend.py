from __future__ import annotations

from typing import TYPE_CHECKING

from great_expectations.data_context.store.gx_cloud_store_backend import (
    GXCloudStoreBackend,
)

if TYPE_CHECKING:
    from typing_extensions import TypeAlias

# Type alias until we can completely remove GeCloudStoreBackend
GeCloudStoreBackend: TypeAlias = GXCloudStoreBackend
