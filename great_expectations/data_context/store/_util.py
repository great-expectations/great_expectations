"""
Chetan - 20221116 - This module is to be deleted after all Stores and StoreBackends have been
refactored to work without `key_to_tuple()` conversion.
"""


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from great_expectations.data_context.store.store_backend import StoreBackend


def is_key_backed_store_backend(store_backend: StoreBackend) -> bool:
    from great_expectations.data_context.store.gx_cloud_store_backend import (
        GXCloudStoreBackend,
    )

    key_backed_store_backends = {GXCloudStoreBackend}
    return store_backend.__class__ in key_backed_store_backends
