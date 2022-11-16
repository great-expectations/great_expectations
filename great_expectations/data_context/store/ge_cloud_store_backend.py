import warnings
from typing import Dict, Optional, Union

from great_expectations.data_context.cloud_constants import (
    CLOUD_DEFAULT_BASE_URL,
    GXCloudRESTResource,
)
from great_expectations.data_context.store.gx_cloud_store_backend import (
    GXCloudStoreBackend,
)


class GeCloudStoreBackend(GXCloudStoreBackend):
    def __init__(
        self,
        ge_cloud_credentials: Dict,
        ge_cloud_base_url: str = CLOUD_DEFAULT_BASE_URL,
        ge_cloud_resource_type: Optional[Union[str, GXCloudRESTResource]] = None,
        ge_cloud_resource_name: Optional[str] = None,
        suppress_store_backend_id: bool = True,
        manually_initialize_store_backend_id: str = "",
        store_name: Optional[str] = None,
    ) -> None:
        # deprecated-v0.15.33
        warnings.warn(
            f"Importing the class {self.__class__.__name__} from `great_expectations.data_context.store` is deprecated as of v0.15.33. "
            f"Please import class {GXCloudStoreBackend.__name__} from `great_expectations.data_context.store` moving forward.",
            DeprecationWarning,
        )
        super().__init__(
            ge_cloud_credentials=ge_cloud_credentials,
            ge_cloud_base_url=ge_cloud_base_url,
            ge_cloud_resource_type=ge_cloud_resource_type,
            ge_cloud_resource_name=ge_cloud_resource_name,
            suppress_store_backend_id=suppress_store_backend_id,
            manually_initialize_store_backend_id=manually_initialize_store_backend_id,
            store_name=store_name,
        )
