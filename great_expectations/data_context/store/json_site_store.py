from __future__ import annotations

from json import loads
from typing import Dict

from great_expectations.compatibility.typing_extensions import override
from great_expectations.data_context.store.store import Store
from great_expectations.data_context.util import load_class
from great_expectations.render import RenderedDocumentContent
from great_expectations.util import (
    filter_properties_dict,
    verify_dynamic_loading_support,
)


class JsonSiteStore(Store):
    """
    A JsonSiteStore manages the JSON artifacts of our renderers, which allows us to render them into final views in HTML by GX Cloud.

    """  # noqa: E501

    def __init__(self, store_backend=None, runtime_environment=None, store_name=None) -> None:
        if store_backend is not None:
            store_backend_module_name = store_backend.get(
                "module_name", "great_expectations.data_context.store"
            )
            store_backend_class_name = store_backend.get("class_name", "InMemoryStoreBackend")
            verify_dynamic_loading_support(module_name=store_backend_module_name)
            # TODO: GG 20220815 loaded store_backend_class is not used remove this if not needed
            _ = load_class(store_backend_class_name, store_backend_module_name)

        super().__init__(
            store_backend=store_backend,
            runtime_environment=runtime_environment,
            store_name=store_name,
        )

        # Gather the call arguments of the present function (include the "module_name" and add the "class_name"), filter  # noqa: E501
        # out the Falsy values, and set the instance "_config" variable equal to the resulting dictionary.  # noqa: E501
        self._config = {
            "store_backend": store_backend,
            "runtime_environment": runtime_environment,
            "store_name": store_name,
            "module_name": self.__class__.__module__,
            "class_name": self.__class__.__name__,
        }
        filter_properties_dict(properties=self._config, clean_falsy=True, inplace=True)

    @override
    @staticmethod
    def gx_cloud_response_json_to_object_dict(response_json: Dict) -> Dict:
        """
        This method takes full json response from GX cloud and outputs a dict appropriate for
        deserialization into a GX object
        """
        ge_cloud_json_site_id = response_json["data"]["id"]
        json_site_dict = response_json["data"]["attributes"]["rendered_data_doc"]
        json_site_dict["id"] = ge_cloud_json_site_id

        return json_site_dict

    def serialize(self, value):  # type: ignore[explicit-override] # FIXME
        return value.to_json_dict()

    def deserialize(self, value):  # type: ignore[explicit-override] # FIXME
        return RenderedDocumentContent(**loads(value))

    @property
    @override
    def config(self) -> dict:
        return self._config
