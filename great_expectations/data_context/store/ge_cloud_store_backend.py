import logging
from abc import ABCMeta
from json import JSONDecodeError
from typing import Dict, Optional
from urllib.parse import urljoin

import requests

from great_expectations.data_context.store.store_backend import StoreBackend
from great_expectations.data_context.types.refs import GeCloudResourceRef
from great_expectations.exceptions import StoreBackendError
from great_expectations.util import (
    filter_properties_dict,
    hyphen,
    pluralize,
    singularize,
)

logger = logging.getLogger(__name__)


class GeCloudStoreBackend(StoreBackend, metaclass=ABCMeta):
    def __init__(
        self,
        ge_cloud_credentials: Dict,
        ge_cloud_base_url: Optional[str] = "https://app.greatexpectations.io/",
        ge_cloud_resource_type: Optional[str] = None,
        ge_cloud_resource_name: Optional[str] = None,
        suppress_store_backend_id: Optional[bool] = True,
        manually_initialize_store_backend_id: Optional[str] = "",
        store_name: Optional[str] = None,
    ):
        super().__init__(
            fixed_length_key=True,
            suppress_store_backend_id=suppress_store_backend_id,
            manually_initialize_store_backend_id=manually_initialize_store_backend_id,
            store_name=store_name,
        )
        assert ge_cloud_resource_type or ge_cloud_resource_name, (
            "Must provide either ge_cloud_resource_type or " "ge_cloud_resource_name"
        )
        self._ge_cloud_base_url = ge_cloud_base_url
        self._ge_cloud_resource_name = ge_cloud_resource_name or pluralize(
            ge_cloud_resource_type
        )
        self._ge_cloud_resource_type = ge_cloud_resource_type or singularize(
            ge_cloud_resource_name
        )
        self._ge_cloud_credentials = ge_cloud_credentials

        # Initialize with store_backend_id if not part of an HTMLSiteStore
        if not self._suppress_store_backend_id:
            _ = self.store_backend_id

        # Gather the call arguments of the present function (include the "module_name" and add the "class_name"), filter
        # out the Falsy values, and set the instance "_config" variable equal to the resulting dictionary.
        self._config = {
            "ge_cloud_base_url": ge_cloud_base_url,
            "ge_cloud_resource_name": ge_cloud_resource_name,
            "ge_cloud_resource_type": ge_cloud_resource_type,
            "fixed_length_key": True,
            "suppress_store_backend_id": suppress_store_backend_id,
            "manually_initialize_store_backend_id": manually_initialize_store_backend_id,
            "store_name": store_name,
            "module_name": self.__class__.__module__,
            "class_name": self.__class__.__name__,
        }
        filter_properties_dict(properties=self._config, inplace=True)

    @property
    def auth_headers(self):
        return {
            "Content-Type": "application/vnd.api+json",
            "Authorization": f'Bearer {self.ge_cloud_credentials.get("access_token")}',
        }

    def _get(self, key):
        ge_cloud_url = self.get_url_for_key(key=key)
        try:
            response = requests.get(ge_cloud_url, headers=self.auth_headers)
            return response.json()
        except JSONDecodeError as jsonError:
            logger.debug(
                "Failed to parse GE Cloud Response into JSON",
                str(response.text),
                str(jsonError),
            )
            raise StoreBackendError("Unable to get object in GE Cloud Store Backend.")

    def _move(self):
        pass

    def _set(self, key, value, **kwargs):
        # Each resource type has corresponding attribute key to include in POST body
        post_body_keys = {
            "suite_validation_result": "result",
            "checkpoint": "checkpoint_config",
        }

        resource_key = self.ge_cloud_resource_name
        resource_type = self.ge_cloud_resource_type
        account_id = self.ge_cloud_credentials["account_id"]

        post_body_key = post_body_keys[resource_type]

        data = {
            "data": {
                "type": resource_type,
                "attributes": {"account_id": account_id},
            }
        }

        data["data"]["attributes"][post_body_key] = value

        url = urljoin(
            self.ge_cloud_base_url,
            f"accounts/" f"{account_id}/" f"{hyphen(resource_key)}",
        )
        try:
            response = requests.post(url, json=data, headers=self.auth_headers)
            response_json = response.json()

            object_id = response_json["data"]["id"]
            object_url = self.get_url_for_key((object_id,))
            return GeCloudResourceRef(
                resource_type=self.ge_cloud_resource_type,
                ge_cloud_id=object_id,
                url=object_url,
            )
        # TODO: [Robby] Show more detailed error messages
        except Exception as e:
            logger.debug(str(e))
            raise StoreBackendError("Unable to set object in GE Cloud Store Backend.")

    @property
    def ge_cloud_base_url(self):
        return self._ge_cloud_base_url

    @property
    def ge_cloud_resource_name(self):
        return self._ge_cloud_resource_name

    @property
    def ge_cloud_resource_type(self):
        return self._ge_cloud_resource_type

    @property
    def ge_cloud_credentials(self):
        return self._ge_cloud_credentials

    def list_keys(self):
        url = urljoin(
            self.ge_cloud_base_url,
            f"accounts/"
            f"{self.ge_cloud_credentials['account_id']}/"
            f"{hyphen(self.ge_cloud_resource_name)}",
        )
        try:
            response = requests.get(url, headers=self.auth_headers)
            response_json = response.json()
            keys = [(resource["id"],) for resource in response_json.get("data")]
            return keys
        except Exception as e:
            logger.debug(str(e))
            raise StoreBackendError("Unable to list keys in GE Cloud Store Backend.")

    def get_url_for_key(self, key, protocol=None):
        ge_cloud_id = key[0]
        url = urljoin(
            self.ge_cloud_base_url,
            f"accounts/{self.ge_cloud_credentials['account_id']}/{self.ge_cloud_resource_name}/{ge_cloud_id}",
        )
        return url

    def remove_key(self, key):
        if not isinstance(key, tuple):
            key = key.to_tuple()

        ge_cloud_id = key[0]

        data = {
            "data": {
                "type": self.ge_cloud_resource_type,
                "id": ge_cloud_id,
                "attributes": {
                    "deleted": True,
                },
            }
        }

        url = urljoin(
            self.ge_cloud_base_url,
            f"accounts/"
            f"{self.ge_cloud_credentials['account_id']}/"
            f"{self.ge_cloud_resource_name}/"
            f"{ge_cloud_id}",
        )
        try:
            response = requests.patch(url, json=data, headers=self.auth_headers)
            response_status_code = response.status_code

            if response_status_code < 300:
                return True
            return False
        except Exception as e:
            logger.debug(str(e))
            raise StoreBackendError(
                "Unable to delete object in GE Cloud Store Backend."
            )

    def _has_key(self, key):
        all_keys = self.list_keys()
        return key in all_keys

    @property
    def config(self) -> dict:
        return self._config
