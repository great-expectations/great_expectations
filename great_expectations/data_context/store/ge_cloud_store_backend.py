import logging
import os
import random
import re
import shutil
import uuid
from abc import ABCMeta
from urllib.parse import urljoin

import requests

from great_expectations.data_context.store.store_backend import StoreBackend
from great_expectations.data_context.types.resource_identifiers import ConfigurationIdentifier
from great_expectations.exceptions import InvalidKeyError, StoreBackendError
from great_expectations.util import filter_properties_dict

logger = logging.getLogger(__name__)


class GeCloudStoreBackend(StoreBackend, metaclass=ABCMeta):
    def __init__(
        self,
        ge_cloud_base_url,
        ge_cloud_resource_name,
        suppress_store_backend_id=False,
        manually_initialize_store_backend_id: str = "",
        store_name=None,
    ):
        super().__init__(
            fixed_length_key=True,
            suppress_store_backend_id=suppress_store_backend_id,
            manually_initialize_store_backend_id=manually_initialize_store_backend_id,
            store_name=store_name,
        )
        self._ge_cloud_base_url = ge_cloud_base_url
        self._ge_cloud_resource_name = ge_cloud_resource_name

        # Initialize with store_backend_id if not part of an HTMLSiteStore
        if not self._suppress_store_backend_id:
            _ = self.store_backend_id

        # Gather the call arguments of the present function (include the "module_name" and add the "class_name"), filter
        # out the Falsy values, and set the instance "_config" variable equal to the resulting dictionary.
        self._config = {
            "ge_cloud_base_url": ge_cloud_base_url,
            "ge_cloud_resource_name": ge_cloud_resource_name,
            "fixed_length_key": True,
            "suppress_store_backend_id": suppress_store_backend_id,
            "manually_initialize_store_backend_id": manually_initialize_store_backend_id,
            "store_name": store_name,
            "module_name": self.__class__.__module__,
            "class_name": self.__class__.__name__,
        }
        filter_properties_dict(properties=self._config, inplace=True)

    def _get(self, key):
        ge_cloud_url = self.get_url_for_key(key=key)
        headers = {
            "Content-Type": "application/vnd.api+json"
        }
        print(ge_cloud_url)
        response = requests.get(ge_cloud_url, headers=headers)
        return response.json()

    def _move(self):
        pass

    def _set(self, key, value, **kwargs):
        new_ge_cloud_id = str(uuid.uuid4())
        key: ConfigurationIdentifier = ConfigurationIdentifier(
            configuration_key=new_ge_cloud_id,
        )

        data = {
            "data": {
                'type': 'checkpoint',
                "attributes": {
                    "created_at": '2021-04-27 02:41:36.885264',
                    "created_by_id": "df665fc4-1891-4ef7-9a12-a0c46015c92c",
                    "checkpoint_config": value.to_json_dict()
                },
            }
        }
        headers = {
            "Content-Type": "application/vnd.api+json"
        }
        url = urljoin(self.ge_cloud_base_url, self.ge_cloud_resource_name)
        print(url)
        response = requests.post(url, json=data, headers=headers)
        return response.json()
        # checkpoint_config = CheckpointConfig(**new_checkpoint.config.to_json_dict())
        # self.checkpoint_store.set(key=key, value=checkpoint_config)
        #
        # s3_object_key = self._build_s3_object_key(key)
        #
        # s3 = self._create_resource()
        #
        # try:
        #     result_s3 = s3.Object(self.bucket, s3_object_key)
        #     if isinstance(value, str):
        #         result_s3.put(
        #             Body=value.encode(content_encoding),
        #             ContentEncoding=content_encoding,
        #             ContentType=content_type,
        #         )
        #     else:
        #         result_s3.put(Body=value, ContentType=content_type)
        # except s3.meta.client.exceptions.ClientError as e:
        #     logger.debug(str(e))
        #     raise StoreBackendError("Unable to set object in s3.")
        #
        # return s3_object_key

    @property
    def ge_cloud_base_url(self):
        return self._ge_cloud_base_url

    @property
    def ge_cloud_resource_name(self):
        return self._ge_cloud_resource_name

    def list_keys(self):
        pass

    def get_url_for_key(self, key, protocol=None):
        print("=====")
        print(key[0])
        print("======")
        ge_cloud_id = key[0]
        url = urljoin(self.ge_cloud_base_url, f"{self.ge_cloud_resource_name}/{ge_cloud_id}")
        print(url)
        return url

    def remove_key(self, key):
        pass

    def _has_key(self, key):
        all_keys = self.list_keys()
        return key in all_keys

    @property
    def config(self) -> dict:
        return self._config
