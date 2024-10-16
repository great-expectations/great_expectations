from __future__ import annotations

import json
import logging
import weakref
from abc import ABCMeta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from urllib.parse import urljoin

import requests
from typing_extensions import TypedDict

from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.http import create_session
from great_expectations.data_context.cloud_constants import (
    CLOUD_DEFAULT_BASE_URL,
    SUPPORT_EMAIL,
    GXCloudRESTResource,
)
from great_expectations.data_context.store.store_backend import StoreBackend
from great_expectations.data_context.types.refs import GXCloudResourceRef
from great_expectations.data_context.types.resource_identifiers import GXCloudIdentifier
from great_expectations.exceptions import StoreBackendError, StoreBackendTransientError
from great_expectations.util import bidict, filter_properties_dict, hyphen

logger = logging.getLogger(__name__)


class ErrorDetail(TypedDict):
    code: Optional[str]
    detail: Optional[str]
    source: Union[str, Dict[str, str], None]


class ErrorPayload(TypedDict):
    errors: List[ErrorDetail]


class EndpointVersion(str, Enum):
    V0 = "V0"
    V1 = "V1"


def get_user_friendly_error_message(
    http_exc: requests.exceptions.HTTPError, log_level: int = logging.WARNING
) -> str:
    # TODO: define a GeCloud service/client for this & other related behavior
    support_message = []
    response: requests.Response = http_exc.response

    logger.log(log_level, f"{http_exc.__class__.__name__}:{http_exc} - {response}")

    request_id = response.headers.get("request-id", "")
    if request_id:
        support_message.append(f"Request-Id: {request_id}")

    try:
        error_json: ErrorPayload = http_exc.response.json()
        if isinstance(error_json, list):
            errors = error_json
        else:
            errors = error_json.get("errors")
        if errors:
            support_message.append(json.dumps(errors))
        else:
            support_message.append(json.dumps(error_json))

    except json.JSONDecodeError:
        support_message.append(f"Please contact the Great Expectations team at {SUPPORT_EMAIL}")
    return " ".join(support_message)


class GXCloudStoreBackend(StoreBackend, metaclass=ABCMeta):
    PAYLOAD_ATTRIBUTES_KEYS: Dict[GXCloudRESTResource, str] = {
        GXCloudRESTResource.CHECKPOINT: "checkpoint_config",
        GXCloudRESTResource.DATASOURCE: "datasource_config",
        GXCloudRESTResource.DATA_CONTEXT: "data_context_config",
        GXCloudRESTResource.DATA_CONTEXT_VARIABLES: "data_context_variables",
        GXCloudRESTResource.EXPECTATION_SUITE: "suite",
        GXCloudRESTResource.VALIDATION_RESULT: "result",
        GXCloudRESTResource.VALIDATION_DEFINITION: "validation_definition",
    }

    ALLOWED_SET_KWARGS_BY_RESOURCE_TYPE: Dict[GXCloudRESTResource, Set[str]] = {
        GXCloudRESTResource.EXPECTATION_SUITE: {"clause_id"},
        GXCloudRESTResource.VALIDATION_RESULT: {
            "checkpoint_id",
            "expectation_suite_id",
        },
    }

    RESOURCE_PLURALITY_LOOKUP_DICT: bidict = bidict(  # type: ignore[misc] # Keywords must be str
        **{  # type: ignore[arg-type]
            GXCloudRESTResource.CHECKPOINT: "checkpoints",
            GXCloudRESTResource.DATASOURCE: "datasources",
            GXCloudRESTResource.DATA_ASSET: "data_assets",
            GXCloudRESTResource.DATA_CONTEXT_VARIABLES: "data_context_variables",
            GXCloudRESTResource.EXPECTATION_SUITE: "expectation_suites",
            GXCloudRESTResource.VALIDATION_DEFINITION: "validation_definitions",
            GXCloudRESTResource.VALIDATION_RESULT: "validation_results",
        }
    )

    _ENDPOINT_VERSION_LOOKUP: dict[str, EndpointVersion] = {
        GXCloudRESTResource.CHECKPOINT: EndpointVersion.V1,
        GXCloudRESTResource.DATASOURCE: EndpointVersion.V1,
        GXCloudRESTResource.DATA_ASSET: EndpointVersion.V1,
        GXCloudRESTResource.DATA_CONTEXT: EndpointVersion.V1,
        GXCloudRESTResource.DATA_CONTEXT_VARIABLES: EndpointVersion.V1,
        GXCloudRESTResource.EXPECTATION_SUITE: EndpointVersion.V1,
        GXCloudRESTResource.VALIDATION_DEFINITION: EndpointVersion.V1,
        GXCloudRESTResource.VALIDATION_RESULT: EndpointVersion.V1,
    }
    # we want to support looking up EndpointVersion from either GXCloudRESTResource
    # or a pluralized version of it, as defined by RESOURCE_PLURALITY_LOOKUP_DICT.
    for key, value in RESOURCE_PLURALITY_LOOKUP_DICT.items():
        # try to set the pluralized GXCloudRESTResource to the same endpoint as its singular,
        # with a fallback default of EndpointVersion.V0.
        _ENDPOINT_VERSION_LOOKUP[value] = _ENDPOINT_VERSION_LOOKUP.get(key, EndpointVersion.V0)

    def __init__(  # noqa: PLR0913
        self,
        ge_cloud_credentials: Dict,
        ge_cloud_base_url: str = CLOUD_DEFAULT_BASE_URL,
        ge_cloud_resource_type: Optional[Union[str, GXCloudRESTResource]] = None,
        ge_cloud_resource_name: Optional[str] = None,
        suppress_store_backend_id: bool = True,
        manually_initialize_store_backend_id: str = "",
        store_name: Optional[str] = None,
    ) -> None:
        super().__init__(
            fixed_length_key=True,
            suppress_store_backend_id=suppress_store_backend_id,
            manually_initialize_store_backend_id=manually_initialize_store_backend_id,
            store_name=store_name,
        )
        assert (
            ge_cloud_resource_type or ge_cloud_resource_name
        ), "Must provide either ge_cloud_resource_type or ge_cloud_resource_name"

        self._ge_cloud_base_url = ge_cloud_base_url

        self._ge_cloud_resource_name = (
            ge_cloud_resource_name or self.RESOURCE_PLURALITY_LOOKUP_DICT[ge_cloud_resource_type]
        )

        # While resource_types should be coming in as enums, configs represent the arg
        # as strings and require manual casting.
        if ge_cloud_resource_type and isinstance(ge_cloud_resource_type, str):
            ge_cloud_resource_type = ge_cloud_resource_type.upper()
            ge_cloud_resource_type = GXCloudRESTResource[ge_cloud_resource_type]

        self._ge_cloud_resource_type = (
            ge_cloud_resource_type or self.RESOURCE_PLURALITY_LOOKUP_DICT[ge_cloud_resource_name]
        )

        self._ge_cloud_credentials = ge_cloud_credentials

        # Initialize with store_backend_id if not part of an HTMLSiteStore
        if not self._suppress_store_backend_id:
            _ = self.store_backend_id

        self._session = create_session(access_token=self._ge_cloud_credentials["access_token"])
        # Finalizer to close the session when the object is garbage collected.
        # https://docs.python.org/3.11/library/weakref.html#weakref.finalize
        self._finalizer = weakref.finalize(self, close_session, self._session)

        # Gather the call arguments of the present function (include the "module_name" and add the "class_name"), filter  # noqa: E501
        # out the Falsy values, and set the instance "_config" variable equal to the resulting dictionary.  # noqa: E501
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

    @override
    def _get(  # type: ignore[override]
        self, key: Tuple[GXCloudRESTResource, str | None, str | None]
    ) -> dict:
        url = self.get_url_for_key(key=key)

        # if name is included in the key, add as a param
        params: dict | None
        if len(key) > 2 and key[2]:  # noqa: PLR2004
            params = {"name": key[2]}
            url = url.rstrip("/")
        else:
            params = None

        payload = self._send_get_request_to_api(url=url, params=params)

        # Requests using query params may return {"data": []} if the object doesn't exist
        # We need to validate that even if we have a 200, there are contents to support existence
        response_has_data = bool(payload.get("data"))
        if not response_has_data:
            raise StoreBackendError(  # noqa: TRY003
                "Unable to get object in GX Cloud Store Backend: Object does not exist."
            )

        return payload

    @override
    def _get_all(self) -> dict:  # type: ignore[override]
        url = self.construct_versioned_url(
            base_url=self.ge_cloud_base_url,
            organization_id=self.ge_cloud_credentials["organization_id"],
            resource_name=self.ge_cloud_resource_name,
        )

        payload = self._send_get_request_to_api(url=url)
        return payload

    def _send_get_request_to_api(self, url: str, params: dict | None = None) -> dict:
        try:
            response = self._session.get(
                url=url,
                params=params,
            )
            response.raise_for_status()
            response_json: dict = response.json()
            return response_json
        except json.JSONDecodeError as jsonError:
            logger.debug(  # noqa: PLE1205
                "Failed to parse GX Cloud Response into JSON",
                str(response.text),  # type: ignore[possibly-undefined] # will be present for json error
                str(jsonError),
            )
            raise StoreBackendError(  # noqa: TRY003
                f"Unable to get object in GX Cloud Store Backend: {jsonError}"
            ) from jsonError
        except requests.HTTPError as http_err:
            raise StoreBackendError(  # noqa: TRY003
                f"Unable to get object in GX Cloud Store Backend: {get_user_friendly_error_message(http_err)}"  # noqa: E501
            ) from http_err
        except requests.ConnectionError as conn_err:
            raise StoreBackendError(  # noqa: TRY003
                f"Unable to get object in GX Cloud Store Backend: {conn_err}"
            ) from conn_err
        except requests.Timeout as timeout_exc:
            logger.exception(timeout_exc)  # noqa: TRY401
            raise StoreBackendTransientError(  # noqa: TRY003
                "Unable to get object in GX Cloud Store Backend: This is likely a transient error. Please try again."  # noqa: E501
            ) from timeout_exc

    @override
    def _move(self) -> None:  # type: ignore[override]
        pass

    def _put(self, id: str, value: Any) -> GXCloudResourceRef | bool:
        # This wonky signature is a sign that our abstractions are not helping us.
        # The cloud backend returns a bool for some resources, and the updated
        # resource for others. Since we route all update calls through this single
        # method, we need to handle both cases.

        resource_type = self.ge_cloud_resource_type
        organization_id = self.ge_cloud_credentials["organization_id"]
        attributes_key = self.PAYLOAD_ATTRIBUTES_KEYS[resource_type]

        data = self.construct_versioned_payload(
            resource_type=resource_type.value,
            attributes_key=attributes_key,
            attributes_value=value,
            organization_id=organization_id,
            resource_id=id or None,  # filter out empty string
        )

        url = self.construct_versioned_url(
            base_url=self.ge_cloud_base_url,
            organization_id=organization_id,
            resource_name=self.ge_cloud_resource_name,
        )

        if id:
            url = urljoin(f"{url}/", id)

        try:
            response = self._session.put(url, json=data)
            response_status_code = response.status_code

            # 2022-07-28 - Chetan - GX Cloud does not currently support PUT requests
            # for the ExpectationSuite endpoint. As such, this is a temporary fork to
            # ensure that legacy PATCH behavior is supported.
            if (
                response_status_code == 405  # noqa: PLR2004
                and resource_type is GXCloudRESTResource.EXPECTATION_SUITE
            ):
                response = self._session.patch(url, json=data)
                response_status_code = response.status_code

            response.raise_for_status()

            HTTP_NO_CONTENT = 204
            if response_status_code == HTTP_NO_CONTENT:
                # endpoint has returned NO_CONTENT, so the caller expects a boolean
                return True
            else:
                # expect that there's a JSON payload associated with this response
                response_json = response.json()
                return GXCloudResourceRef(
                    resource_type=resource_type,
                    id=id,
                    url=url,
                    response_json=response_json,
                )

        except requests.HTTPError as http_exc:
            raise StoreBackendError(  # noqa: TRY003
                f"Unable to update object in GX Cloud Store Backend: {get_user_friendly_error_message(http_exc)}"  # noqa: E501
            ) from http_exc
        except requests.Timeout as timeout_exc:
            logger.exception(timeout_exc)  # noqa: TRY401
            raise StoreBackendTransientError(  # noqa: TRY003
                "Unable to update object in GX Cloud Store Backend: This is likely a transient error. Please try again."  # noqa: E501
            ) from timeout_exc
        except Exception as e:
            logger.debug(repr(e))
            raise StoreBackendError(  # noqa: TRY003
                f"Unable to update object in GX Cloud Store Backend: {e}"
            ) from e

    @property
    def allowed_set_kwargs(self) -> Set[str]:
        return self.ALLOWED_SET_KWARGS_BY_RESOURCE_TYPE.get(self.ge_cloud_resource_type, set())

    def validate_set_kwargs(self, kwargs: dict) -> Union[bool, None]:
        kwarg_names = set(kwargs.keys())
        if len(kwarg_names) == 0:
            return True
        if kwarg_names <= self.allowed_set_kwargs:
            return True
        if not (kwarg_names <= self.allowed_set_kwargs):
            extra_kwargs = kwarg_names - self.allowed_set_kwargs
            raise ValueError(  # noqa: TRY003
                f'Invalid kwargs: {(", ").join(extra_kwargs)}'
            )
        return None

    @override
    def _set(  # type: ignore[override]
        self,
        key: Tuple[GXCloudRESTResource, ...],
        value: Any,
        **kwargs,
    ) -> Union[bool, GXCloudResourceRef]:
        # Each V0 resource type has corresponding attribute key to include in POST body
        resource = key[0]
        id: str = key[1]

        # if key has an id, perform _put instead

        # Chetan - 20220713 - DataContextVariables are a special edge case for the Cloud product
        # and always necessitate a PUT.
        if id or resource is GXCloudRESTResource.DATA_CONTEXT_VARIABLES:
            # _put returns a bool
            return self._put(id=id, value=value)

        return self._post(value=value, **kwargs)

    def _post(self, value: Any, **kwargs) -> GXCloudResourceRef:
        resource_type = self.ge_cloud_resource_type
        resource_name = self.ge_cloud_resource_name
        organization_id = self.ge_cloud_credentials["organization_id"]

        attributes_key = self.PAYLOAD_ATTRIBUTES_KEYS[resource_type]

        kwargs = kwargs if self.validate_set_kwargs(kwargs) else {}
        data = self.construct_versioned_payload(
            resource_type=resource_type,
            attributes_key=attributes_key,
            attributes_value=value,
            organization_id=organization_id,
            **kwargs,
        )

        url = self.construct_versioned_url(
            base_url=self.ge_cloud_base_url,
            organization_id=organization_id,
            resource_name=resource_name,
        )

        try:
            response = self._session.post(url, json=data)
            response.raise_for_status()
            response_json = response.json()

            object_id = response_json["data"]["id"]
            object_url = self.get_url_for_key((self.ge_cloud_resource_type, object_id, None))
            # This method is where posts get made for all cloud store endpoints. We pass
            # the response_json back up to the caller because the specific resource may
            # want to parse resource specific data out of the response.
            return GXCloudResourceRef(
                resource_type=resource_type,
                id=object_id,
                url=object_url,
                response_json=response_json,
            )
        except requests.HTTPError as http_exc:
            raise StoreBackendError(  # noqa: TRY003
                f"Unable to set object in GX Cloud Store Backend: {get_user_friendly_error_message(http_exc)}"  # noqa: E501
            ) from http_exc
        except requests.Timeout as timeout_exc:
            logger.exception(timeout_exc)  # noqa: TRY401
            raise StoreBackendTransientError(  # noqa: TRY003
                "Unable to set object in GX Cloud Store Backend: This is likely a transient error. Please try again."  # noqa: E501
            ) from timeout_exc
        except Exception as e:
            logger.debug(str(e))
            raise StoreBackendError(  # noqa: TRY003
                f"Unable to set object in GX Cloud Store Backend: {e}"
            ) from e

    @property
    def ge_cloud_base_url(self) -> str:
        return self._ge_cloud_base_url

    @property
    def ge_cloud_resource_name(self) -> str:
        return self._ge_cloud_resource_name

    @property
    def ge_cloud_resource_type(self) -> GXCloudRESTResource:
        return self._ge_cloud_resource_type

    @property
    def ge_cloud_credentials(self) -> dict:
        return self._ge_cloud_credentials

    @override
    def list_keys(self, prefix: Tuple = ()) -> List[Tuple[GXCloudRESTResource, str, str]]:
        url = self.construct_versioned_url(
            base_url=self.ge_cloud_base_url,
            organization_id=self.ge_cloud_credentials["organization_id"],
            resource_name=self.ge_cloud_resource_name,
        )

        resource_type = self.ge_cloud_resource_type

        try:
            response_json = self._send_get_request_to_api(url=url)

            keys = []
            resource_name: str
            for resource in response_json["data"]:
                id: str = resource["id"]
                if self._is_v1_resource:
                    resource_name = resource["name"]
                else:  # V0 config
                    attributes_key = self.PAYLOAD_ATTRIBUTES_KEYS[resource_type]
                    resource_dict: dict = resource.get("attributes", {}).get(attributes_key, {})
                    resource_name = resource_dict.get("name", "")
                key = (resource_type, id, resource_name)
                keys.append(key)

            return keys
        except Exception as e:
            logger.debug(str(e))
            raise StoreBackendError(  # noqa: TRY003
                f"Unable to list keys in GX Cloud Store Backend: {e}"
            ) from e

    @override
    def get_url_for_key(
        self,
        key: Tuple[GXCloudRESTResource, str | None, str | None],
        protocol: Optional[Any] = None,
    ) -> str:
        id = key[1]
        url = self.construct_versioned_url(
            base_url=self.ge_cloud_base_url,
            organization_id=self.ge_cloud_credentials["organization_id"],
            resource_name=self.ge_cloud_resource_name,
            id=id,
        )
        return url

    def remove_key(self, key):  # type: ignore[explicit-override] # FIXME
        if not isinstance(key, tuple):
            key = key.to_tuple()

        id = key[1]
        if len(key) == 3:  # noqa: PLR2004
            resource_object_name = key[2]
        else:
            resource_object_name = None

        try:
            # prefer deletion by id if id present
            if id:
                url = self.construct_versioned_url(
                    base_url=self.ge_cloud_base_url,
                    organization_id=self.ge_cloud_credentials["organization_id"],
                    resource_name=self.ge_cloud_resource_name,
                    id=id,
                )
                response = self._session.delete(url)
                response.raise_for_status()
                return True
            # delete by name
            elif resource_object_name:
                url = self.construct_versioned_url(
                    base_url=self.ge_cloud_base_url,
                    organization_id=self.ge_cloud_credentials["organization_id"],
                    resource_name=self.ge_cloud_resource_name,
                )
                response = self._session.delete(url, params={"name": resource_object_name})
                response.raise_for_status()
                return True
        except requests.HTTPError as http_exc:
            logger.exception(http_exc)  # noqa: TRY401
            raise StoreBackendError(  # noqa: TRY003
                f"Unable to delete object in GX Cloud Store Backend: {get_user_friendly_error_message(http_exc)}"  # noqa: E501
            ) from http_exc
        except requests.Timeout as timeout_exc:
            logger.exception(timeout_exc)  # noqa: TRY401
            raise StoreBackendTransientError(  # noqa: TRY003
                "Unable to delete object in GX Cloud Store Backend: This is likely a transient error. Please try again."  # noqa: E501
            ) from timeout_exc
        except Exception as e:
            logger.debug(str(e))
            raise StoreBackendError(  # noqa: TRY003
                f"Unable to delete object in GX Cloud Store Backend: {e!r}"
            ) from e

    def _get_one_or_none_from_response_data(
        self,
        response_data: list[dict] | dict,
        key: tuple[GXCloudRESTResource, str | None, str | None],
    ) -> dict | None:
        """
        GET requests to cloud can either return response data that is a single object (get by id) or a
        list of objects with length >= 0 (get by name). This method takes this response data and returns a single
        object or None.
        """  # noqa: E501
        if not isinstance(response_data, list):
            return response_data
        if len(response_data) == 0:
            return None
        if len(response_data) == 1:
            return response_data[0]
        raise StoreBackendError(  # noqa: TRY003
            f"Unable to update object in GX Cloud Store Backend: the provided key ({key}) maps "
            f"to more than one object."
        )

    @override
    def _update(
        self,
        key: tuple[GXCloudRESTResource, str | None, str | None],
        value: dict,
        **kwargs,
    ) -> GXCloudResourceRef:
        # todo: ID should never be optional for update - remove this additional get
        response_data = self._get(key)["data"]
        # if the provided key does not contain id (only name), cloud will return a list of resources filtered  # noqa: E501
        # by name, with length >= 0, instead of a single object (or error if not found)
        existing = self._get_one_or_none_from_response_data(response_data=response_data, key=key)

        if existing is None:
            raise StoreBackendError(  # noqa: TRY003
                f"Unable to update object in GX Cloud Store Backend: could not find object associated with key {key}."  # noqa: E501
            )

        if key[1] is None:
            key = (key[0], existing["id"], key[2])

        return self.set(key=key, value=value, **kwargs)

    def _add_or_update(self, key, value, **kwargs):  # type: ignore[explicit-override] # FIXME
        try:
            response_data = self._get(key)["data"]
        except StoreBackendError as e:
            logger.info(f"Could not find object associated with key {key}: {e}")
            response_data = None

        # if the provided key does not contain id (only name), cloud will return a list of resources filtered  # noqa: E501
        # by name, with length >= 0, instead of a single object (or error if not found)
        existing = self._get_one_or_none_from_response_data(response_data=response_data, key=key)

        if existing is not None:
            id = key[1] if key[1] is not None else existing["id"]
            key = (key[0], id, key[2])
            return self.set(key=key, value=value, **kwargs)
        return self.add(key=key, value=value, **kwargs)

    @override
    def _has_key(self, key: Tuple[GXCloudRESTResource, str | None, str | None]) -> bool:
        try:
            _ = self._get(key)
            return True
        except StoreBackendTransientError:
            raise
        except StoreBackendError as e:
            logger.info(f"Could not find object associated with key {key}: {e}")
            return False

    @property
    @override
    def config(self) -> dict:
        return self._config

    @override
    def build_key(
        self,
        id: Optional[str] = None,
        name: Optional[str] = None,
    ) -> GXCloudIdentifier:
        """Get the store backend specific implementation of the key. ignore resource_type since it is defined when initializing the cloud store backend."""  # noqa: E501
        return GXCloudIdentifier(
            resource_type=self.ge_cloud_resource_type,
            id=id,
            resource_name=name,
        )

    @override
    def _validate_key(self, key) -> None:
        if not isinstance(key, tuple) or len(key) != 3:  # noqa: PLR2004
            raise TypeError(  # noqa: TRY003
                "Key used for GXCloudStoreBackend must contain a resource_type, id, and resource_name; see GXCloudIdentifier for more information."  # noqa: E501
            )

        resource_type, _id, _resource_name = key
        try:
            GXCloudRESTResource(resource_type)
        except ValueError as e:
            raise TypeError(  # noqa: TRY003
                f"The provided resource_type {resource_type} is not a valid GXCloudRESTResource"
            ) from e

    @classmethod
    def construct_versioned_url(
        cls,
        base_url: str,
        organization_id: str,
        resource_name: str,
        id: Optional[str] = None,
    ) -> str:
        """Construct the correct url for a given resource."""
        version = cls._ENDPOINT_VERSION_LOOKUP.get(resource_name, EndpointVersion.V0)

        if version == EndpointVersion.V1:
            url = urljoin(
                base_url,
                f"api/v1/organizations/{organization_id}/{hyphen(resource_name)}",
            )
        else:  # default to EndpointVersion.V0
            url = urljoin(
                base_url,
                f"organizations/{organization_id}/{hyphen(resource_name)}",
            )

        if id:
            url = f"{url}/{id}"

        return url

    @classmethod
    def construct_versioned_payload(
        cls,
        resource_type: str,
        organization_id: str,
        attributes_key: str,
        attributes_value: Union[dict, Any],
        resource_id: str | None = None,
        **kwargs: dict,
    ) -> dict:
        """Construct the correct payload for the cloud backend.

        Arguments `resource_type`, `resource_id`, and `attributes_value` of type Any
        are deprecated in GX V1, and are only required for resources still using V0 endpoints.
        """
        version = cls._ENDPOINT_VERSION_LOOKUP.get(resource_type, EndpointVersion.V0)
        if version == EndpointVersion.V1:
            if isinstance(attributes_value, dict):
                payload = {**attributes_value, **kwargs}
            elif attributes_value is None:
                payload = kwargs
            else:
                raise TypeError(  # noqa: TRY003
                    f"Parameter attributes_value of type {type(attributes_value)}"
                    f" is unsupported in GX V1."
                )

            return cls._construct_json_payload_v1(payload=payload)
        else:
            return cls._construct_json_payload_v0(
                resource_type=resource_type,
                organization_id=organization_id,
                attributes_key=attributes_key,
                attributes_value=attributes_value,
                resource_id=resource_id,
                **kwargs,
            )

    @classmethod
    def _construct_json_payload_v0(
        cls,
        resource_type: str,
        organization_id: str,
        attributes_key: str,
        attributes_value: Any,
        resource_id: str | None,
        **kwargs: dict,
    ) -> dict:
        data = {
            "data": {
                "type": resource_type,
                "attributes": {
                    "organization_id": organization_id,
                    attributes_key: attributes_value,
                    **kwargs,
                },
            }
        }
        if resource_id:
            data["data"]["id"] = resource_id
        return data

    @classmethod
    def _construct_json_payload_v1(
        cls,
        payload: dict,
    ) -> dict:
        return {
            "data": {
                **payload,
            }
        }

    @property
    def _is_v1_resource(self) -> bool:
        return self._ENDPOINT_VERSION_LOOKUP.get(self.ge_cloud_resource_type) == EndpointVersion.V1


def close_session(session: requests.Session):
    """Close the session.
    Used by a finalizer to close the session when the GXCloudStoreBackend is garbage collected.

    This is not a bound method on the GXCloudStoreBackend class because of this note
    in the Python docs (https://docs.python.org/3.11/library/weakref.html#weakref.finalize):
    Note It is important to ensure that func, args and kwargs do not own any references to obj,
    either directly or indirectly, since otherwise obj will never be garbage collected.
    In particular, func should not be a bound method of obj.

    """
    session.close()
