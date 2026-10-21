from __future__ import annotations

import json
import logging
import os
import uuid
from typing import TYPE_CHECKING, Any

import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility.pydantic import ErrorWrapper, ValidationError
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.data_context_key import DataContextKey, StringKey
from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.store.store import Store
from great_expectations.data_context.store.tuple_store_backend import TupleStoreBackend
from great_expectations.data_context.types.base import DataContextConfigDefaults
from great_expectations.data_context.types.resource_identifiers import (
    GXCloudIdentifier,
)

if TYPE_CHECKING:
    from great_expectations.checkpoint.checkpoint import Checkpoint
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )

logger = logging.getLogger(__name__)


class CheckpointStore(Store):
    _key_class = StringKey

    def __init__(
        self,
        store_backend: dict | None = None,
        runtime_environment: dict | None = None,
        store_name: str = "no_store_name",
        data_context: AbstractDataContext | None = None,
    ) -> None:
        self._data_context = data_context
        store_backend_class = self._determine_store_backend_class(store_backend)
        if store_backend and issubclass(store_backend_class, TupleStoreBackend):
            store_backend["filepath_suffix"] = store_backend.get("filepath_suffix", ".json")

        super().__init__(
            store_backend=store_backend,
            runtime_environment=runtime_environment,
            store_name=store_name,
        )

    @property
    def data_context(self) -> AbstractDataContext | None:
        """The Data Context that built this store, or None for a directly constructed store."""
        return self._data_context

    def get_key(self, name: str, id: str | None = None) -> GXCloudIdentifier | StringKey:
        """Given a name and optional ID, build the correct key for use in the CheckpointStore."""
        if self.cloud_mode:
            return GXCloudIdentifier(
                resource_type=GXCloudRESTResource.CHECKPOINT,
                id=id,
                resource_name=name,
            )
        return self._key_class(key=name)

    @override
    @classmethod
    def gx_cloud_response_json_to_object_dict(cls, response_json: dict) -> dict:
        response_data = response_json["data"]

        checkpoint_data: dict
        if isinstance(response_data, list):
            if len(response_data) != 1:
                if len(response_data) == 0:
                    msg = f"Cannot parse empty data from GX Cloud payload: {response_json}"
                else:
                    msg = f"Cannot parse multiple items from GX Cloud payload: {response_json}"
                raise ValueError(msg)
            checkpoint_data = response_data[0]
        else:
            checkpoint_data = response_data

        return cls._convert_raw_json_to_object_dict(checkpoint_data)

    @override
    @staticmethod
    def _convert_raw_json_to_object_dict(data: dict) -> dict:
        return data

    @override
    def serialize(self, value):
        # In order to enable the custom json_encoders in Checkpoint, we need to set `models_as_dict` off  # noqa: E501 # FIXME CoP
        # Ref: https://docs.pydantic.dev/1.10/usage/exporting_models/#serialising-self-reference-or-other-models
        data = value.json(models_as_dict=False, indent=2, sort_keys=True, exclude_none=True)

        if self.cloud_mode:
            return json.loads(data)
        return data

    @override
    def deserialize(self, value):
        from great_expectations.checkpoint.checkpoint import Checkpoint

        if self._data_context is None:
            if self.cloud_mode:
                return Checkpoint.parse_obj(value)

            return Checkpoint.parse_raw(value)

        # A store built by a Data Context resolves the validation definitions a record names
        # through that context, not through whichever context is current (#12209).
        data = self._load_record(value)
        if not isinstance(data, dict):
            # Not a record at all: let pydantic raise the error it always has.
            return Checkpoint.parse_obj(data)

        definitions = data.get("validation_definitions")
        if not (isinstance(definitions, list) and definitions and isinstance(definitions[0], dict)):
            return Checkpoint.parse_obj(data)

        try:
            data["validation_definitions"] = self._resolve_validation_definitions(definitions)
        except (ValueError, TypeError, AssertionError) as e:
            # Pydantic validators wrap these three; so does this seam, so callers and
            # Store.get_all see the error they always have.
            raise self._validation_error(data, ErrorWrapper(e, loc="validation_definitions")) from e
        return Checkpoint.parse_obj(data)

    def _load_record(self, value):
        from great_expectations.checkpoint.checkpoint import Checkpoint

        if self.cloud_mode:
            return dict(value) if isinstance(value, dict) else value
        try:
            return json.loads(value)
        except (ValueError, TypeError) as e:
            raise ValidationError([ErrorWrapper(e, loc="__root__")], Checkpoint) from e

    def _resolve_validation_definitions(self, definitions: list) -> list:
        """Load each validation definition the record names from this context's store.

        The store's own context resolves each one's suite and batch definition in turn, so the
        whole nested load goes through one context. A miss keeps the plain message: the lookup
        did not go through the current context.
        """
        from great_expectations.checkpoint.checkpoint import Checkpoint, _IdentifierBundle

        assert self._data_context is not None
        identifier_bundles = [_IdentifierBundle(**v) for v in definitions]
        return Checkpoint._deserialize_identifier_bundles_to_validation_definitions(
            identifier_bundles=identifier_bundles,
            store=self._data_context.validation_definition_store,
        )

    @staticmethod
    def _validation_error(data: dict, resolution_error: ErrorWrapper) -> ValidationError:
        """The error parsing `data` raises when its validation definitions cannot be resolved.

        Pydantic reports every failed field, in field order. The field that failed to resolve
        is left out of the record, the rest are parsed to collect their errors, and pydantic's
        "field required" error at the omitted field is dropped in favour of the resolution error.
        """
        from great_expectations.checkpoint.checkpoint import Checkpoint

        field_names = list(Checkpoint.__fields__)

        def field_of(raw_error: Any) -> str:
            while isinstance(raw_error, (list, tuple)):
                raw_error = raw_error[0]
            return str(raw_error.loc_tuple()[0])

        def position(raw_error: Any) -> int:
            name = field_of(raw_error)
            return field_names.index(name) if name in field_names else len(field_names)

        failed = field_of(resolution_error)
        try:
            Checkpoint.parse_obj({k: v for k, v in data.items() if k != failed})
        except ValidationError as other:
            raw_errors = [e for e in other.raw_errors if field_of(e) != failed]
        else:
            raw_errors = []
        return ValidationError(sorted([*raw_errors, resolution_error], key=position), Checkpoint)

    @override
    def _add(self, key: DataContextKey, value: Checkpoint, **kwargs):
        if not self.cloud_mode:
            value.id = str(uuid.uuid4())
        return super()._add(key=key, value=value, **kwargs)

    @override
    def _update(self, key: DataContextKey, value: Checkpoint, **kwargs):
        try:
            super()._update(key=key, value=value, **kwargs)
        except gx_exceptions.StoreBackendError as e:
            name = key.to_tuple()[0]
            raise ValueError(f"Could not update Checkpoint '{name}'") from e  # noqa: TRY003 # FIXME CoP

    @staticmethod
    def default_checkpoints_exist(directory_path: str) -> bool:
        if not directory_path:
            return False

        checkpoints_directory_path: str = os.path.join(  # noqa: PTH118 # FIXME CoP
            directory_path,
            DataContextConfigDefaults.DEFAULT_CHECKPOINT_STORE_BASE_DIRECTORY_RELATIVE_NAME.value,
        )
        return os.path.isdir(checkpoints_directory_path)  # noqa: PTH112 # FIXME CoP
