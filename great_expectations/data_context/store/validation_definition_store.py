from __future__ import annotations

import json
import uuid
from typing import TYPE_CHECKING, Any

from great_expectations.compatibility.pydantic import ErrorWrapper, ValidationError
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.data_context_key import DataContextKey, StringKey
from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.store.store import Store
from great_expectations.data_context.store.tuple_store_backend import TupleStoreBackend
from great_expectations.data_context.types.resource_identifiers import (
    GXCloudIdentifier,
)

if TYPE_CHECKING:
    from great_expectations.core.validation_definition import ValidationDefinition
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )


class ValidationDefinitionStore(Store):
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
        """Given a name and optional ID, build the correct key for use in the ValidationDefinitionStore."""  # noqa: E501 # FIXME CoP
        if self.cloud_mode:
            return GXCloudIdentifier(
                resource_type=GXCloudRESTResource.VALIDATION_DEFINITION,
                id=id,
                resource_name=name,
            )
        return StringKey(key=name)

    @override
    @staticmethod
    def gx_cloud_response_json_to_object_dict(response_json: dict) -> dict:
        response_data = response_json["data"]

        validation_data: dict
        if isinstance(response_data, list):
            if len(response_data) != 1:
                if len(response_data) == 0:
                    msg = f"Cannot parse empty data from GX Cloud payload: {response_json}"
                else:
                    msg = f"Cannot parse multiple items from GX Cloud payload: {response_json}"
                raise ValueError(msg)
            validation_data = response_data[0]
        else:
            validation_data = response_data

        return validation_data

    @override
    @staticmethod
    def _convert_raw_json_to_object_dict(data: dict) -> dict:
        return data

    @override
    def serialize(self, value):
        # In order to enable the custom json_encoders in ValidationDefinition, we need to set `models_as_dict` off  # noqa: E501 # FIXME CoP
        # Ref: https://docs.pydantic.dev/1.10/usage/exporting_models/#serialising-self-reference-or-other-models
        output = value.json(models_as_dict=False, indent=2, sort_keys=True)

        if self.cloud_mode:
            output_dict = json.loads(output)
            output_dict.pop("id", None)
            return output_dict
        else:
            return output

    @override
    def deserialize(self, value):
        from great_expectations.core.validation_definition import ValidationDefinition

        if self._data_context is None:
            if self.cloud_mode:
                return ValidationDefinition.parse_obj(value)

            return ValidationDefinition.parse_raw(value)

        # A store built by a Data Context resolves the suite and the batch definition a record
        # names through that context, not through whichever context is current (#12209).
        data = self._load_record(value)
        if not isinstance(data, dict):
            # Not a record at all: let pydantic raise the error it always has.
            return ValidationDefinition.parse_obj(data)

        self._resolve_references(data)
        return ValidationDefinition.parse_obj(data)

    def _load_record(self, value):
        from great_expectations.core.validation_definition import ValidationDefinition

        if self.cloud_mode:
            return dict(value) if isinstance(value, dict) else value
        try:
            return json.loads(value)
        except (ValueError, TypeError) as e:
            raise ValidationError([ErrorWrapper(e, loc="__root__")], ValidationDefinition) from e

    def _resolve_references(self, data: dict) -> None:
        """Replace the record's `data` and `suite` identifiers with objects from this context.

        Fields are decoded in the model's field order and every failed field is reported
        together, as the field validators do.
        """
        from great_expectations.core.validation_definition import ValidationDefinition

        decoders = (
            ("data", ValidationDefinition._decode_data),
            ("suite", ValidationDefinition._decode_suite),
        )
        errors: list[ErrorWrapper] = []
        for field, decode in decoders:
            if not isinstance(data.get(field), dict):
                continue
            try:
                data[field] = decode(data[field], context=self._data_context)
            except (ValueError, TypeError, AssertionError) as e:
                # Pydantic validators wrap these three; so does this seam, so callers and
                # Store.get_all see the error they always have.
                errors.append(ErrorWrapper(e, loc=field))
        if errors:
            raise self._validation_error(data, errors)

    @staticmethod
    def _validation_error(data: dict, resolution_errors: list[ErrorWrapper]) -> ValidationError:
        """The error parsing `data` raises when its suite or batch definition cannot be resolved.

        Pydantic reports every failed field, in field order. The fields that failed to resolve
        are left out of the record, the rest are parsed to collect their errors, and pydantic's
        "field required" errors at the omitted fields are dropped in favour of the resolution
        errors.
        """
        from great_expectations.core.validation_definition import ValidationDefinition

        field_names = list(ValidationDefinition.__fields__)

        def field_of(raw_error: Any) -> str:
            while isinstance(raw_error, (list, tuple)):
                raw_error = raw_error[0]
            return str(raw_error.loc_tuple()[0])

        def position(raw_error: Any) -> int:
            name = field_of(raw_error)
            return field_names.index(name) if name in field_names else len(field_names)

        failed = {field_of(error) for error in resolution_errors}
        try:
            ValidationDefinition.parse_obj({k: v for k, v in data.items() if k not in failed})
        except ValidationError as other:
            raw_errors = [e for e in other.raw_errors if field_of(e) not in failed]
        else:
            raw_errors = []
        return ValidationError(
            sorted([*raw_errors, *resolution_errors], key=position), ValidationDefinition
        )

    @override
    def _add(self, key: DataContextKey, value: ValidationDefinition, **kwargs):
        if not self.cloud_mode:
            # this logic should move to the store backend, but is implemented here for now
            value.id = str(uuid.uuid4())
        return super()._add(key=key, value=value, **kwargs)
