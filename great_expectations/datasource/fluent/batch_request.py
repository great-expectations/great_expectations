from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    Callable,
    Dict,
    Mapping,
    Optional,
    Union,
)

import pydantic
from pydantic import StrictStr
from typing_extensions import TypeAlias

from great_expectations.core._docs_decorators import public_api
from great_expectations.datasource.data_connector.batch_filter import (
    BatchSlice,
    parse_batch_slice,
)
from great_expectations.datasource.fluent.config import JSON_ENCODERS

if TYPE_CHECKING:
    MappingIntStrAny = Mapping[Union[int, str], Any]
    AbstractSetIntStr = AbstractSet[Union[int, str]]


# BatchRequestOptions is a dict that is composed into a BatchRequest that specifies the
# Batches one wants as returned. The keys represent dimensions one can filter the data along
# and the values are the realized. If a value is None or unspecified, the batch_request
# will capture all data along this dimension. For example, if we have a year and month
# splitter, and we want to query all months in the year 2020, the batch request options
# would look like:
#   options = { "year": 2020 }
BatchRequestOptions: TypeAlias = Dict[str, Any]


@public_api
class BatchRequest(pydantic.BaseModel):
    """A BatchRequest is the way to specify which data Great Expectations will validate.

    A Batch Request is provided to a Data Asset in order to create one or more Batches.

    Args:
        datasource_name: The name of the Datasource used to connect to the data.
        data_asset_name: The name of the Data Asset used to connect to the data.
        options: A dict that can be used to filter the batch groups associated with the Data Asset.
            The dict structure depends on the asset type. The available keys for dict can be obtained by
            calling DataAsset.batch_request_options.
        batch_slice: A python slice that can be used to limit the sorted batches by index.
            e.g. `batch_slice = "[-5:]"` will request only the last 5 batches after the options filter is applied.

    Returns:
        BatchRequest
    """

    datasource_name: StrictStr
    data_asset_name: StrictStr
    options: BatchRequestOptions = pydantic.Field(default_factory=dict)
    batch_slice_input: Optional[BatchSlice] = pydantic.Field(
        default=None,
        alias="batch_slice",
    )

    @property
    def batch_slice(self) -> slice:
        return parse_batch_slice(batch_slice=self.batch_slice_input)

    class Config:
        allow_mutation = False
        arbitrary_types_allowed = True
        extra = pydantic.Extra.ignore
        json_encoders = JSON_ENCODERS
        validate_assignment = True

    # @classmethod
    # def __modify_schema__(
    #     cls, field_schema: Dict[str, Any], field: Optional[pydantic.fields.ModelField]
    # ):
    #     if field:
    #         batch_slice_input_slice = field.field_info.extra['batch_slice_input_slice']
    #         field_schema['examples'] = [c * 3 for c in batch_slice_input_slice]

    @pydantic.validator("options")
    def _validate_options(cls, options):
        if any(not isinstance(key, str) for key in options):
            raise pydantic.ValidationError(
                "BatchRequestOptions keys must all be strings."
            )
        return options

    def json(
        self,
        *,
        include: Optional[Union[AbstractSetIntStr, MappingIntStrAny]] = None,
        exclude: Optional[Union[AbstractSetIntStr, MappingIntStrAny]] = None,
        # Default to True to serialize the batch_slice_input with the name batch_slice
        by_alias: bool = True,
        skip_defaults: Optional[bool] = None,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        encoder: Optional[Callable[[Any], Any]] = None,
        models_as_dict: bool = True,
        **dumps_kwargs: Any,
    ) -> str:
        """
        Generate a json representation of the model, optionally specifying which
        fields to include or exclude.

        Deviates from pydantic
          - `by_alias` `True` by default instead of `False` by default.
        """
        return super().json(
            include=include,
            exclude=exclude,
            by_alias=by_alias,
            skip_defaults=skip_defaults,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
            encoder=encoder,
            models_as_dict=models_as_dict,
            **dumps_kwargs,
        )

    def dict(
        self,
        *,
        include: AbstractSetIntStr | MappingIntStrAny | None = None,
        exclude: AbstractSetIntStr | MappingIntStrAny | None = None,
        # Default to True to serialize the batch_slice_input with the name batch_slice
        by_alias: bool = True,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        # deprecated - use exclude_unset instead
        skip_defaults: bool | None = None,
    ) -> dict[str, Any]:
        """
        Generate a dictionary representation of the model, optionally specifying which
        fields to include or exclude.

        Deviates from pydantic
          - `by_alias` `True` by default instead of `False` by default.
        """
        return super().dict(
            include=include,
            exclude=exclude,
            by_alias=by_alias,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
            skip_defaults=skip_defaults,
        )
