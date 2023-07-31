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
from pydantic.json import pydantic_encoder
from pydantic.schema import default_ref_template

from great_expectations.core._docs_decorators import public_api
from great_expectations.datasource.data_connector.batch_filter import (
    BatchSlice,
    parse_batch_slice,
)

if TYPE_CHECKING:
    from typing_extensions import TypeAlias

    MappingIntStrAny = Mapping[Union[int, str], Any]
    AbstractSetIntStr = AbstractSet[Union[int, str]]


# BatchRequestOptions is a dict that is composed into a BatchRequest that specifies the
# Batches one wants as returned. The keys represent dimensions one can filter the data along
# and the values are the realized. If a value is None or unspecified, the batch_request
# will capture all data along this dimension. For example, if we have a year and month
# splitter, and we want to query all months in the year 2020, the batch request options
# would look like:
#   options = { "year": 2020 }
BatchRequestOptions: TypeAlias = Dict[StrictStr, Any]


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
        batch_slice: A python slice that can be used to filter the sorted batches by index.
            e.g. `batch_slice = "[-5:]"` will request only the last 5 batches after the options filter is applied.

    Returns:
        BatchRequest
    """

    datasource_name: StrictStr = pydantic.Field(
        ...,
        allow_mutation=False,
        description="The name of the Datasource used to connect to the data.",
    )
    data_asset_name: StrictStr = pydantic.Field(
        ...,
        allow_mutation=False,
        description="The name of the Data Asset used to connect to the data.",
    )
    options: BatchRequestOptions = pydantic.Field(
        default_factory=dict,
        allow_mutation=True,
        description=(
            "A map that can be used to filter the batch groups associated with the Data Asset. "
            "The structure and types depends on the asset type."
        ),
    )
    _batch_slice_input: Optional[BatchSlice] = pydantic.PrivateAttr(
        default=None,
    )

    def __init__(self, **kwargs) -> None:
        _batch_slice_input: Optional[BatchSlice] = None
        if "batch_slice" in kwargs:
            _batch_slice_input = kwargs.pop("batch_slice")
        super().__init__(**kwargs)
        self._batch_slice_input = _batch_slice_input

    @property
    def batch_slice(self) -> slice:
        """A built-in slice that can be used to filter a list of batches by index."""
        return parse_batch_slice(batch_slice=self._batch_slice_input)

    @public_api
    def update_batch_slice(self, value: Optional[BatchSlice] = None) -> None:
        """Updates the batch_slice on this BatchRequest.

        Args:
            value: The new value to be parsed into a python slice and set on the batch_slice attribute.

        Returns:
            None
        """
        try:
            parse_batch_slice(batch_slice=value)
        except (TypeError, ValueError) as e:
            raise ValueError(f"Failed to parse BatchSlice to slice: {e}")
        self._batch_slice_input = value

    class Config:
        extra = pydantic.Extra.forbid
        property_set_methods = {"batch_slice": "update_batch_slice"}
        validate_assignment = True

    def __setattr__(self, key, val):
        method = self.__config__.property_set_methods.get(key)
        if method is None:
            super().__setattr__(key, val)
        else:
            getattr(self, method)(val)

    @pydantic.validator("options", pre=True)
    def _validate_options(cls, options) -> BatchRequestOptions:
        if options is None:
            return {}
        if not isinstance(options, dict):
            raise TypeError("BatchRequestOptions must take the form of a dictionary.")
        if any(not isinstance(key, str) for key in options):
            raise TypeError("BatchRequestOptions keys must all be strings.")
        return options

    @public_api
    def json(  # noqa: PLR0913
        self,
        *,
        include: Optional[Union[AbstractSetIntStr, MappingIntStrAny]] = None,
        exclude: Optional[Union[AbstractSetIntStr, MappingIntStrAny]] = None,
        by_alias: bool = False,
        skip_defaults: Optional[bool] = None,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        encoder: Optional[Callable[[Any], Any]] = None,
        models_as_dict: bool = True,
        **dumps_kwargs: Any,
    ) -> str:
        """
        Generate a json representation of the BatchRequest, optionally specifying which
        fields to include or exclude.
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

    @public_api
    def dict(  # noqa: PLR0913
        self,
        *,
        include: AbstractSetIntStr | MappingIntStrAny | None = None,
        exclude: AbstractSetIntStr | MappingIntStrAny | None = None,
        by_alias: bool = False,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        # deprecated - use exclude_unset instead
        skip_defaults: bool | None = None,
    ) -> dict[str, Any]:
        """
        Generate a dictionary representation of the BatchRequest, optionally specifying which
        fields to include or exclude.
        """
        # batch_slice is only a property/pydantic setter, so we need to add a field
        # if we want it to show up in dict() with the _batch_request_input
        self.__fields__["batch_slice"] = pydantic.fields.ModelField(
            name="batch_slice",
            type_=Optional[BatchSlice],  # type: ignore[arg-type]
            required=False,
            default=None,
            model_config=self.__config__,
            class_validators=None,
        )
        property_set_methods = self.__config__.property_set_methods  # type: ignore[attr-defined]
        self.__config__.property_set_methods = {}  # type: ignore[attr-defined]
        self.__setattr__("batch_slice", self._batch_slice_input)
        result = super().dict(
            include=include,
            exclude=exclude,
            by_alias=by_alias,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
            skip_defaults=skip_defaults,
        )
        # revert model changes
        self.__config__.property_set_methods = property_set_methods  # type: ignore[attr-defined]
        self.__fields__.pop("batch_slice")
        return result

    @classmethod
    def schema_json(
        cls,
        *,
        by_alias: bool = True,
        ref_template: str = default_ref_template,
        **dumps_kwargs: Any,
    ) -> str:
        # batch_slice is only a property/pydantic setter, so we need to add a field
        # if we want its definition to show up in schema_json()
        cls.__fields__["batch_slice"] = pydantic.fields.ModelField(
            name="batch_slice",
            type_=Optional[BatchSlice],  # type: ignore[arg-type]
            required=False,
            default=None,
            model_config=cls.__config__,
            class_validators=None,
        )
        result = cls.__config__.json_dumps(
            cls.schema(by_alias=by_alias, ref_template=ref_template),
            default=pydantic_encoder,
            **dumps_kwargs,
        )
        # revert model changes
        cls.__fields__.pop("batch_slice")
        return result
