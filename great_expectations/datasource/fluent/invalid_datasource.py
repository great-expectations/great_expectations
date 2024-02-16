from __future__ import annotations

from typing import ClassVar, List, Type, overload

from great_expectations.compatibility import pydantic
from great_expectations.compatibility.pydantic import Field
from great_expectations.compatibility.typing_extensions import override
from great_expectations.datasource.fluent import (
    DataAsset,
    Datasource,
    GxDatasourceWarning,
    TestConnectionError,
)
from great_expectations.datasource.fluent.type_lookup import TypeLookup, ValidTypes


class GxInvalidDatasourceWarning(GxDatasourceWarning):
    """
    A warning that the Datasource configuration is invalid and will must be updated before it can used.
    """


class InvalidAsset(DataAsset):
    """A DataAsset that is invalid."""

    type: str = "invalid"  # TODO: ensure this isn't registered as a real data asset
    name: str = "invalid"

    class Config:
        extra = "allow"
        arbitrary_types_allowed = True


class InvalidAssetTypeLookup(TypeLookup):
    """A TypeLookup that always returns InvalidAsset for any type."""

    @overload
    def __getitem__(self, key: str) -> Type:
        ...

    @overload
    def __getitem__(self, key: Type) -> str:
        ...

    @override
    def __getitem__(self, key: ValidTypes) -> ValidTypes:
        if isinstance(key, str):
            return InvalidAsset
        return "invalid"


class InvalidDatasource(Datasource):
    """A Datasource that is invalid.

    This is used to represent a Datasource that is invalid and cannot be used.
    """

    # class var definitions
    asset_types: ClassVar[List[Type[DataAsset]]] = [InvalidAsset]
    _type_lookup: ClassVar[TypeLookup] = InvalidAssetTypeLookup()
    type: str = "invalid"
    config_error: pydantic.ValidationError = Field(
        ..., description="The error that caused the Datasource to be invalid."
    )
    assets: List[InvalidAsset] = []

    class Config:
        extra = "allow"
        arbitrary_types_allowed = True
        json_encoders = {
            pydantic.ValidationError: lambda v: v.errors(),
        }

    # TODO: override more methods to raise helpful errors

    @override
    def test_connection(self, test_assets: bool = True) -> None:
        raise TestConnectionError(
            "This Datasource configuration is invalid and cannot be used. Please fix the error and try again"
        ) from self.config_error
