from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from great_expectations.compatibility.pydantic import BaseModel
from great_expectations.data_context.store import (
    CheckpointStore,  # noqa: TCH001
    ExpectationsStore,  # noqa: TCH001
    SuiteParameterStore,  # noqa: TCH001
    ValidationDefinitionStore,  # noqa: TCH001
    ValidationResultsStore,  # noqa: TCH001
)
from great_expectations.data_context.types.base import DataContextConfigDefaults

if TYPE_CHECKING:
    from great_expectations.data_context.store.store import Store


class StoreManager(BaseModel):
    """
    A simple abstraction for managing multiple stores within a DataContext.

    We use this to ensure that all stores are properly initialized and accessible.
    Note that this class is not intended to be used directly by users.
    A user can only have a single store of each type within a DataContext.
    """

    class Config:
        arbitrary_types_allowed = True

    expectations_store: ExpectationsStore
    checkpoint_store: CheckpointStore
    validation_results_store: ValidationResultsStore
    validation_definition_store: ValidationDefinitionStore
    suite_parameter_store: SuiteParameterStore

    # NOTE: These are only kept around for legacy purposes
    #       We should no longer refer to stores by name
    expectations_store_name: ClassVar[str] = (
        DataContextConfigDefaults.DEFAULT_EXPECTATIONS_STORE_NAME.value
    )
    checkpoint_store_name: ClassVar[str] = (
        DataContextConfigDefaults.DEFAULT_CHECKPOINT_STORE_NAME.value
    )
    validation_results_store_name: ClassVar[str] = (
        DataContextConfigDefaults.DEFAULT_VALIDATIONS_STORE_NAME.value
    )
    validation_definition_store_name: ClassVar[str] = (
        DataContextConfigDefaults.DEFAULT_VALIDATION_DEFINITION_STORE_NAME.value
    )
    suite_parameter_store_name: ClassVar[str] = (
        DataContextConfigDefaults.DEFAULT_SUITE_PARAMETER_STORE_NAME.value
    )

    # NOTE: Subscriptable access is only kept around for legacy purposes
    #       We should not allow for dictionary-like behavior here
    def __getitem__(self, item: str) -> Store:
        try:
            return getattr(self, item)
        except AttributeError as e:
            msg = (
                f"Store with name {item} not found;"
                f"{self.__class__.__name__} only supports the following: {self.__fields__.keys()}"
            )
            raise KeyError(msg) from e
