from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence

from great_expectations._docs_decorators import public_api
from great_expectations.compatibility.typing_extensions import override
from great_expectations.exceptions.exceptions import GreatExpectationsError

if TYPE_CHECKING:
    from great_expectations.checkpoint.checkpoint import CheckpointResult


@public_api
class ODCSError(GreatExpectationsError):
    """Base class for errors raised while importing an ODCS data contract."""


@public_api
class ODCSContractError(ODCSError):
    """Raised when an ODCS contract is malformed or uses an unsupported construct."""


@public_api
class UnsupportedODCSRuleError(ODCSError):
    """Raised when a valid ODCS rule cannot be represented by a GX Expectation."""


@public_api
class DataContractValidationError(GreatExpectationsError):
    """Raised when a checkpoint run fails its data contract gate.

    Args:
        result: The failed checkpoint result.
        failed_batch_ids: Batch identifiers for the runs that failed.
    """

    def __init__(
        self,
        result: CheckpointResult,
        failed_batch_ids: Sequence[str],
    ) -> None:
        self.result = result
        self.failed_batch_ids = list(failed_batch_ids)
        if self.failed_batch_ids:
            message = "Data contract validation failed for batch(es): " + ", ".join(
                self.failed_batch_ids
            )
        else:
            message = "Data contract validation failed."
        super().__init__(message)

    @override
    def __reduce__(self) -> tuple[type[DataContractValidationError], tuple[Any, list[str]]]:
        return (self.__class__, (self.result, self.failed_batch_ids))


__all__ = [
    "DataContractValidationError",
    "ODCSContractError",
    "ODCSError",
    "UnsupportedODCSRuleError",
]
