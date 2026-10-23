from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING

from great_expectations._docs_decorators import public_api
from great_expectations.contracts.errors import DataContractValidationError

if TYPE_CHECKING:
    from great_expectations.checkpoint.checkpoint import CheckpointResult


@public_api
def enforce_contract_gate(
    result: CheckpointResult,
    *,
    on_failure: Callable[[Sequence[str]], None] | None = None,
) -> None:
    """Raise when a checkpoint result violates its data contract.

    This is a small, explicit gate for scripts and pipelines that need a failed
    validation to stop execution. It is not a ValidationAction; call it after
    ``checkpoint.run()`` and let the raised error stop the caller.

    Args:
        result: The result returned by ``checkpoint.run()``.
        on_failure: Optional callback invoked with the failed batch identifiers
            before the error is raised. Use it to quarantine or report the
            affected batches.

    Raises:
        DataContractValidationError: If any validation in the checkpoint failed.
    """
    if result.success:
        return

    failed_batch_ids = sorted(
        {
            str(run_result.batch_id or identifier.batch_identifier)
            for identifier, run_result in result.run_results.items()
            if not run_result.success
            and (run_result.batch_id or identifier.batch_identifier) is not None
        }
    )

    if on_failure is not None:
        on_failure(failed_batch_ids)

    raise DataContractValidationError(result, failed_batch_ids)


__all__ = ["enforce_contract_gate"]
