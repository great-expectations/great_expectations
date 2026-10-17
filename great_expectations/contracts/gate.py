from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Callable, Sequence

if TYPE_CHECKING:
    from great_expectations.checkpoint.checkpoint import CheckpointResult

logger = logging.getLogger(__name__)


class DataContractValidationError(Exception):
    """Raised when data validation against a data contract fails at a checkpoint gate.

    Attributes:
        result: The CheckpointResult containing validation failure details.
        failed_batch_ids: The list of batch IDs that failed validation.
        message: Human-readable failure explanation.
    """

    def __init__(
        self,
        message: str,
        result: CheckpointResult | None = None,
        failed_batch_ids: Sequence[str] | None = None,
    ) -> None:
        super().__init__(message)
        self.result = result
        self.failed_batch_ids = list(failed_batch_ids or [])


def get_failed_batch_ids(result: CheckpointResult) -> list[str]:
    """Extract all batch IDs that failed validation from a CheckpointResult.

    Args:
        result: The CheckpointResult returned by checkpoint.run().

    Returns:
        A list of failed batch IDs.
    """
    failed_batch_ids: set[str] = set()
    for run_result in result.run_results.values():
        val_result = getattr(run_result, "validation_result", None) or run_result
        success = getattr(val_result, "success", True)
        if not success:
            meta = getattr(val_result, "meta", {}) or {}
            batch_id = meta.get("batch_id")
            if not batch_id:
                active_batch_def = meta.get("active_batch_definition")
                if active_batch_def:
                    batch_id = getattr(active_batch_def, "batch_identifiers", {}).get(
                        "batch_id"
                    ) or str(active_batch_def)
            if batch_id:
                failed_batch_ids.add(str(batch_id))
            else:
                run_id = getattr(result, "run_id", None)
                if run_id:
                    failed_batch_ids.add(str(run_id))
    return sorted(failed_batch_ids)


def enforce_contract_gate(
    result: CheckpointResult,
    raise_error: bool = True,
    quarantine_callback: Callable[[list[str]], None] | None = None,
    error_message: str | None = None,
) -> list[str]:
    """Enforce a data contract quality gate on a CheckpointResult.

    If validation failed (`result.success == False`):
    1. Extracts all failed batch IDs.
    2. Invokes the optional `quarantine_callback(failed_batch_ids)` to quarantine bad data.
    3. Raises `DataContractValidationError` if `raise_error` is True
       (blocking downstream processing).

    Example usage in a data pipeline:

    ```python
    import great_expectations as gx

    context = gx.get_context()
    suite = gx.contracts.from_odcs("orders_contract.yaml", context=context)

    # Run checkpoint validation definition
    checkpoint_result = checkpoint.run()

    # Quarantine bad records and fail the pipeline
    def quarantine_handler(batch_ids: list[str]) -> None:
        quarantine_table = "quarantine_orders"
        print(f"Quarantining batches {batch_ids} into {quarantine_table}")

    gx.contracts.enforce_contract_gate(
        checkpoint_result,
        raise_error=True,
        quarantine_callback=quarantine_handler,
    )
    ```

    Args:
        result: The CheckpointResult from running a Checkpoint.
        raise_error: Whether to raise DataContractValidationError on failure. Defaults to True.
        quarantine_callback: Optional callable accepting a list of failed batch IDs
            for quarantine handling.
        error_message: Optional custom error message.

    Returns:
        List of failed batch IDs (empty if validation succeeded).
    """
    if result.success:
        logger.info("Data contract gate passed: all validations succeeded.")
        return []

    failed_batch_ids = get_failed_batch_ids(result)
    msg = error_message or (
        f"Data contract gate failed! Success is False. "
        f"Failed batches: {failed_batch_ids or 'None identified'}"
    )
    logger.warning(msg)

    if quarantine_callback is not None:
        logger.info(f"Invoking quarantine callback for failed batches: {failed_batch_ids}")
        quarantine_callback(failed_batch_ids)

    if raise_error:
        raise DataContractValidationError(
            message=msg,
            result=result,
            failed_batch_ids=failed_batch_ids,
        )

    return failed_batch_ids
