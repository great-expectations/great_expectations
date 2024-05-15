from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from great_expectations.compatibility.typing_extensions import override

logger = logging.getLogger(__name__)

from great_expectations.render.renderer.renderer import Renderer

if TYPE_CHECKING:
    from great_expectations.checkpoint.checkpoint import CheckpointResult
    from great_expectations.core.expectation_validation_result import (
        ExpectationSuiteValidationResult,
    )


class OpsgenieRenderer(Renderer):
    @override
    def render(self, checkpoint_result: CheckpointResult):
        text_blocks: list[str] = []
        for run_result in checkpoint_result.run_results.values():
            text_block = self._render_validation_result(result=run_result)
            text_blocks.append(text_block)

        return self._concatenate_text_blocks(
            checkpoint_result=checkpoint_result, text_blocks=text_blocks
        )

    def _render_validation_result(self, result: ExpectationSuiteValidationResult) -> str:
        suite_name = result.suite_name
        data_asset_name = result.asset_name or "__no_data_asset_name__"
        n_checks_succeeded = result.statistics["successful_expectations"]
        n_checks = result.statistics["evaluated_expectations"]
        run_id = result.meta.get("run_id", "__no_run_id__")
        batch_id = result.batch_id or "__no_batch_id__"
        check_details_text = f"{n_checks_succeeded} of {n_checks} expectations were met"

        if result.success:
            status = "Success 🎉"
        else:
            status = "Failed ❌"

        return f"""Batch Validation Status: {status}
Expectation Suite Name: {suite_name}
Data Asset Name: {data_asset_name}
Run ID: {run_id}
Batch ID: {batch_id}
Summary: {check_details_text}"""

    def _concatenate_text_blocks(
        self, checkpoint_result: CheckpointResult, text_blocks: list[str]
    ) -> str:
        checkpoint_name = checkpoint_result.checkpoint_config.name
        success = checkpoint_result.success
        run_id = checkpoint_result.run_id.run_time

        title = f"Checkpoint: {checkpoint_name} - Run ID: {run_id}"
        status = "Status: Failed ❌" if not success else "Status: Success 🎉"
        return f"{title}\n{status}\n\n" + "\n\n".join(text_blocks)

    def _custom_blocks(self, evr):
        return None

    def _get_report_element(self, docs_link):
        return None
