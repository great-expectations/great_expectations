from __future__ import annotations

import logging
import textwrap
from typing import TYPE_CHECKING

from great_expectations.compatibility.typing_extensions import override

logger = logging.getLogger(__name__)

from great_expectations.render.renderer.renderer import Renderer

if TYPE_CHECKING:
    from great_expectations.checkpoint.checkpoint import CheckpointResult
    from great_expectations.core.expectation_validation_result import (
        ExpectationSuiteValidationResult,
    )


class EmailRenderer(Renderer):
    @override
    def render(self, checkpoint_result: CheckpointResult) -> tuple[str, str]:
        checkpoint_name = checkpoint_result.checkpoint_config.name
        status = checkpoint_result.success
        title = f"{checkpoint_name}: {status}"

        text_blocks: list[str] = []
        for result in checkpoint_result.run_results.values():
            html = self._render_validation_result(result=result)
            text_blocks.append(html)

        return title, self._concatenate_blocks(text_blocks=text_blocks)

    def _render_validation_result(self, result: ExpectationSuiteValidationResult) -> str:
        suite_name = result.suite_name
        asset_name = result.asset_name or "__no_asset_name__"
        n_checks_succeeded = result.statistics["successful_expectations"]
        n_checks = result.statistics["evaluated_expectations"]
        run_id = result.meta.get("run_id", "__no_run_id__")
        batch_id = result.batch_id
        check_details_text = f"<strong>{n_checks_succeeded}</strong> of <strong>{n_checks}</strong> expectations were met"  # noqa: E501
        status = "Success 🎉" if result.success else "Failed ❌"

        title = f"<h3><u>{suite_name}</u></h3>"
        html = textwrap.dedent(
            f"""\
            <p><strong>{title}</strong></p>
            <p><strong>Batch Validation Status</strong>: {status}</p>
            <p><strong>Expectation Suite Name</strong>: {suite_name}</p>
            <p><strong>Data Asset Name</strong>: {asset_name}</p>
            <p><strong>Run ID</strong>: {run_id}</p>
            <p><strong>Batch ID</strong>: {batch_id}</p>
            <p><strong>Summary</strong>: {check_details_text}</p>"""
        )

        return html

    def _concatenate_blocks(self, text_blocks: list[str]) -> str:
        return "<br>".join(text_blocks)

    def _get_report_element(self, docs_link):
        report_element = None
        if docs_link:
            try:
                if "file://" in docs_link:
                    # handle special case since the email does not render these links
                    report_element = str(
                        f'<p><strong>DataDocs</strong> can be found here: <a href="{docs_link}">{docs_link}</a>.</br>'  # noqa: E501
                        "(Please copy and paste link into a browser to view)</p>",
                    )
                else:
                    report_element = f'<p><strong>DataDocs</strong> can be found here: <a href="{docs_link}">{docs_link}</a>.</p>'  # noqa: E501
            except Exception as e:
                logger.warning(
                    f"""EmailRenderer had a problem with generating the docs link.
                    link used to generate the docs link is: {docs_link} and is of type: {type(docs_link)}.
                    Error: {e}"""  # noqa: E501
                )
                return
        else:
            logger.warning("No docs link found. Skipping data docs link in the email message.")
        return report_element
