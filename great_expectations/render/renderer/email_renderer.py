from __future__ import annotations

import logging
import pathlib
import textwrap
import urllib
from typing import TYPE_CHECKING

from great_expectations.compatibility.typing_extensions import override

logger = logging.getLogger(__name__)

from great_expectations.render.renderer.renderer import Renderer

if TYPE_CHECKING:
    from great_expectations.core import RunIdentifier
    from great_expectations.core.expectation_validation_result import (
        ExpectationSuiteValidationResult,
    )
    from great_expectations.data_context.types.resource_identifiers import (
        ValidationResultIdentifier,
    )


class EmailRenderer(Renderer):
    @override
    def render(
        self,
        validation_result: ExpectationSuiteValidationResult,
        data_docs_pages: dict[ValidationResultIdentifier, dict[str, str]] | None = None,
        validation_result_urls: list[str] | None = None,
    ) -> tuple[str, str]:
        data_docs_pages = data_docs_pages or {}
        validation_result_urls = validation_result_urls or []

        text_blocks: list[str] = []
        description_block = self._build_description_block(result=validation_result)
        text_blocks.append(description_block)

        report_element_block = self._build_report_element_block(validation_result_urls)
        if report_element_block:
            text_blocks.append(report_element_block)

        return text_blocks

    def _build_description_block(self, result: ExpectationSuiteValidationResult) -> str:
        suite_name = result.suite_name
        asset_name = result.asset_name or "__no_asset_name__"
        n_checks_succeeded = result.statistics["successful_expectations"]
        n_checks = result.statistics["evaluated_expectations"]
        run_id = result.meta.get("run_id", "__no_run_id__")
        batch_id = result.batch_id
        check_details_text = f"<strong>{n_checks_succeeded}</strong> of <strong>{n_checks}</strong> expectations were met"  # noqa: E501
        status = "Success ✅" if result.success else "Failed ❌"

        html = textwrap.dedent(
            f"""\
            <p><strong>Batch Validation Status</strong>: {status}</p>
            <p><strong>Expectation Suite Name</strong>: {suite_name}</p>
            <p><strong>Data Asset Name</strong>: {asset_name}</p>
            <p><strong>Run ID</strong>: {run_id}</p>
            <p><strong>Batch ID</strong>: {batch_id}</p>
            <p><strong>Summary</strong>: {check_details_text}</p>"""
        )

        return html

    def concatenate_blocks(
        self,
        action_name: str,
        text_blocks: list[dict],
        success: bool,
        checkpoint_name: str,
        run_id: RunIdentifier,
    ) -> str:
        status = "Success ✅" if success else "Failed ❌"
        title = f"{action_name} - {checkpoint_name} - {status}"
        return title, "<br>".join(text_blocks)

    def _get_report_element(self, docs_link: str) -> str | None:
        report_element = None
        if docs_link:
            try:
                if "file:/" in docs_link:
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

    def _build_report_element_block(self, validation_result_urls: list[str]) -> str | None:
        for url in validation_result_urls:
            url = pathlib.Path(urllib.parse.unquote(url)).as_posix()
            report_element = self._get_report_element(url)
            return report_element
