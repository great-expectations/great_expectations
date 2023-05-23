import logging

logger = logging.getLogger(__name__)

from great_expectations.core.id_dict import BatchKwargs
from great_expectations.render.renderer.renderer import Renderer


class OpsgenieRenderer(Renderer):
    def __init__(self) -> None:
        super().__init__()

    def render(
        self,
        validation_result=None,
        data_docs_pages=None,
        notify_with=None,
    ):
        summary_text = (
            "No validation occurred. Please ensure you passed a validation_result."
        )
        status = "Failed ❌"

        if validation_result:
            expectation_suite_name = validation_result.meta.get(
                "expectation_suite_name", "__no_expectation_suite_name__"
            )

            if "batch_kwargs" in validation_result.meta:
                data_asset_name = validation_result.meta["batch_kwargs"].get(
                    "data_asset_name", "__no_data_asset_name__"
                )
            elif "active_batch_definition" in validation_result.meta:
                data_asset_name = (
                    validation_result.meta["active_batch_definition"].data_asset_name
                    if validation_result.meta["active_batch_definition"].data_asset_name
                    else "__no_data_asset_name__"
                )
            else:
                data_asset_name = "__no_data_asset_name__"

            n_checks_succeeded = validation_result.statistics["successful_expectations"]
            n_checks = validation_result.statistics["evaluated_expectations"]
            run_id = validation_result.meta.get("run_id", "__no_run_id__")
            batch_id = BatchKwargs(
                validation_result.meta.get("batch_kwargs", {})
            ).to_id()
            check_details_text = "{} of {} expectations were met".format(
                n_checks_succeeded, n_checks
            )

            if validation_result.success:
                status = "Success 🎉"

            summary_text = f"""Batch Validation Status: {status}
Expectation suite name: {expectation_suite_name}
Data asset name: {data_asset_name}
Run ID: {run_id}
Batch ID: {batch_id}
Summary: {check_details_text}"""

        return summary_text

    def _custom_blocks(self, evr):
        return None

    def _get_report_element(self, docs_link):
        return None
