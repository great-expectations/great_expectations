from great_expectations.render.renderer import ValidationResultsPageRenderer
from great_expectations.render.view import DefaultMarkdownPageView
from great_expectations.validation_operators.types.validation_operator_result import (
    ValidationOperatorResult,
)


def render_multiple_validation_result_pages_markdown(
    validation_operator_result: ValidationOperatorResult, run_info_at_end: bool = True,
):
    """
    Loop through and render multiple validation results to markdown.
    Args:
        validation_operator_result: (ValidationOperatorResult) Result of validation operator run
        run_info_at_end: move run info below expectation results

    Returns:
        string containing formatted markdown validation results

    """

    md_str = ""
    validation_results_page_renderer = ValidationResultsPageRenderer(
        run_info_at_end=run_info_at_end
    )
    for validation_result in validation_operator_result.list_validation_results():
        rendered_document_content = validation_results_page_renderer.render(
            validation_result
        )
        md_str += DefaultMarkdownPageView().render(rendered_document_content) + " "

    return md_str
