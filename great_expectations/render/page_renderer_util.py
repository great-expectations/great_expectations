import warnings

from great_expectations.render.renderer import ValidationResultsPageRenderer
from great_expectations.render.view import DefaultMarkdownPageView
from great_expectations.validation_operators.types.validation_operator_result import (
    ValidationOperatorResult,
)


def render_multiple_validation_result_pages_markdown(
    validation_operator_result: ValidationOperatorResult, run_info_at_end: bool = True
) -> str:
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "\n    Loop through and render multiple validation results to markdown.\n    Args:\n        validation_operator_result: (ValidationOperatorResult) Result of validation operator run\n        run_info_at_end: move run info below expectation results\n    Returns:\n        string containing formatted markdown validation results\n    "
    warnings.warn(
        "This 'render_multiple_validation_result_pages_markdown' function is deprecated as of v0.12.1 and will be removed in v0.16.Please use ValidationResultsPageRenderer.render_validation_operator_result() instead.E.g. to replicate the functionality of rendering a ValidationOperatorResult to markdown:validation_results_page_renderer = ValidationResultsPageRenderer(    run_info_at_end=run_info_at_end)rendered_document_content_list = validation_results_page_renderer.render_validation_operator_result(   validation_operator_result=validation_operator_result)return \" \".join(DefaultMarkdownPageView().render(rendered_document_content_list))",
        DeprecationWarning,
    )
    validation_results_page_renderer = ValidationResultsPageRenderer(
        run_info_at_end=run_info_at_end
    )
    rendered_document_content_list = (
        validation_results_page_renderer.render_validation_operator_result(
            validation_operator_result=validation_operator_result
        )
    )
    return " ".join(DefaultMarkdownPageView().render(rendered_document_content_list))
