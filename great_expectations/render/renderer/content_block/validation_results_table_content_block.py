import logging
import traceback
from copy import deepcopy

from great_expectations.expectations.core.expect_column_kl_divergence_to_be_less_than import (
    ExpectColumnKlDivergenceToBeLessThan,
)
from great_expectations.expectations.registry import get_renderer_impl
from great_expectations.render.renderer.content_block.expectation_string import (
    ExpectationStringRenderer,
)
from great_expectations.render.types import (
    CollapseContent,
    RenderedContentBlockContainer,
    RenderedStringTemplateContent,
    RenderedTableContent,
)
from great_expectations.render.util import num_to_str

logger = logging.getLogger(__name__)


class ValidationResultsTableContentBlockRenderer(ExpectationStringRenderer):
    _content_block_type = "table"
    _rendered_component_type = RenderedTableContent
    _rendered_component_default_init_kwargs = {
        "table_options": {"search": True, "icon-size": "sm"}
    }

    _default_element_styling = {
        "default": {"classes": ["badge", "badge-secondary"]},
        "params": {"column": {"classes": ["badge", "badge-primary"]}},
    }

    _default_content_block_styling = {
        "body": {
            "classes": ["table"],
        },
        "classes": ["ml-2", "mr-2", "mt-0", "mb-0", "table-responsive"],
    }

    @classmethod
    def _process_content_block(cls, content_block, has_failed_evr):
        super()._process_content_block(content_block, has_failed_evr)
        content_block.header_row = ["Status", "Expectation", "Observed Value"]
        content_block.header_row_options = {"Status": {"sortable": True}}

        if has_failed_evr is False:
            styling = deepcopy(content_block.styling) if content_block.styling else {}
            if styling.get("classes"):
                styling["classes"].append(
                    "hide-succeeded-validations-column-section-target-child"
                )
            else:
                styling["classes"] = [
                    "hide-succeeded-validations-column-section-target-child"
                ]

            content_block.styling = styling

    @classmethod
    def _get_content_block_fn(cls, expectation_type):
        expectation_string_fn = get_renderer_impl(
            object_name=expectation_type, renderer_type="renderer.prescriptive"
        )
        expectation_string_fn = (
            expectation_string_fn[1] if expectation_string_fn else None
        )
        if expectation_string_fn is None:
            expectation_string_fn = getattr(cls, "_missing_content_block_fn")

        # This function wraps expect_* methods from ExpectationStringRenderer to generate table classes
        def row_generator_fn(
            configuration=None,
            result=None,
            language=None,
            runtime_configuration=None,
            **kwargs,
        ):
            eval_param_value_dict = kwargs.get("evaluation_parameters", None)
            # loading into evaluation parameters to be passed onto prescriptive renderer
            if eval_param_value_dict is not None:
                runtime_configuration["evaluation_parameters"] = eval_param_value_dict

            expectation = result.expectation_config
            expectation_string_cell = expectation_string_fn(
                configuration=expectation, runtime_configuration=runtime_configuration
            )

            status_icon_renderer = get_renderer_impl(
                object_name=expectation_type,
                renderer_type="renderer.diagnostic.status_icon",
            )
            status_cell = (
                [status_icon_renderer[1](result=result)]
                if status_icon_renderer
                else [getattr(cls, "_diagnostic_status_icon_renderer")(result=result)]
            )
            unexpected_statement = []
            unexpected_table = None
            observed_value = ["--"]

            data_docs_exception_message = f"""\
An unexpected Exception occurred during data docs rendering.  Because of this error, certain parts of data docs will \
not be rendered properly and/or may not appear altogether.  Please use the trace, included in this message, to \
diagnose and repair the underlying issue.  Detailed information follows:
            """
            try:
                unexpected_statement_renderer = get_renderer_impl(
                    object_name=expectation_type,
                    renderer_type="renderer.diagnostic.unexpected_statement",
                )
                unexpected_statement = (
                    unexpected_statement_renderer[1](result=result)
                    if unexpected_statement_renderer
                    else []
                )
            except Exception as e:
                exception_traceback = traceback.format_exc()
                exception_message = (
                    data_docs_exception_message
                    + f'{type(e).__name__}: "{str(e)}".  Traceback: "{exception_traceback}".'
                )
                logger.error(exception_message)
            try:
                unexpected_table_renderer = get_renderer_impl(
                    object_name=expectation_type,
                    renderer_type="renderer.diagnostic.unexpected_table",
                )
                unexpected_table = (
                    unexpected_table_renderer[1](result=result)
                    if unexpected_table_renderer
                    else None
                )
            except Exception as e:
                exception_traceback = traceback.format_exc()
                exception_message = (
                    data_docs_exception_message
                    + f'{type(e).__name__}: "{str(e)}".  Traceback: "{exception_traceback}".'
                )
                logger.error(exception_message)
            try:
                observed_value_renderer = get_renderer_impl(
                    object_name=expectation_type,
                    renderer_type="renderer.diagnostic.observed_value",
                )
                observed_value = [
                    observed_value_renderer[1](result=result)
                    if observed_value_renderer
                    else "--"
                ]
            except Exception as e:
                exception_traceback = traceback.format_exc()
                exception_message = (
                    data_docs_exception_message
                    + f'{type(e).__name__}: "{str(e)}".  Traceback: "{exception_traceback}".'
                )
                logger.error(exception_message)

            # If the expectation has some unexpected values...:
            if unexpected_statement:
                expectation_string_cell += unexpected_statement
            if unexpected_table:
                expectation_string_cell.append(unexpected_table)
            if len(expectation_string_cell) > 1:
                return [status_cell + [expectation_string_cell] + observed_value]
            else:
                return [status_cell + expectation_string_cell + observed_value]

        return row_generator_fn
