import logging
import traceback
import warnings
from copy import deepcopy

from great_expectations.core.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.expectations.registry import get_renderer_impl
from great_expectations.render import (
    LegacyDiagnosticRendererType,
    LegacyRendererType,
    RenderedTableContent,
)
from great_expectations.render.renderer.content_block.expectation_string import (
    ExpectationStringRenderer,
)

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
    def _get_custom_columns(cls, validation_results):
        custom_columns = []
        if (
            len(validation_results) > 0
            and "meta_properties_to_render"
            in validation_results[0].expectation_config.kwargs
            and validation_results[0].expectation_config.kwargs[
                "meta_properties_to_render"
            ]
            is not None
        ):
            custom_columns = list(
                validation_results[0]
                .expectation_config.kwargs["meta_properties_to_render"]
                .keys()
            )
        return sorted(custom_columns)

    @classmethod
    def _process_content_block(
        cls, content_block, has_failed_evr, render_object=None
    ) -> None:
        super()._process_content_block(content_block, has_failed_evr)
        content_block.header_row = ["Status", "Expectation", "Observed Value"]
        content_block.header_row_options = {"Status": {"sortable": True}}

        # Add custom meta_properties_to_render header
        if render_object is not None:
            custom_columns = cls._get_custom_columns(render_object)
            content_block.header_row += custom_columns
            for column in custom_columns:
                content_block.header_row_options[column] = {"sortable": True}

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
            object_name=expectation_type, renderer_type=LegacyRendererType.PRESCRIPTIVE
        )
        expectation_string_fn = (
            expectation_string_fn[1] if expectation_string_fn else None
        )
        if expectation_string_fn is None:
            expectation_string_fn = cls._get_legacy_v2_api_style_expectation_string_fn(
                expectation_type
            )
        if expectation_string_fn is None:
            expectation_string_fn = getattr(cls, "_missing_content_block_fn")

        # This function wraps expect_* methods from ExpectationStringRenderer to generate table classes
        def row_generator_fn(
            configuration=None,
            result=None,
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
                renderer_type=LegacyDiagnosticRendererType.STATUS_ICON,
            )
            status_cell = (
                [status_icon_renderer[1](result=result)]
                if status_icon_renderer
                else [getattr(cls, "_diagnostic_status_icon_renderer")(result=result)]
            )
            unexpected_statement = []
            unexpected_table = None
            observed_value = ["--"]

            data_docs_exception_message = """\
An unexpected Exception occurred during data docs rendering.  Because of this error, certain parts of data docs will \
not be rendered properly and/or may not appear altogether.  Please use the trace, included in this message, to \
diagnose and repair the underlying issue.  Detailed information follows:
            """
            try:
                unexpected_statement_renderer = get_renderer_impl(
                    object_name=expectation_type,
                    renderer_type=LegacyDiagnosticRendererType.UNEXPECTED_STATEMENT,
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
                    renderer_type=LegacyDiagnosticRendererType.UNEXPECTED_TABLE,
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
                    renderer_type=LegacyDiagnosticRendererType.OBSERVED_VALUE,
                )
                observed_value = [
                    observed_value_renderer[1](result=result)
                    if observed_value_renderer
                    else (
                        cls._get_legacy_v2_api_observed_value(
                            expectation_string_fn, result
                        )
                        or "--"
                    )
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
                expectation_string_cell += unexpected_table
            if len(expectation_string_cell) > 1:
                output_row = [status_cell + [expectation_string_cell] + observed_value]
            else:
                output_row = [status_cell + expectation_string_cell + observed_value]

            meta_properties_renderer = get_renderer_impl(
                object_name=expectation_type,
                renderer_type=LegacyDiagnosticRendererType.META_PROPERTIES,
            )
            if meta_properties_renderer:
                output_row[0] += meta_properties_renderer[1](result=result)

            return output_row

        return row_generator_fn

    @classmethod
    def _get_legacy_v2_api_style_expectation_string_fn(cls, expectation_type):
        legacy_expectation_string_fn = getattr(cls, expectation_type, None)
        if legacy_expectation_string_fn is None:
            # With the V2 API, expectation rendering was implemented by defining a method with the same name as the expectation.
            # If no legacy rendering is present, return None.
            return None

        # deprecated-v0.13.28
        warnings.warn(
            "V2 API style custom rendering is deprecated as of v0.13.28 and is not fully supported anymore; "
            "As it will be removed in v0.16, please transition to V3 API and associated rendering style",
            DeprecationWarning,
        )

        def expectation_string_fn_with_legacy_translation(
            configuration: ExpectationConfiguration, runtime_configuration: dict
        ):
            if runtime_configuration is None:
                runtime_configuration = {}

            # With the V2 API, the expectation string function had a different signature; the below translates from the new signature to the legacy signature.
            return legacy_expectation_string_fn(
                expectation=configuration,
                styling=runtime_configuration.get("styling", None),
                include_column_name=runtime_configuration.get(
                    "include_column_name", True
                ),
            )

        return expectation_string_fn_with_legacy_translation

    @staticmethod
    def _get_legacy_v2_api_observed_value(expectation_string_fn, result):
        if (
            expectation_string_fn.__name__
            != "expectation_string_fn_with_legacy_translation"
        ):
            # If legacy V2 API style rendering is used, "expectation_string_fn" will be the method defined in the above "_get_legacy_v2_api_style_expectation_string_fn".
            # If this isn't the case, return None, so we don't do any legacy logic.
            return None

        # With V2 API style rendering, the result had an "observed_value" entry that could be rendered.
        return result["result"].get("observed_value")
