from typing import Optional

from lxml import etree

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    render_evaluation_parameter_string,
)
from great_expectations.expectations.metrics.import_manager import F, sparktypes
from great_expectations.expectations.metrics.map_metric import (
    ColumnMapMetricProvider,
    column_condition_partial,
)
from great_expectations.render import RenderedStringTemplateContent
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.util import (
    num_to_str,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)

try:
    import sqlalchemy as sa
except ImportError:
    pass


class ColumnValuesMatchXmlSchema(ColumnMapMetricProvider):
    condition_metric_name = "column_values.match_xml_schema"
    condition_value_keys = ("xml_schema",)

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, xml_schema, format, **kwargs):
        try:
            xmlschema_doc = etree.fromstring(xml_schema)
            xmlschema = etree.XMLSchema(xmlschema_doc)
        except etree.ParseError:
            raise
        except:
            raise

        def matches_xml_schema(val):
            try:
                xml_doc = etree.fromstring(val)
                return xmlschema(xml_doc)
            except:
                raise

        return column.map(matches_xml_schema)

    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, xml_schema, **kwargs):
        try:
            xmlschema_doc = etree.fromstring(xml_schema)
            xmlschema = etree.XMLSchema(xmlschema_doc)
        except etree.ParseError:
            raise
        except:
            raise

        def matches_xml_schema(val):
            if val is None:
                return False
            try:
                xml_doc = etree.fromstring(val)
                return xmlschema(xml_doc)
            except:
                raise

        matches_xml_schema_udf = F.udf(matches_xml_schema, sparktypes.BooleanType())

        return matches_xml_schema_udf(column)


class ExpectColumnValuesToMatchXmlSchema(ColumnMapExpectation):
    """Expect column entries to be XML documents matching a given [XMLSchema](https://en.wikipedia.org/wiki/XML_schema).

    expect_column_values_to_match_xml_schema is a \
    :func:`column_map_expectation <great_expectations.execution_engine.execution_engine.MetaExecutionEngine
    .column_map_expectation>`.

    Args:
        column (str): \
            The column name.
        xml_schema (str): \
            The XMLSchema name.

    Keyword Args:
        mostly (None or a float between 0 and 1): \
            Return `"success": True` if at least mostly fraction of values match the expectation. \
            For more detail, see :ref:`mostly`.

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
            For more detail, see :ref:`result_format <result_format>`.
        include_config (boolean): \
            If True, then include the expectation config as part of the result object. \
            For more detail, see :ref:`include_config`.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see :ref:`catch_exceptions`.
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see :ref:`meta`.

    Returns:
        An ExpectationSuiteValidationResult

        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

    See Also:
        :func:`expect_column_values_to_be_xml_parseable \
        <great_expectations.execution_engine.execution_engine.ExecutionEngine
        .expect_column_values_to_be_xml_parseable>`


        The `XMLSchema docs <https://www.w3.org/XML/Schema>`_.
    """

    # These examples will be shown in the public gallery, and also executed as unit tests for your Expectation
    examples = [{"data": {}, "tests": []}]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "experimental",  # "experimental", "beta", or "production"
        "tags": ["xml", "glam"],
        "contributors": ["@mielvds"],
        "requirements": ["lxml"],
    }

    map_metric = "column_values.match_xml_schema"
    success_keys = (
        "xml_schema",
        "mostly",
    )

    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,  # we expect this to be explicitly set whenever a row_condition is passed
        "mostly": 1,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": True,
    }

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration]
    ) -> None:
        super().validate_configuration(configuration)

    @classmethod
    @renderer(renderer_type="renderer.prescriptive")
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs
    ):
        runtime_configuration = runtime_configuration or {}
        include_column_name = (
            False if runtime_configuration.get("include_column_name") is False else True
        )
        styling = runtime_configuration.get("styling")
        params = substitute_none_for_missing(
            configuration.kwargs,
            ["column", "mostly", "xml_schema", "row_condition", "condition_parser"],
        )

        if not params.get("xml_schema"):
            template_str = "values must match a XML Schema but none was specified."
        else:
            params["formatted_xml"] = (
                "<pre>"
                + etree.tostring(params.get("xml_schema"), pretty_print=True)
                + "</pre>"  # TODO:
            )
            if params["mostly"] is not None:
                params["mostly_pct"] = num_to_str(
                    params["mostly"] * 100, precision=15, no_scientific=True
                )
                # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
                template_str = "values must match the following XML Schema, at least $mostly_pct % of the time: $formatted_xml"
            else:
                template_str = (
                    "values must match the following XML Schema: $formatted_xml"
                )

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": {"params": {"formatted_xml": {"classes": []}}},
                    },
                }
            )
        ]


if __name__ == "__main__":
    ExpectColumnValuesToMatchXmlSchema().print_diagnostic_checklist()
