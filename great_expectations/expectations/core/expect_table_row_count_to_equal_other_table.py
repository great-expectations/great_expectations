from great_expectations.expectations.expectation import DatasetExpectation
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import RenderedStringTemplateContent
from great_expectations.render.util import substitute_none_for_missing, num_to_str


class ExpectTableRowCountToEqualOtherTable(DatasetExpectation):
    metric_dependencies = tuple()
    success_keys = (
        "other_table_name"
    )
    default_kwarg_values = {
        "other_table_name": None,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }

    @classmethod
    @renderer(renderer_type="renderer.prescriptive")
    def _prescriptive_renderer(
        cls, configuration=None, result=None, language=None, runtime_configuration=None, **kwargs
    ):
        runtime_configuration = runtime_configuration or {}
        include_column_name = runtime_configuration.get("include_column_name", True)
        styling = runtime_configuration.get("styling")
        params = substitute_none_for_missing(
            configuration.kwargs, ["other_table_name"]
        )
        template_str = "Row count must equal the row count of table $other_table_name."

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    @renderer(renderer_type="renderer.diagnostic.observed_value")
    def _diagnostic_observed_value_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs
    ):
        if not result.result.get("observed_value"):
            return "--"

        self_table_row_count = num_to_str(result.result["observed_value"]["self"])
        other_table_row_count = num_to_str(result.result["observed_value"]["other"])

        return RenderedStringTemplateContent(
            **{
                "content_block_type": "string_template",
                "string_template": {
                    "template": "Row Count: $self_table_row_count<br>Other Table Row Count: $other_table_row_count",
                    "params": {
                        "self_table_row_count": self_table_row_count,
                        "other_table_row_count": other_table_row_count,
                    },
                    "styling": {"classes": ["mb-2"]},
                },
            }
        )
