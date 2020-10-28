from great_expectations.expectations.expectation import TableExpectation
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import RenderedStringTemplateContent
from great_expectations.render.util import substitute_none_for_missing


class ExpectTableColumnsToMatchSet(TableExpectation):
    metric_dependencies = tuple()
    success_keys = (
        "column_set",
        "exact_match",
    )
    default_kwarg_values = {
        "column_set": None,
        "exact_match": True,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }

    @classmethod
    @renderer(renderer_type="renderer.prescriptive")
    def _prescriptive_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        runtime_configuration = runtime_configuration or {}
        include_column_name = runtime_configuration.get("include_column_name", True)
        styling = runtime_configuration.get("styling")
        params = substitute_none_for_missing(
            configuration.kwargs, ["column_set", "exact_match"]
        )

        if params["column_set"] is None:
            template_str = "Must specify a set or list of columns."

        else:
            # standardize order of the set for output
            params["column_list"] = list(params["column_set"])

            column_list_template_str = ", ".join(
                [f"$column_list_{idx}" for idx in range(len(params["column_list"]))]
            )

            exact_match_str = "exactly" if params["exact_match"] is True else "at least"

            template_str = f"Must have {exact_match_str} these columns (in any order): {column_list_template_str}"

            for idx in range(len(params["column_list"])):
                params["column_list_" + str(idx)] = params["column_list"][idx]

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
