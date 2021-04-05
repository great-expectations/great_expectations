import pytest

from great_expectations.core import ExpectationConfiguration
from great_expectations.exceptions import GreatExpectationsError
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.util import render_evaluation_parameter_string
from great_expectations.render.types import RenderedStringTemplateContent
from great_expectations.validator.validation_graph import MetricConfiguration


@pytest.fixture(scope="module")
def expectation_and_runtime_configuration_with_evaluation_parameters():
    configuration = ExpectationConfiguration(
        expectation_type="expect_column_min_to_be_between",
        kwargs={
            "column": "live",
            "min_value": {"$PARAMETER": "MIN_VAL_PARAM"},
            "max_value": {"$PARAMETER": "MAX_VAL_PARAM"},
            "result_format": "SUMMARY",
        },
        meta={"BasicDatasetProfiler": {"confidence": "very low"}},
    )
    # runtime configuration with evaluation_parameters loaded
    runtime_configuration_with_eval = {
        "styling": {
            "default": {"classes": ["badge", "badge-secondary"]},
            "params": {"column": {"classes": ["badge", "badge-primary"]}},
        },
        "include_column_name": None,
        "evaluation_parameters": {"MIN_VAL_PARAM": 15, "MAX_VAL_PARAM": 20},
    }
    return configuration, runtime_configuration_with_eval


def test_prescriptive_renderer_no_decorator(
    expectation_and_runtime_configuration_with_evaluation_parameters,
):
    (
        configuration,
        runtime_configuration_with_eval,
    ) = expectation_and_runtime_configuration_with_evaluation_parameters

    def bare_bones_prescriptive_renderer(
        configuration=None,
        runtime_configuration=None,
    ):
        runtime_configuration = runtime_configuration or {}
        styling = runtime_configuration.get("styling")
        params = configuration.kwargs
        template_str = "$column minimum value must be greater than or equal to $min_value and less than or equal to $max_value"
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

    res = bare_bones_prescriptive_renderer(
        configuration=configuration,
        runtime_configuration=runtime_configuration_with_eval,
    )
    assert len(res) == 1
    # string template should remain constant
    assert (
        res[0].string_template["template"]
        == "$column minimum value must be greater than or equal to $min_value and less than or equal to $max_value"
    )

    # params should contain our evaluation parameters
    assert res[0].string_template["params"]["min_value"] == {
        "$PARAMETER": "MIN_VAL_PARAM"
    }
    assert res[0].string_template["params"]["max_value"] == {
        "$PARAMETER": "MAX_VAL_PARAM"
    }

    # full json dict comparison
    assert res[0].to_json_dict() == {
        "content_block_type": "string_template",
        "string_template": {
            "template": "$column minimum value must be greater than or equal to $min_value and less than or equal to $max_value",
            "params": {
                "column": "live",
                "min_value": {"$PARAMETER": "MIN_VAL_PARAM"},
                "max_value": {"$PARAMETER": "MAX_VAL_PARAM"},
                "result_format": "SUMMARY",
            },
            "styling": {
                "default": {"classes": ["badge", "badge-secondary"]},
                "params": {"column": {"classes": ["badge", "badge-primary"]}},
            },
        },
    }


def test_prescriptive_renderer_with_decorator(
    expectation_and_runtime_configuration_with_evaluation_parameters,
):
    (
        configuration,
        runtime_configuration_with_eval,
    ) = expectation_and_runtime_configuration_with_evaluation_parameters

    @render_evaluation_parameter_string
    def bare_bones_prescriptive_renderer(
        configuration=None,
        runtime_configuration=None,
    ):
        runtime_configuration = runtime_configuration or {}
        styling = runtime_configuration.get("styling")
        params = configuration.kwargs
        template_str = "$column minimum value must be greater than or equal to $min_value and less than or equal to $max_value"
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

    res = bare_bones_prescriptive_renderer(
        configuration=configuration,
        runtime_configuration=runtime_configuration_with_eval,
    )
    assert len(res) == 3

    # string template should remain constant
    assert (
        res[0].string_template["template"]
        == "$column minimum value must be greater than or equal to $min_value and less than or equal to $max_value"
    )

    # params should contain our evaluation parameters
    assert res[0].string_template["params"]["min_value"] == {
        "$PARAMETER": "MIN_VAL_PARAM"
    }
    assert res[0].string_template["params"]["max_value"] == {
        "$PARAMETER": "MAX_VAL_PARAM"
    }
    assert res[0].to_json_dict() == {
        "content_block_type": "string_template",
        "string_template": {
            "template": "$column minimum value must be greater than or equal to $min_value and less than or equal to $max_value",
            "params": {
                "column": "live",
                "min_value": {"$PARAMETER": "MIN_VAL_PARAM"},
                "max_value": {"$PARAMETER": "MAX_VAL_PARAM"},
                "result_format": "SUMMARY",
            },
            "styling": {
                "default": {"classes": ["badge", "badge-secondary"]},
                "params": {"column": {"classes": ["badge", "badge-primary"]}},
            },
        },
    }

    assert (
        res[1].string_template["template"]
        == "\n - $eval_param = $eval_param_value (at time of validation)."
    )
    # params should contain our evaluation parameters
    assert res[1].string_template["params"]["eval_param"] == "MIN_VAL_PARAM"
    assert res[1].string_template["params"]["eval_param_value"] == 15
    assert res[1].to_json_dict() == {
        "content_block_type": "string_template",
        "string_template": {
            "template": "\n - $eval_param = $eval_param_value (at time of validation).",
            "params": {"eval_param": "MIN_VAL_PARAM", "eval_param_value": 15},
            "styling": {
                "default": {"classes": ["badge", "badge-secondary"]},
                "params": {"column": {"classes": ["badge", "badge-primary"]}},
            },
        },
    }

    assert (
        res[2].string_template["template"]
        == "\n - $eval_param = $eval_param_value (at time of validation)."
    )
    # params should contain our evaluation parameters
    assert res[2].string_template["params"]["eval_param"] == "MAX_VAL_PARAM"
    assert res[2].string_template["params"]["eval_param_value"] == 20
    assert res[2].to_json_dict() == {
        "content_block_type": "string_template",
        "string_template": {
            "template": "\n - $eval_param = $eval_param_value (at time of validation).",
            "params": {"eval_param": "MAX_VAL_PARAM", "eval_param_value": 20},
            "styling": {
                "default": {"classes": ["badge", "badge-secondary"]},
                "params": {"column": {"classes": ["badge", "badge-primary"]}},
            },
        },
    }

    # with no runtime_configuration, throw an error
    with pytest.raises(GreatExpectationsError):
        res = bare_bones_prescriptive_renderer(
            configuration=configuration, runtime_configuration={}
        )

    # configuration should always be of ExpectationConfiguration-type
    with pytest.raises(AttributeError):
        res = bare_bones_prescriptive_renderer(
            configuration={}, runtime_configuration={}
        )

    # extra evaluation parameters will not have an effect
    runtime_configuration_with_extra = {
        "styling": {
            "default": {"classes": ["badge", "badge-secondary"]},
            "params": {"column": {"classes": ["badge", "badge-primary"]}},
        },
        "include_column_name": None,
        "evaluation_parameters": {
            "MIN_VAL_PARAM": 15,
            "MAX_VAL_PARAM": 20,
            "IAMEXTRA": "EXTRA",
        },
    }

    res = bare_bones_prescriptive_renderer(
        configuration=configuration,
        runtime_configuration=runtime_configuration_with_extra,
    )
    assert len(res) == 3

    # missing evaluation_parameters will not render (MAX_VAL_PARAM is missing)
    runtime_configuration_with_missing = {
        "styling": {
            "default": {"classes": ["badge", "badge-secondary"]},
            "params": {"column": {"classes": ["badge", "badge-primary"]}},
        },
        "include_column_name": None,
        "evaluation_parameters": {"MIN_VAL_PARAM": 15},
    }
    res = bare_bones_prescriptive_renderer(
        configuration=configuration,
        runtime_configuration=runtime_configuration_with_missing,
    )
    assert len(res) == 2


def get_table_columns_metric(engine: ExecutionEngine) -> [MetricConfiguration, dict]:
    resolved_metrics: dict = {}

    results: dict

    table_column_types_metric: MetricConfiguration = MetricConfiguration(
        metric_name="table.column_types",
        metric_domain_kwargs=dict(),
        metric_value_kwargs={
            "include_nested": True,
        },
        metric_dependencies=None,
    )
    results = engine.resolve_metrics(metrics_to_resolve=(table_column_types_metric,))
    resolved_metrics.update(results)

    table_columns_metric: MetricConfiguration = MetricConfiguration(
        metric_name="table.columns",
        metric_domain_kwargs=dict(),
        metric_value_kwargs=None,
        metric_dependencies={
            "table.column_types": table_column_types_metric,
        },
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(table_columns_metric,), metrics=resolved_metrics
    )
    resolved_metrics.update(results)

    return table_columns_metric, resolved_metrics
