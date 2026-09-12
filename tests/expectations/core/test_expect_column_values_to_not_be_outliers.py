from decimal import Decimal
from string import Template

import pandas as pd
import pytest

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.compatibility import pydantic
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.expectations.metrics.column_aggregate_metrics.column_outlier_statistics import (  # noqa: E501
    _to_float,
    validate_method,
)
from great_expectations.expectations.registry import get_renderer_impl
from great_expectations.render import RenderedAtomicContent


def _render_outliers_prescriptive_value(configuration: ExpectationConfiguration) -> dict:
    """Render the atomic prescriptive summary and return its ``value`` dict."""
    renderer_impl = get_renderer_impl(
        object_name="expect_column_values_to_not_be_outliers",
        renderer_type="atomic.prescriptive.summary",
    )
    assert renderer_impl is not None
    rendered = renderer_impl.renderer(configuration=configuration)
    assert isinstance(rendered, RenderedAtomicContent)
    json_dict = rendered.to_json_dict()
    assert isinstance(json_dict, dict)
    value = json_dict["value"]
    assert isinstance(value, dict)
    return value


@pytest.mark.unit
@pytest.mark.parametrize(
    "method",
    ["unknown", "IQR", "stddev", "std_dev", "zscore"],
)
def test_unsupported_method_is_rejected_at_configuration_time(method: str) -> None:
    """An unsupported method must not survive into a stored suite to fail on every run."""
    with pytest.raises(pydantic.ValidationError, match="permitted: 'iqr', 'std'"):
        # deliberately passing an unsupported method value; that rejection is the point
        gxe.ExpectColumnValuesToNotBeOutliers(column="amount", method=method)  # type: ignore[arg-type]


@pytest.mark.unit
def test_unsupported_method_from_a_suite_parameter_is_rejected() -> None:
    """A method resolved from a suite parameter is held to the same set of values."""
    context = gx.get_context(mode="ephemeral")
    batch = (
        context.data_sources.add_pandas(name="pandas")
        .add_dataframe_asset(name="amounts")
        .add_batch_definition_whole_dataframe("batch_definition")
        .get_batch(batch_parameters={"dataframe": pd.DataFrame({"amount": [1, 2, 3]})})
    )
    expectation = gxe.ExpectColumnValuesToNotBeOutliers(
        column="amount", method={"$PARAMETER": "chosen_method"}
    )

    with pytest.raises(pydantic.ValidationError, match="permitted: 'iqr', 'std'"):
        batch.validate(expectation, expectation_parameters={"chosen_method": "unknown"})


@pytest.mark.unit
def test_unsupported_method_raises_when_the_metric_is_resolved_directly() -> None:
    """The metric keeps its own guard for callers that bypass the Expectation."""
    with pytest.raises(NotImplementedError, match="method 'unknown' has not been implemented"):
        validate_method("unknown")


@pytest.mark.unit
def test_negative_multiplier_is_rejected_at_configuration_time() -> None:
    """A negative multiplier would silently report every row an outlier."""
    with pytest.raises(pydantic.ValidationError, match="greater than or equal to 0"):
        gxe.ExpectColumnValuesToNotBeOutliers(column="amount", multiplier=-1.5)


@pytest.mark.unit
def test_prescriptive_renderer_substitutes_defaults() -> None:
    """A configuration that leaves method and multiplier at their defaults still renders.

    Those defaults are not serialized into the configuration's kwargs, and a param the
    renderer cannot find a value for is dropped - which would leave the placeholders
    themselves in the sentence a user reads in Data Docs.
    """
    configuration = ExpectationConfiguration(
        type="expect_column_values_to_not_be_outliers",
        kwargs={"column": "amount"},
    )
    rendered = _render_outliers_prescriptive_value(configuration)
    params = rendered["params"]
    assert isinstance(params, dict)
    substituted = Template(str(rendered["template"])).safe_substitute(
        {name: param["value"] for name, param in params.items()}
    )

    assert substituted == (
        "amount values must not be statistical outliers using the iqr method "
        "with a multiplier of 1.5."
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    ("mostly", "expected_template"),
    [
        pytest.param(
            1.0,
            "$column values must not be statistical outliers using the $method method "
            "with a multiplier of $multiplier.",
            id="all_values",
        ),
        pytest.param(
            0.9,
            "$column values must not be statistical outliers using the $method method "
            "with a multiplier of $multiplier, at least $mostly_pct % of the time.",
            id="mostly",
        ),
    ],
)
def test_prescriptive_renderer(mostly: float, expected_template: str) -> None:
    configuration = ExpectationConfiguration(
        type="expect_column_values_to_not_be_outliers",
        kwargs={
            "column": "amount",
            "method": "iqr",
            "multiplier": 1.5,
            "mostly": mostly,
        },
    )
    rendered = _render_outliers_prescriptive_value(configuration)

    assert rendered["template"] == expected_template


@pytest.mark.unit
@pytest.mark.parametrize(
    "value",
    [
        pytest.param(None, id="none"),
        pytest.param(float("nan"), id="nan"),
        pytest.param(float("inf"), id="positive_infinity"),
        pytest.param(float("-inf"), id="negative_infinity"),
        pytest.param(Decimal("NaN"), id="decimal_nan"),
    ],
)
def test_a_statistic_that_is_not_a_finite_number_is_reported_absent(value: object) -> None:
    """Arithmetic on NaN or infinity reverses the comparison instead of failing.

    `abs(x - inf) < inf` is False for every finite value, so an infinite spread would
    report every finite row an outlier and none of the infinite ones.
    """
    assert _to_float(value) is None


@pytest.mark.unit
@pytest.mark.parametrize(
    "value",
    [
        pytest.param("not a number", id="text"),
        pytest.param(pd.Timestamp("2026-01-01"), id="timestamp"),
        pytest.param(b"bytes", id="bytes"),
    ],
)
def test_a_statistic_that_is_not_numeric_at_all_raises(value: object) -> None:
    """A non-numeric column must not read as "no statistic", which passes every row."""
    with pytest.raises(TypeError, match="non-numerical column"):
        _to_float(value)
