from datetime import datetime
from pprint import pprint
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import pytest

from great_expectations.core import ExpectationValidationResult
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.expectations.registry import get_renderer_impl
from great_expectations.render import RenderedAtomicContent
from great_expectations.render.renderer.observed_value_renderer import ObservedValueRenderState


@pytest.fixture
def expectation_configuration_kwargs():
    # These below fields are defaults; specific tests will overwrite as deemed necessary
    return {
        "id": "abcdefgh-ijkl-mnop-qrst-uvwxyz123456",
        "type": "",
        "kwargs": {},
        "meta": {},
    }


@pytest.fixture
def get_prescriptive_rendered_content(
    expectation_configuration_kwargs: Dict[str, Union[str, dict]],
) -> Callable:
    def _get_prescriptive_rendered_content(
        update_dict: Dict[str, Union[str, dict]],
    ) -> RenderedAtomicContent:
        # Overwrite any fields passed in from test and instantiate ExpectationConfiguration
        expectation_configuration_kwargs.update(update_dict)
        config = ExpectationConfiguration(**expectation_configuration_kwargs)  # type: ignore[arg-type]
        expectation_type = str(expectation_configuration_kwargs["type"])

        # Programatically determine the renderer implementations
        renderer_impl = get_renderer_impl(
            object_name=expectation_type,
            renderer_type="atomic.prescriptive.summary",
        )
        assert renderer_impl is not None

        # Determine RenderedAtomicContent output
        res = renderer_impl.renderer(configuration=config)
        assert isinstance(res, RenderedAtomicContent)
        return res

    return _get_prescriptive_rendered_content


@pytest.fixture
def evr_kwargs(expectation_configuration_kwargs):
    # These below fields are defaults; specific tests will overwrite as deemed necessary
    return {
        "expectation_config": ExpectationConfiguration(**expectation_configuration_kwargs),
        "result": {},
    }


@pytest.fixture
def get_diagnostic_rendered_content(
    evr_kwargs: Dict[str, Union[dict, ExpectationConfiguration]],
) -> Callable:
    def _get_diagnostic_rendered_content(
        update_dict: Dict[str, Union[dict, ExpectationConfiguration]],
    ) -> RenderedAtomicContent:
        # Overwrite any fields passed in from test and instantiate ExpectationValidationResult
        evr_kwargs.update(update_dict)
        evr = ExpectationValidationResult(**evr_kwargs)  # type: ignore[arg-type]
        expectation_config = evr_kwargs["expectation_config"]
        expectation_type = str(expectation_config["type"])

        # Programatically determine the renderer implementations
        renderer_impl = get_renderer_impl(
            object_name=expectation_type,
            renderer_type="atomic.diagnostic.observed_value",
        )
        assert renderer_impl is not None

        # Determine RenderedAtomicContent output
        res = renderer_impl.renderer(result=evr)
        assert isinstance(res, RenderedAtomicContent)
        return res

    return _get_diagnostic_rendered_content


# "atomic.prescriptive.summary" tests


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_bootstrapped_ks_test_p_value_to_be_greater_than(
    get_prescriptive_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_chisquare_test_p_value_to_be_greater_than(
    get_prescriptive_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_distinct_values_to_be_in_set(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_distinct_values_to_be_in_set",
        "kwargs": {
            "column": "my_column",
            "value_set": [1, 2, 3],
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column": {"schema": {"type": "string"}, "value": "my_column"},
                "v__0": {"schema": {"type": "number"}, "value": 1},
                "v__1": {"schema": {"type": "number"}, "value": 2},
                "v__2": {"schema": {"type": "number"}, "value": 3},
                "value_set": {"schema": {"type": "array"}, "value": [1, 2, 3]},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$column distinct values must belong to this set: $v__0 $v__1 $v__2.",
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_distinct_values_to_contain_set(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_distinct_values_to_contain_set",
        "kwargs": {
            "column": "my_column",
            "value_set": ["a", "b", "c"],
            "parse_strings_as_datetimes": True,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column": {"schema": {"type": "string"}, "value": "my_column"},
                "v__0": {"schema": {"type": "string"}, "value": "a"},
                "v__1": {"schema": {"type": "string"}, "value": "b"},
                "v__2": {"schema": {"type": "string"}, "value": "c"},
                "value_set": {"schema": {"type": "array"}, "value": ["a", "b", "c"]},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$column distinct values must contain this set: $v__0 $v__1 $v__2.",
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_distinct_values_to_equal_set(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_distinct_values_to_equal_set",
        "kwargs": {
            "column": "my_column",
            "value_set": ["a", "b", "c"],
            "parse_strings_as_datetimes": True,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column": {"schema": {"type": "string"}, "value": "my_column"},
                "v__0": {"schema": {"type": "string"}, "value": "a"},
                "v__1": {"schema": {"type": "string"}, "value": "b"},
                "v__2": {"schema": {"type": "string"}, "value": "c"},
                "value_set": {"schema": {"type": "array"}, "value": ["a", "b", "c"]},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$column distinct values must match this set: $v__0 $v__1 $v__2.",
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_kl_divergence_to_be_less_than(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_kl_divergence_to_be_less_than",
        "kwargs": {
            "column": "min_event_time",
            "partition_object": {
                "bins": [0, 5, 10, 30, 50],
                "weights": [0.2, 0.3, 0.1, 0.4],
            },
            "threshold": 0.1,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)

    assert res["name"] == "atomic.prescriptive.summary"
    assert res["value_type"] == "GraphType"

    graph = res["value"]["graph"]
    assert "vega.github.io/schema/vega-lite" in graph["$schema"]
    assert graph["autosize"] == "fit"
    assert graph["height"] == 400
    assert graph["width"] == 250

    mark = graph["mark"]
    assert mark in ("bar", {"type": "bar"})

    assert graph["encoding"]["x"] == {"field": "bin_min", "type": "ordinal"}
    assert graph["encoding"]["x2"] == {"field": "bin_max"}
    assert graph["encoding"]["y"] == {"field": "fraction", "type": "quantitative"}

    dataset_name = graph["data"]["name"]
    dataset = graph["datasets"][dataset_name]
    assert dataset == [
        {"bin_max": 5, "bin_min": 0, "fraction": 0.2},
        {"bin_max": 10, "bin_min": 5, "fraction": 0.3},
        {"bin_max": 30, "bin_min": 10, "fraction": 0.1},
        {"bin_max": 50, "bin_min": 30, "fraction": 0.4},
    ]

    header = res["value"]["header"]
    assert header["value"]["params"]["column"] == {
        "schema": {"type": "string"},
        "value": "min_event_time",
    }
    assert header["value"]["params"]["threshold"] == {
        "schema": {"type": "number"},
        "value": 0.1,
    }
    assert (
        header["value"]["template"]
        == "$column Kullback-Leibler (KL) divergence with respect to the following distribution must be lower than $threshold."  # noqa: E501 # FIXME CoP
    )


@pytest.mark.unit
def test_atomic_diagnostic_observed_value_expect_column_kl_divergence_to_be_less_than(
    get_diagnostic_rendered_content,
):
    # Please note that the vast majority of Expectations are calling `Expectation._atomic_diagnostic_observed_value()`  # noqa: E501 # FIXME CoP
    # As such, the specific expectation_type used here is irrelevant and is simply used to trigger the parent class.  # noqa: E501 # FIXME CoP
    expectation_config = {
        "type": "expect_column_kl_divergence_to_be_less_than",
        "kwargs": {
            "column": "min_event_time",
            "partition_object": {
                "bins": [0, 5, 10, 30, 50],
                "weights": [0.2, 0.3, 0.1, 0.4],
            },
            "threshold": 0.1,
        },
        "meta": {},
        "id": "4b53c4d5-90ba-467a-b7a7-379640bbd729",
    }
    update_dict = {
        "expectation_config": ExpectationConfiguration(**expectation_config),
        "result": {
            "observed_value": 0.0,
            "details": {
                "observed_partition": {
                    "values": [1, 2, 4],
                    "weights": [0.3754, 0.615, 0.0096],
                },
                "expected_partition": {
                    "values": [1, 2, 4],
                    "weights": [0.3754, 0.615, 0.0096],
                },
            },
        },
    }
    rendered_content = get_diagnostic_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)

    assert res["name"] == "atomic.diagnostic.observed_value"
    assert res["value_type"] == "GraphType"

    graph = res["value"]["graph"]
    assert "vega.github.io/schema/vega-lite" in graph["$schema"]
    assert graph["autosize"] == "fit"
    assert graph["height"] == 400
    assert graph["width"] == 250

    mark = graph["mark"]
    assert mark in ("bar", {"type": "bar"})

    assert graph["encoding"]["x"] == {"field": "values", "type": "nominal"}
    assert graph["encoding"]["y"] == {"field": "fraction", "type": "quantitative"}

    dataset_name = graph["data"]["name"]
    dataset = graph["datasets"][dataset_name]
    assert dataset == [
        {"fraction": 0.3754, "values": 1},
        {"fraction": 0.615, "values": 2},
        {"fraction": 0.0096, "values": 4},
    ]

    header = res["value"]["header"]
    assert header["value"]["template"] == "KL Divergence: $observed_value"
    observed_value = header["value"]["params"]["observed_value"]["value"]
    assert observed_value == "None (-infinity, infinity, or NaN)"


@pytest.mark.unit
def test_atomic_diagnostic_observed_value_with_boolean_column_expect_column_kl_divergence_to_be_less_than(  # noqa: E501 # FIXME CoP
    get_diagnostic_rendered_content,
):
    # Please note that the vast majority of Expectations are calling `Expectation._atomic_diagnostic_observed_value()`  # noqa: E501 # FIXME CoP
    # As such, the specific expectation_type used here is irrelevant and is simply used to trigger the parent class.  # noqa: E501 # FIXME CoP
    expectation_config = {
        "type": "expect_column_kl_divergence_to_be_less_than",
        "kwargs": {
            "column": "boolean_event",
            "partition_object": {
                "bins": [True, False],
                "weights": [0.5, 0.5],
            },
            "threshold": 0.1,
        },
        "meta": {},
        "id": "4b53c4d5-90ba-467a-b7a7-379640bbd729",
    }
    update_dict = {
        "expectation_config": ExpectationConfiguration(**expectation_config),
        "result": {
            "observed_value": 0.0,
            "details": {
                "observed_partition": {
                    "values": [True, False],
                    "weights": [0.5, 0.5],
                },
                "expected_partition": {
                    "values": [True, False],
                    "weights": [0.5, 0.5],
                },
            },
        },
    }
    rendered_content = get_diagnostic_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)

    assert res["name"] == "atomic.diagnostic.observed_value"
    assert res["value_type"] == "GraphType"

    graph = res["value"]["graph"]
    assert "vega.github.io/schema/vega-lite" in graph["$schema"]
    assert graph["autosize"] == "fit"
    assert graph["height"] == 400
    assert graph["width"] == 250

    # mark may be a string or dict depending on altair version
    mark = graph["mark"]
    assert mark in ("bar", {"type": "bar"})

    assert graph["encoding"]["x"] == {"field": "values", "type": "nominal"}
    assert graph["encoding"]["y"] == {"field": "fraction", "type": "quantitative"}

    # Verify datasets contain the expected data (hash name varies by altair version)
    dataset_name = graph["data"]["name"]
    dataset = graph["datasets"][dataset_name]
    assert dataset == [
        {"fraction": 0.5, "values": "True"},
        {"fraction": 0.5, "values": "False"},
    ]

    header = res["value"]["header"]
    assert header["value"]["template"] == "KL Divergence: $observed_value"
    observed_value = header["value"]["params"]["observed_value"]["value"]
    assert observed_value == "None (-infinity, infinity, or NaN)"


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_max_to_be_between(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_max_to_be_between",
        "kwargs": {
            "column": "my_column",
            "min_value": 1,
            "max_value": 5,
            "parse_strings_as_datetimes": True,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column": {"schema": {"type": "string"}, "value": "my_column"},
                "max_value": {"schema": {"type": "number"}, "value": 5},
                "min_value": {"schema": {"type": "number"}, "value": 1},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$column maximum value must be greater than or equal to $min_value and less than or equal to $max_value.",  # noqa: E501 # FIXME CoP
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_mean_to_be_between(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_mean_to_be_between",
        "kwargs": {
            "column": "my_column",
            "min_value": 3,
            "max_value": 7,
            "parse_strings_as_datetimes": True,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column": {"schema": {"type": "string"}, "value": "my_column"},
                "max_value": {"schema": {"type": "number"}, "value": 7},
                "min_value": {"schema": {"type": "number"}, "value": 3},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$column mean must be greater than or equal to $min_value and less than or equal to $max_value.",  # noqa: E501 # FIXME CoP
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_median_to_be_between(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_median_to_be_between",
        "kwargs": {
            "column": "my_column",
            "min_value": 5,
            "max_value": 10,
            "parse_strings_as_datetimes": True,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column": {"schema": {"type": "string"}, "value": "my_column"},
                "max_value": {"schema": {"type": "number"}, "value": 10},
                "min_value": {"schema": {"type": "number"}, "value": 5},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$column median must be greater than or equal to $min_value and less than or equal to $max_value.",  # noqa: E501 # FIXME CoP
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_min_to_be_between(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_min_to_be_between",
        "kwargs": {
            "column": "my_column",
            "min_value": 1,
            "max_value": 5,
            "parse_strings_as_datetimes": True,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column": {"schema": {"type": "string"}, "value": "my_column"},
                "max_value": {"schema": {"type": "number"}, "value": 5},
                "min_value": {"schema": {"type": "number"}, "value": 1},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$column minimum value must be greater than or equal to $min_value and less than or equal to $max_value.",  # noqa: E501 # FIXME CoP
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_most_common_value_to_be_in_set(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_most_common_value_to_be_in_set",
        "kwargs": {
            "column": "my_column",
            "value_set": [1, 2, 3],
            "ties_okay": True,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column": {"schema": {"type": "string"}, "value": "my_column"},
                "ties_okay": {"schema": {"type": "boolean"}, "value": True},
                "v__0": {"schema": {"type": "number"}, "value": 1},
                "v__1": {"schema": {"type": "number"}, "value": 2},
                "v__2": {"schema": {"type": "number"}, "value": 3},
                "value_set": {"schema": {"type": "array"}, "value": [1, 2, 3]},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$column most common value must belong to this set: $v__0 $v__1 $v__2. Values outside this set that are as common (but not more common) are allowed.",  # noqa: E501 # FIXME CoP
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
@pytest.mark.xfail(
    strict=False,
    reason="ExpectColumnPairCramersPhiValueToBeLessThan is not fully implemented",
)
def test_atomic_prescriptive_summary_expect_column_pair_cramers_phi_value_to_be_less_than(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_pair_cramers_phi_value_to_be_less_than",
        "kwargs": {
            "column_A": "foo",
            "column_B": "bar",
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {}


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_pair_values_a_to_be_greater_than_b(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_pair_values_a_to_be_greater_than_b",
        "kwargs": {
            "column_A": "foo",
            "column_B": "bar",
            "parse_strings_as_datetimes": True,
            "mostly": 0.8,
            "ignore_row_if": "baz",
            "or_equal": True,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column_A": {"schema": {"type": "string"}, "value": "foo"},
                "column_B": {"schema": {"type": "string"}, "value": "bar"},
                "ignore_row_if": {"schema": {"type": "string"}, "value": "baz"},
                "mostly": {"schema": {"type": "number"}, "value": 0.8},
                "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
                "or_equal": {"schema": {"type": "boolean"}, "value": True},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "Values in $column_A must be greater than or equal to those in $column_B, at least $mostly_pct % of the time.",  # noqa: E501 # FIXME CoP
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_pair_values_to_be_equal(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_pair_values_to_be_equal",
        "kwargs": {
            "column_A": "foo",
            "column_B": "bar",
            "mostly": 0.8,
            "ignore_row_if": "baz",
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column_A": {"schema": {"type": "string"}, "value": "foo"},
                "column_B": {"schema": {"type": "string"}, "value": "bar"},
                "ignore_row_if": {"schema": {"type": "string"}, "value": "baz"},
                "mostly": {"schema": {"type": "number"}, "value": 0.8},
                "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "Values in $column_A and $column_B must be equal, at least $mostly_pct % of the time.",  # noqa: E501 # FIXME CoP
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_pair_values_to_be_in_set(
    get_prescriptive_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(  # noqa: E501 # FIXME CoP
    get_prescriptive_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_proportion_of_unique_values_to_be_between(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_proportion_of_unique_values_to_be_between",
        "kwargs": {
            "column": "my_column",
            "min_value": 10,
            "max_value": 20,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column": {"schema": {"type": "string"}, "value": "my_column"},
                "max_value": {"schema": {"type": "number"}, "value": 20},
                "min_value": {"schema": {"type": "number"}, "value": 10},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$column proportion of unique values must be greater than or equal to $min_value and less than or equal to $max_value.",  # noqa: E501 # FIXME CoP
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_quantile_values_to_be_between(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_quantile_values_to_be_between",
        "kwargs": {
            "column": "Unnamed: 0",
            "quantile_ranges": {
                "quantiles": [0.05, 0.25, 0.5, 0.75, 0.95],
                "value_ranges": [
                    [66, 68],
                    [328, 330],
                    [656, 658],
                    [984, 986],
                    [1246, 1248],
                ],
            },
            "allow_relative_error": False,
        },
        "meta": {},
        "id": "cd6b4f19-8167-4984-b495-54bffcb070da",
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "header": {
                "schema": {"type": "StringValueType"},
                "value": {
                    "params": {"column": {"schema": {"type": "string"}, "value": "Unnamed: 0"}},
                    "template": "$column quantiles must be within the following value ranges.",
                },
            },
            "header_row": [
                {"schema": {"type": "string"}, "value": "Quantile"},
                {"schema": {"type": "string"}, "value": "Min Value"},
                {"schema": {"type": "string"}, "value": "Max Value"},
            ],
            "schema": {"type": "TableType"},
            "table": [
                [
                    {"schema": {"type": "string"}, "value": "0.05"},
                    {"schema": {"type": "number"}, "value": 66},
                    {"schema": {"type": "number"}, "value": 68},
                ],
                [
                    {"schema": {"type": "string"}, "value": "Q1"},
                    {"schema": {"type": "number"}, "value": 328},
                    {"schema": {"type": "number"}, "value": 330},
                ],
                [
                    {"schema": {"type": "string"}, "value": "Median"},
                    {"schema": {"type": "number"}, "value": 656},
                    {"schema": {"type": "number"}, "value": 658},
                ],
                [
                    {"schema": {"type": "string"}, "value": "Q3"},
                    {"schema": {"type": "number"}, "value": 984},
                    {"schema": {"type": "number"}, "value": 986},
                ],
                [
                    {"schema": {"type": "string"}, "value": "0.95"},
                    {"schema": {"type": "number"}, "value": 1246},
                    {"schema": {"type": "number"}, "value": 1248},
                ],
            ],
        },
        "value_type": "TableType",
    }


@pytest.mark.unit
def test_atomic_diagnostic_observed_value_expect_column_quantile_values_to_be_between(
    get_diagnostic_rendered_content,
):
    # Please note that the vast majority of Expectations are calling `Expectation._atomic_diagnostic_observed_value()`  # noqa: E501 # FIXME CoP
    # As such, the specific expectation_type used here is irrelevant and is simply used to trigger the parent class.  # noqa: E501 # FIXME CoP
    expectation_config = {
        "type": "expect_column_quantile_values_to_be_between",
        "kwargs": {
            "column": "Unnamed: 0",
            "quantile_ranges": {
                "quantiles": [0.05, 0.25, 0.5, 0.75, 0.95],
                "value_ranges": [
                    [66, 68],
                    [328, 330],
                    [656, 658],
                    [984, 986],
                    [1246, 1248],
                ],
            },
            "allow_relative_error": False,
        },
        "meta": {},
        "id": "cd6b4f19-8167-4984-b495-54bffcb070da",
    }
    update_dict = {
        "expectation_config": ExpectationConfiguration(**expectation_config),
        "result": {
            "observed_value": {
                "quantiles": [0.05, 0.25, 0.5, 0.75, 0.95],
                "values": [67, 329, 657, 985, 1247],
            },
            "element_count": 1313,
            "missing_count": None,
            "missing_percent": None,
            "details": {"success_details": [True, True, True, True, True]},
        },
    }
    rendered_content = get_diagnostic_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.diagnostic.observed_value",
        "value": {
            "header_row": [
                {"schema": {"type": "string"}, "value": "Quantile"},
                {"schema": {"type": "string"}, "value": "Value"},
            ],
            "schema": {"type": "TableType"},
            "table": [
                [
                    {"schema": {"type": "string"}, "value": "0.05"},
                    {"schema": {"type": "number"}, "value": 67},
                ],
                [
                    {"schema": {"type": "string"}, "value": "Q1"},
                    {"schema": {"type": "number"}, "value": 329},
                ],
                [
                    {"schema": {"type": "string"}, "value": "Median"},
                    {"schema": {"type": "number"}, "value": 657},
                ],
                [
                    {"schema": {"type": "string"}, "value": "Q3"},
                    {"schema": {"type": "number"}, "value": 985},
                ],
                [
                    {"schema": {"type": "string"}, "value": "0.95"},
                    {"schema": {"type": "number"}, "value": 1247},
                ],
            ],
        },
        "value_type": "TableType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_stdev_to_be_between(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_stdev_to_be_between",
        "kwargs": {
            "column": "my_column",
            "min_value": 10,
            "max_value": 20,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column": {"schema": {"type": "string"}, "value": "my_column"},
                "max_value": {"schema": {"type": "number"}, "value": 20},
                "min_value": {"schema": {"type": "number"}, "value": 10},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$column standard deviation must be greater than or equal to $min_value and less than or equal to $max_value.",  # noqa: E501 # FIXME CoP
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_sum_to_be_between(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_sum_to_be_between",
        "kwargs": {
            "column": "my_column",
            "min_value": 10,
            "max_value": 20,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column": {"schema": {"type": "string"}, "value": "my_column"},
                "max_value": {"schema": {"type": "number"}, "value": 20},
                "min_value": {"schema": {"type": "number"}, "value": 10},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$column sum must be greater than or equal to $min_value and less than or equal to $max_value.",  # noqa: E501 # FIXME CoP
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_to_exist(get_prescriptive_rendered_content):
    update_dict = {
        "type": "expect_column_to_exist",
        "kwargs": {
            "column": "my_column",
            "column_index": 5,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column": {"schema": {"type": "string"}, "value": "my_column"},
                "column_index": {"schema": {"type": "number"}, "value": 5},
                "column_indexth": {"schema": {"type": "string"}, "value": "5th"},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$column must be the $column_indexth field.",
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_unique_value_count_to_be_between(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_unique_value_count_to_be_between",
        "kwargs": {
            "column": "my_column",
            "mostly": 0.8,
            "min_value": 10,
            "max_value": 20,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column": {"schema": {"type": "string"}, "value": "my_column"},
                "max_value": {"schema": {"type": "number"}, "value": 20},
                "min_value": {"schema": {"type": "number"}, "value": 10},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$column must have greater than or equal to $min_value and less than or equal to $max_value unique values.",  # noqa: E501 # FIXME CoP
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_value_lengths_to_be_between(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_value_lengths_to_be_between",
        "kwargs": {
            "column": "my_column",
            "mostly": 0.8,
            "min_value": 10,
            "max_value": 20,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column": {"schema": {"type": "string"}, "value": "my_column"},
                "max_value": {"schema": {"type": "number"}, "value": 20},
                "min_value": {"schema": {"type": "number"}, "value": 10},
                "mostly": {"schema": {"type": "number"}, "value": 0.8},
                "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$column values must be greater than or equal to $min_value and less than or equal to $max_value characters long, at least $mostly_pct % of the time.",  # noqa: E501 # FIXME CoP
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_value_lengths_to_equal(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_value_lengths_to_equal",
        "kwargs": {
            "column": "my_column",
            "value": 100,
            "mostly": 0.8,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column": {"schema": {"type": "string"}, "value": "my_column"},
                "mostly": {"schema": {"type": "number"}, "value": 0.8},
                "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
                "value": {"schema": {"type": "number"}, "value": 100},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$column values must be $value characters long, at least $mostly_pct % of the time.",  # noqa: E501 # FIXME CoP
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_value_z_scores_to_be_less_than(
    get_prescriptive_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_values_to_be_between(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_values_to_be_between",
        "kwargs": {
            "column": "my_column",
            "mostly": 0.8,
            "min_value": 1,
            "max_value": 5,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column": {"schema": {"type": "string"}, "value": "my_column"},
                "max_value": {"schema": {"type": "number"}, "value": 5},
                "min_value": {"schema": {"type": "number"}, "value": 1},
                "mostly": {"schema": {"type": "number"}, "value": 0.8},
                "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$column values must be greater than or equal to $min_value and less than or equal to $max_value, at least $mostly_pct % of the time.",  # noqa: E501 # FIXME CoP
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_values_to_be_dateutil_parseable(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_values_to_be_dateutil_parseable",
        "kwargs": {
            "column": "my_column",
            "mostly": 0.8,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column": {"schema": {"type": "string"}, "value": "my_column"},
                "mostly": {"schema": {"type": "number"}, "value": 0.8},
                "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$column values must be parseable by dateutil, at least $mostly_pct % of the time.",  # noqa: E501 # FIXME CoP
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_values_to_be_decreasing(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_values_to_be_decreasing",
        "kwargs": {
            "column": "my_column",
            "mostly": 0.8,
            "parse_strings_as_datetimes": True,
            "strictly": True,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column": {"schema": {"type": "string"}, "value": "my_column"},
                "mostly": {"schema": {"type": "number"}, "value": 0.8},
                "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
                "strictly": {"schema": {"type": "boolean"}, "value": True},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$column values must be strictly less than previous values, at least $mostly_pct % of the time.",  # noqa: E501 # FIXME CoP
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_values_to_be_in_set(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_values_to_be_in_set",
        "kwargs": {
            "column": "my_column",
            "value_set": [1, 2, 3, 4],
            "mostly": 0.8,
            "parse_strings_as_datetimes": True,
            "strictly": 50,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column": {"schema": {"type": "string"}, "value": "my_column"},
                "mostly": {"schema": {"type": "number"}, "value": 0.8},
                "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
                "v__0": {"schema": {"type": "number"}, "value": 1},
                "v__1": {"schema": {"type": "number"}, "value": 2},
                "v__2": {"schema": {"type": "number"}, "value": 3},
                "v__3": {"schema": {"type": "number"}, "value": 4},
                "value_set": {"schema": {"type": "array"}, "value": [1, 2, 3, 4]},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$column values must belong to this set: $v__0 $v__1 $v__2 $v__3, at least $mostly_pct % of the time.",  # noqa: E501 # FIXME CoP
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_values_to_be_in_type_list(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_values_to_be_in_type_list",
        "kwargs": {
            "column": "my_column",
            "type_list": ["type_a", "type_b", "type_c"],
            "value_set": [1, 2, 3, 4],
            "mostly": 0.8,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column": {"schema": {"type": "string"}, "value": "my_column"},
                "mostly": {"schema": {"type": "number"}, "value": 0.8},
                "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
                "type_list": {
                    "schema": {"type": "array"},
                    "value": ["type_a", "type_b", "type_c"],
                },
                "v__0": {"schema": {"type": "string"}, "value": "type_a"},
                "v__1": {"schema": {"type": "string"}, "value": "type_b"},
                "v__2": {"schema": {"type": "string"}, "value": "type_c"},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$column value types must belong to this set: $v__0 $v__1 $v__2, at least $mostly_pct % of the time.",  # noqa: E501 # FIXME CoP
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_values_to_be_increasing(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_values_to_be_increasing",
        "kwargs": {
            "column": "my_column",
            "strictly": True,
            "mostly": 0.8,
            "parse_strings_as_datetimes": True,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column": {"schema": {"type": "string"}, "value": "my_column"},
                "mostly": {"schema": {"type": "number"}, "value": 0.8},
                "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
                "strictly": {"schema": {"type": "boolean"}, "value": True},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$column values must be strictly greater than previous values, at least $mostly_pct % of the time.",  # noqa: E501 # FIXME CoP
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_values_to_be_json_parseable(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_values_to_be_json_parseable",
        "kwargs": {
            "column": "my_column",
            "mostly": 0.8,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column": {"schema": {"type": "string"}, "value": "my_column"},
                "mostly": {"schema": {"type": "number"}, "value": 0.8},
                "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$column values must be parseable as JSON, at least $mostly_pct % of the time.",  # noqa: E501 # FIXME CoP
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_values_to_be_null(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_values_to_be_null",
        "kwargs": {
            "column": "my_column",
            "mostly": 0.8,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column": {"schema": {"type": "string"}, "value": "my_column"},
                "mostly": {"schema": {"type": "number"}, "value": 0.8},
                "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$column values must be null, at least $mostly_pct % of the time.",
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_values_to_be_null_with_mostly_equals_1(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_values_to_be_null",
        "kwargs": {
            "column": "my_column",
            "mostly": 1.0,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column": {"schema": {"type": "string"}, "value": "my_column"},
                "mostly": {"schema": {"type": "number"}, "value": 1.0},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$column values must be null.",
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_values_to_be_of_type(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_values_to_be_of_type",
        "kwargs": {
            "column": "my_column",
            "type_": "my_type",
            "mostly": 0.8,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column": {"schema": {"type": "string"}, "value": "my_column"},
                "mostly": {"schema": {"type": "number"}, "value": 0.8},
                "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
                "type_": {"schema": {"type": "string"}, "value": "my_type"},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$column values must be of type $type_, at least $mostly_pct % of the time.",  # noqa: E501 # FIXME CoP
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_values_to_be_unique(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_values_to_be_unique",
        "kwargs": {
            "column": "my_column",
            "mostly": 0.8,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column": {"schema": {"type": "string"}, "value": "my_column"},
                "mostly": {"schema": {"type": "number"}, "value": 0.8},
                "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$column values must be unique, at least $mostly_pct % of the time.",
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_values_to_match_json_schema(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_values_to_match_json_schema",
        "kwargs": {
            "column": "my_column",
            "mostly": 0.8,
            "json_schema": {"foo": "bar"},
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column": {"schema": {"type": "string"}, "value": "my_column"},
                "formatted_json": {
                    "schema": {"type": "string"},
                    "value": """<pre>{\n    "foo": "bar"\n}</pre>""",
                },
                "json_schema": {"schema": {"type": "object"}, "value": {"foo": "bar"}},
                "mostly": {"schema": {"type": "number"}, "value": 0.8},
                "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$column values must match the following JSON Schema, at least $mostly_pct % of the time: $formatted_json",  # noqa: E501 # FIXME CoP
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_values_to_match_like_pattern(
    get_prescriptive_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_values_to_match_like_pattern_list(
    get_prescriptive_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_values_to_match_regex(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_values_to_match_regex",
        "kwargs": {
            "column": "my_column",
            "mostly": 0.8,
            "regex": "^superconductive$",
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column": {"schema": {"type": "string"}, "value": "my_column"},
                "mostly": {"schema": {"type": "number"}, "value": 0.8},
                "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
                "regex": {"schema": {"type": "string"}, "value": "^superconductive$"},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$column values must match this regular expression: $regex, at least $mostly_pct % of the time.",  # noqa: E501 # FIXME CoP
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_values_to_match_regex_list(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_values_to_match_regex_list",
        "kwargs": {
            "column": "my_column",
            "regex_list": ["^superconductive$", "ge|great_expectations"],
            "mostly": 0.8,
            "match_on": "all",
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column": {"schema": {"type": "string"}, "value": "my_column"},
                "match_on": {"schema": {"type": "string"}, "value": "all"},
                "mostly": {"schema": {"type": "number"}, "value": 0.8},
                "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
                "regex_list": {
                    "schema": {"type": "array"},
                    "value": ["^superconductive$", "ge|great_expectations"],
                },
                "v__0": {"schema": {"type": "string"}, "value": "^superconductive$"},
                "v__1": {"schema": {"type": "string"}, "value": "ge|great_expectations"},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$column values must match all of the following regular expressions: $v__0 $v__1, at least $mostly_pct % of the time.",  # noqa: E501 # FIXME CoP
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_values_to_match_strftime_format(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_values_to_match_strftime_format",
        "kwargs": {
            "column": "my_column",
            "strftime_format": "%Y-%m",
            "mostly": 0.8,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column": {"schema": {"type": "string"}, "value": "my_column"},
                "mostly": {"schema": {"type": "number"}, "value": 0.8},
                "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
                "strftime_format": {"schema": {"type": "string"}, "value": "%Y-%m"},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$column values must match the following strftime format: $strftime_format, at least $mostly_pct % of the time.",  # noqa: E501 # FIXME CoP
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_values_to_not_be_in_set(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_values_to_not_be_in_set",
        "kwargs": {
            "column": "my_column",
            "value_set": [1, 2, 3],
            "mostly": 0.8,
            "parse_string_as_datetimes": True,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column": {"schema": {"type": "string"}, "value": "my_column"},
                "mostly": {"schema": {"type": "number"}, "value": 0.8},
                "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
                "v__0": {"schema": {"type": "number"}, "value": 1},
                "v__1": {"schema": {"type": "number"}, "value": 2},
                "v__2": {"schema": {"type": "number"}, "value": 3},
                "value_set": {"schema": {"type": "array"}, "value": [1, 2, 3]},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$column values must not belong to this set: $v__0 $v__1 $v__2, at least $mostly_pct % of the time.",  # noqa: E501 # FIXME CoP
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_values_to_not_be_null(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_values_to_not_be_null",
        "kwargs": {
            "column": "my_column",
            "mostly": 0.8,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column": {"schema": {"type": "string"}, "value": "my_column"},
                "mostly": {"schema": {"type": "number"}, "value": 0.8},
                "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$column values must not be null, at least $mostly_pct % of the time.",
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_values_to_not_match_like_pattern(
    get_prescriptive_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_values_to_not_match_like_pattern_list(
    get_prescriptive_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_values_to_not_match_regex(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_values_to_not_match_regex",
        "kwargs": {
            "column": "my_column",
            "regex": "^superconductive$",
            "mostly": 0.8,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column": {"schema": {"type": "string"}, "value": "my_column"},
                "mostly": {"schema": {"type": "number"}, "value": 0.8},
                "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
                "regex": {"schema": {"type": "string"}, "value": "^superconductive$"},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$column values must not match this regular expression: $regex, at least $mostly_pct % of the time.",  # noqa: E501 # FIXME CoP
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_column_values_to_not_match_regex_list(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_column_values_to_not_match_regex_list",
        "kwargs": {
            "column": "my_column",
            "regex_list": ["^a", "^b", "^c"],
            "mostly": 0.8,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column": {"schema": {"type": "string"}, "value": "my_column"},
                "mostly": {"schema": {"type": "number"}, "value": 0.8},
                "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
                "regex_list": {"schema": {"type": "array"}, "value": ["^a", "^b", "^c"]},
                "v__0": {"schema": {"type": "string"}, "value": "^a"},
                "v__1": {"schema": {"type": "string"}, "value": "^b"},
                "v__2": {"schema": {"type": "string"}, "value": "^c"},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$column values must not match any of the following regular expressions: $v__0 $v__1 $v__2, at least $mostly_pct % of the time.",  # noqa: E501 # FIXME CoP
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_compound_columns_to_be_unique(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_compound_columns_to_be_unique",
        "kwargs": {
            "column_list": ["my_first_col", "my_second_col", "my_third_col"],
            "mostly": 0.8,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column_list": {
                    "schema": {"type": "array"},
                    "value": ["my_first_col", "my_second_col", "my_third_col"],
                },
                "column_list_0": {"schema": {"type": "string"}, "value": "my_first_col"},
                "column_list_1": {"schema": {"type": "string"}, "value": "my_second_col"},
                "column_list_2": {"schema": {"type": "string"}, "value": "my_third_col"},
                "mostly": {"schema": {"type": "number"}, "value": 0.8},
                "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "Values for given compound columns must be unique together, at least $mostly_pct % of the time: $column_list_0 $column_list_1 $column_list_2",  # noqa: E501 # FIXME CoP
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_multicolumn_sum_to_equal(
    get_prescriptive_rendered_content,
):
    # Expectation is a stub; open to implement test once renderer method is available
    pass


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_multicolumn_values_to_be_equal(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_multicolumn_values_to_be_equal",
        "kwargs": {
            "column_list": ["A", "B", "C"],
            "ignore_row_if": "never",
            "mostly": 0.8,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column_list": {"schema": {"type": "array"}, "value": ["A", "B", "C"]},
                "column_list_0": {"schema": {"type": "string"}, "value": "A"},
                "column_list_1": {"schema": {"type": "string"}, "value": "B"},
                "column_list_2": {"schema": {"type": "string"}, "value": "C"},
                "ignore_row_if": {"schema": {"type": "string"}, "value": "never"},
                "mostly": {"schema": {"type": "number"}, "value": 0.8},
                "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "Values across columns $column_list_0 $column_list_1 $column_list_2 must be equal, at least $mostly_pct % of the time.",  # noqa: E501 # FIXME CoP
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_multicolumn_values_to_be_unique(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_multicolumn_values_to_be_unique",
        "kwargs": {
            "column_list": ["A", "B", "C"],
            "ignore_row_if": "foo",
            "mostly": 0.8,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column_list": {"schema": {"type": "array"}, "value": ["A", "B", "C"]},
                "column_list_0": {"schema": {"type": "string"}, "value": "A"},
                "column_list_1": {"schema": {"type": "string"}, "value": "B"},
                "column_list_2": {"schema": {"type": "string"}, "value": "C"},
                "ignore_row_if": {"schema": {"type": "string"}, "value": "foo"},
                "mostly": {"schema": {"type": "number"}, "value": 0.8},
                "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "Values must be unique across columns, at least $mostly_pct % of the time: $column_list_0 $column_list_1 $column_list_2",  # noqa: E501 # FIXME CoP
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_select_column_values_to_be_unique_within_record(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_select_column_values_to_be_unique_within_record",
        "kwargs": {
            "column_list": ["my_first_column", "my_second_column"],
            "mostly": 0.8,
            "ignore_row_if": "foo",
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column_list": {
                    "schema": {"type": "array"},
                    "value": ["my_first_column", "my_second_column"],
                },
                "column_list_0": {"schema": {"type": "string"}, "value": "my_first_column"},
                "column_list_1": {
                    "schema": {"type": "string"},
                    "value": "my_second_column",
                },
                "ignore_row_if": {"schema": {"type": "string"}, "value": "foo"},
                "mostly": {"schema": {"type": "number"}, "value": 0.8},
                "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "Values must be unique across columns, at least $mostly_pct % of the time: $column_list_0 $column_list_1",  # noqa: E501 # FIXME CoP
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_table_column_count_to_be_between(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_table_column_count_to_be_between",
        "kwargs": {
            "min_value": 5,
            "max_value": None,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {"min_value": {"schema": {"type": "number"}, "value": 5}},
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "Must have greater than or equal to $min_value columns.",
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_table_column_count_to_equal(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_table_column_count_to_equal",
        "kwargs": {
            "value": 10,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {"value": {"schema": {"type": "number"}, "value": 10}},
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "Must have exactly $value columns.",
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_table_columns_to_match_ordered_list(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_table_columns_to_match_ordered_list",
        "kwargs": {"column_list": ["a", "b", "c"]},
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column_list": {"schema": {"type": "array"}, "value": ["a", "b", "c"]},
                "column_list_0": {"schema": {"type": "string"}, "value": "a"},
                "column_list_1": {"schema": {"type": "string"}, "value": "b"},
                "column_list_2": {"schema": {"type": "string"}, "value": "c"},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "Must have these columns in this order: $column_list_0 $column_list_1 $column_list_2",  # noqa: E501 # FIXME CoP
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_table_columns_to_match_set(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_table_columns_to_match_set",
        "kwargs": {
            "column_set": ["a", "b", "c"],
            "exact_match": True,
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column_set": {"schema": {"type": "array"}, "value": ["a", "b", "c"]},
                "column_set_0": {"schema": {"type": "string"}, "value": "a"},
                "column_set_1": {"schema": {"type": "string"}, "value": "b"},
                "column_set_2": {"schema": {"type": "string"}, "value": "c"},
                "exact_match": {"schema": {"type": "boolean"}, "value": True},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "Must have exactly these columns (in any order): $column_set_0 $column_set_1 $column_set_2",  # noqa: E501 # FIXME CoP
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_columns_match_set_no_exact_match_uses_default(
    get_prescriptive_rendered_content,
):
    """When exact_match is omitted, renderer uses model default (True)."""
    update_dict = {
        "type": "expect_table_columns_to_match_set",
        "kwargs": {
            "column_set": ["a", "b", "c"],
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column_set": {"schema": {"type": "array"}, "value": ["a", "b", "c"]},
                "column_set_0": {"schema": {"type": "string"}, "value": "a"},
                "column_set_1": {"schema": {"type": "string"}, "value": "b"},
                "column_set_2": {"schema": {"type": "string"}, "value": "c"},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "Must have exactly these columns (in any order): $column_set_0 $column_set_1 $column_set_2",  # noqa: E501 # FIXME CoP
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_legacy_prescriptive_columns_match_set_no_exact_match_uses_default():
    from great_expectations.expectations.core.expect_table_columns_to_match_set import (
        ExpectTableColumnsToMatchSet,
    )

    config = ExpectationConfiguration(
        type="expect_table_columns_to_match_set",
        kwargs={"column_set": ["x", "y"]},
    )
    rendered = ExpectTableColumnsToMatchSet._prescriptive_renderer(configuration=config)
    assert len(rendered) == 1
    template = rendered[0].string_template["template"]
    assert template == (
        "Must have exactly these columns (in any order): $column_list_0, $column_list_1"
    )


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_table_row_count_to_be_between(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_table_row_count_to_be_between",
        "kwargs": {"max_value": None, "min_value": 1},
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {"min_value": {"schema": {"type": "number"}, "value": 1}},
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "Must have greater than or equal to $min_value rows.",
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_table_row_count_to_equal(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_table_row_count_to_equal",
        "kwargs": {"value": 10},
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {"value": {"schema": {"type": "number"}, "value": 10}},
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "Must have exactly $value rows.",
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_prescriptive_summary_expect_table_row_count_to_equal_other_table(
    get_prescriptive_rendered_content,
):
    update_dict = {
        "type": "expect_table_row_count_to_equal_other_table",
        "kwargs": {
            "other_table_name": {
                "schema": {"type": "string"},
                "value": "other_table_name",
            }
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "other_table_name": {
                    "schema": {"type": "string"},
                    "value": {"schema": {"type": "string"}, "value": "other_table_name"},
                }
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "Row count must equal the row count of table $other_table_name.",
        },
        "value_type": "StringValueType",
    }


# "atomic.diagnostic.observed_value" tests


@pytest.mark.unit
def test_atomic_diagnostic_observed_value_without_result(get_diagnostic_rendered_content):
    # Please note that the vast majority of Expectations are calling `Expectation._atomic_diagnostic_observed_value()`  # noqa: E501 # FIXME CoP
    # As such, the specific expectation_type used here is irrelevant and is simply used to trigger the parent class.  # noqa: E501 # FIXME CoP
    expectation_config = {
        "type": "expect_table_row_count_to_equal",
        "kwargs": {},
    }
    update_dict = {
        "expectation_config": ExpectationConfiguration(**expectation_config),
    }
    rendered_content = get_diagnostic_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.diagnostic.observed_value",
        "value": {
            "params": {"observed_value": {"schema": {"type": "string"}, "value": "--"}},
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$observed_value",
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_diagnostic_observed_value_with_numeric_observed_value(
    get_diagnostic_rendered_content,
):
    # Please note that the vast majority of Expectations are calling `Expectation._atomic_diagnostic_observed_value()`  # noqa: E501 # FIXME CoP
    # As such, the specific expectation_type used here is irrelevant and is simply used to trigger the parent class.  # noqa: E501 # FIXME CoP
    expectation_config = {
        "type": "expect_table_row_count_to_equal",
        "kwargs": {},
    }
    update_dict = {
        "expectation_config": ExpectationConfiguration(**expectation_config),
        "result": {"observed_value": 1776},
    }
    rendered_content = get_diagnostic_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.diagnostic.observed_value",
        "value": {
            "params": {"observed_value": {"schema": {"type": "number"}, "value": 1776}},
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$observed_value",
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_diagnostic_observed_value_with_str_observed_value(get_diagnostic_rendered_content):
    # Please note that the vast majority of Expectations are calling `Expectation._atomic_diagnostic_observed_value()`  # noqa: E501 # FIXME CoP
    # As such, the specific expectation_type used here is irrelevant and is simply used to trigger the parent class.  # noqa: E501 # FIXME CoP
    expectation_config = {
        "type": "expect_table_row_count_to_equal",
        "kwargs": {},
    }
    update_dict = {
        "expectation_config": ExpectationConfiguration(**expectation_config),
        "result": {"observed_value": "foo"},
    }
    rendered_content = get_diagnostic_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.diagnostic.observed_value",
        "value": {
            "params": {"observed_value": {"schema": {"type": "string"}, "value": "foo"}},
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$observed_value",
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_diagnostic_observed_value_with_unexpected_percent(get_diagnostic_rendered_content):
    # Please note that the vast majority of Expectations are calling `Expectation._atomic_diagnostic_observed_value()`  # noqa: E501 # FIXME CoP
    # As such, the specific expectation_type used here is irrelevant and is simply used to trigger the parent class.  # noqa: E501 # FIXME CoP
    expectation_config = {
        "type": "expect_table_row_count_to_equal",
        "kwargs": {},
    }
    update_dict = {
        "expectation_config": ExpectationConfiguration(**expectation_config),
        "result": {"unexpected_percent": 10},
    }
    rendered_content = get_diagnostic_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.diagnostic.observed_value",
        "value": {
            "params": {"observed_value": {"schema": {"type": "string"}, "value": "10% unexpected"}},
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$observed_value",
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_diagnostic_observed_value_with_empty_result(get_diagnostic_rendered_content):
    # Please note that the vast majority of Expectations are calling `Expectation._atomic_diagnostic_observed_value()`  # noqa: E501 # FIXME CoP
    # As such, the specific expectation_type used here is irrelevant and is simply used to trigger the parent class.  # noqa: E501 # FIXME CoP
    expectation_config = {
        "type": "expect_table_row_count_to_equal",
        "kwargs": {},
    }
    update_dict = {
        "expectation_config": ExpectationConfiguration(**expectation_config),
        "result": {},
    }
    rendered_content = get_diagnostic_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.diagnostic.observed_value",
        "value": {
            "params": {"observed_value": {"schema": {"type": "string"}, "value": "--"}},
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "$observed_value",
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
@pytest.mark.parametrize(
    "result, expected_template, expected_params",
    [
        (
            {"observed_value": "foo"},
            "$observed_value",
            {"observed_value": {"schema": {"type": "string"}, "value": "foo"}},
        ),
        (
            {"observed_value": False},
            "$observed_value",
            {"observed_value": {"schema": {"type": "boolean"}, "value": False}},
        ),
        (
            {
                "observed_value": datetime.strptime(
                    "1999-12-31T23:59:59-00:00", "%Y-%m-%dT%H:%M:%S%z"
                )
            },
            "$observed_value",
            {
                "observed_value": {
                    "schema": {"type": "datetime"},
                    "value": datetime.strptime("1999-12-31T23:59:59-00:00", "%Y-%m-%dT%H:%M:%S%z"),
                }
            },
        ),
        (
            {"observed_value": "1999-12-31 23:59:59"},
            "$observed_value",
            {"observed_value": {"schema": {"type": "datetime"}, "value": "1999-12-31 23:59:59"}},
        ),
        (
            {"observed_value": 1776},
            "$observed_value",
            {"observed_value": {"schema": {"type": "number"}, "value": 1776}},
        ),
        (
            {"unexpected_percent": 10},
            "$observed_value",
            {"observed_value": {"schema": {"type": "string"}, "value": "10% unexpected"}},
        ),
        (
            {"observed_value": ["%", "_"]},
            "$observed_value",
            {"observed_value": {"schema": {"type": "array"}, "value": ["%", "_"]}},
        ),
        (
            {"observed_value": [1, 2, 3]},
            "$observed_value",
            {"observed_value": {"schema": {"type": "array"}, "value": [1, 2, 3]}},
        ),
        # when all value types are inferrable string will end up taking precendence over
        # object otherwise object would overrride most other inference types as it
        # is permissible to any value type
        (
            {"observed_value": {"foo": "bar"}},
            "$observed_value",
            {"observed_value": {"schema": {"type": "string"}, "value": {"foo": "bar"}}},
        ),
        (
            {"observed_value": None},
            "$observed_value",
            {"observed_value": {"schema": {"type": "string"}, "value": "--"}},
        ),
        (
            {},
            "$observed_value",
            {"observed_value": {"schema": {"type": "string"}, "value": "--"}},
        ),
    ],
)
def test_atomic_diagnostic_observed_param_type_inference(
    get_diagnostic_rendered_content, result, expected_template, expected_params
):
    expectation_config = {
        "type": "expect_table_row_count_to_equal",
        "kwargs": {},
    }
    update_dict = {
        "expectation_config": ExpectationConfiguration(**expectation_config),
        "result": result,
    }
    rendered_content = get_diagnostic_rendered_content(update_dict)
    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.diagnostic.observed_value",
        "value": {
            "params": expected_params,
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": expected_template,
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
@pytest.mark.parametrize(
    "description, expected_value, observed_value, expected_result",
    [
        (
            "happy",
            ["a", "b", "c"],
            ["a", "b", "c"],
            [
                ("ov__0", "a", "expected"),
                ("ov__1", "b", "expected"),
                ("ov__2", "c", "expected"),
            ],
        ),
        (
            "disjoint",
            ["a", "b", "c"],
            ["x", "y", "z"],
            [
                ("exp__0", "a", "missing"),
                ("exp__1", "b", "missing"),
                ("exp__2", "c", "missing"),
                ("ov__0", "x", "unexpected"),
                ("ov__1", "y", "unexpected"),
                ("ov__2", "z", "unexpected"),
            ],
        ),
        (
            "transposed chars",
            ["a", "b", "c", "d"],
            ["a", "c", "b", "d"],
            [
                ("ov__0", "a", "expected"),
                ("ov__1", "c", "unexpected"),
                ("ov__2", "b", "unexpected"),
                ("ov__3", "d", "expected"),
            ],
        ),
        (
            "renamed",
            ["a", "b", "c"],
            ["a", "c", "b2"],
            [
                ("ov__0", "a", ObservedValueRenderState.EXPECTED),
                ("exp__1", "b", ObservedValueRenderState.MISSING),
                ("ov__1", "c", ObservedValueRenderState.EXPECTED),
                ("ov__2", "b2", ObservedValueRenderState.UNEXPECTED),
            ],
        ),
        (
            "pair transposed",
            ["a", "b"],
            ["b", "a"],
            [
                ("ov__0", "b", ObservedValueRenderState.UNEXPECTED),
                ("ov__1", "a", ObservedValueRenderState.UNEXPECTED),
            ],
        ),
        (
            # NOTE: this is a confusing one, but it is hard to generalize
            "first unexpected and last missing",
            ["a", "b"],
            ["x", "a"],
            [
                ("ov__0", "x", ObservedValueRenderState.UNEXPECTED),
                ("exp__1", "b", ObservedValueRenderState.MISSING),
                ("ov__1", "a", ObservedValueRenderState.UNEXPECTED),
            ],
        ),
        (
            "pair second index missing",
            ["a", "b"],
            ["a", "x"],
            [
                ("ov__0", "a", ObservedValueRenderState.EXPECTED),
                ("exp__1", "b", ObservedValueRenderState.MISSING),
                ("ov__1", "x", ObservedValueRenderState.UNEXPECTED),
            ],
        ),
        (
            "empty actual",
            ["a", "b"],
            [],
            [
                ("exp__0", "a", ObservedValueRenderState.MISSING),
                ("exp__1", "b", ObservedValueRenderState.MISSING),
            ],
        ),
        (
            "one column deleted",
            ["a", "b", "c"],
            ["a", "c"],
            [
                ("ov__0", "a", ObservedValueRenderState.EXPECTED),
                ("exp__1", "b", ObservedValueRenderState.MISSING),
                ("ov__1", "c", ObservedValueRenderState.EXPECTED),
            ],
        ),
        (
            "last column deleted",
            ["a", "b", "c", "d"],
            ["a", "b", "c"],
            [
                ("ov__0", "a", ObservedValueRenderState.EXPECTED),
                ("ov__1", "b", ObservedValueRenderState.EXPECTED),
                ("ov__2", "c", ObservedValueRenderState.EXPECTED),
                ("exp__3", "d", ObservedValueRenderState.MISSING),
            ],
        ),
        (
            "empty expected",
            [],
            ["a", "b"],
            [
                ("ov__0", "a", ObservedValueRenderState.UNEXPECTED),
                ("ov__1", "b", ObservedValueRenderState.UNEXPECTED),
            ],
        ),
        (
            "one column added",
            ["a", "b"],
            ["a", "b", "c"],
            [
                ("ov__0", "a", ObservedValueRenderState.EXPECTED),
                ("ov__1", "b", ObservedValueRenderState.EXPECTED),
                ("ov__2", "c", ObservedValueRenderState.UNEXPECTED),
            ],
        ),
        (
            "missing bookends",
            ["f", "a", "b", "c", "d"],
            ["a", "b", "c"],
            [
                ("exp__0", "f", ObservedValueRenderState.MISSING),
                ("ov__0", "a", ObservedValueRenderState.EXPECTED),
                ("ov__1", "b", ObservedValueRenderState.EXPECTED),
                ("ov__2", "c", ObservedValueRenderState.EXPECTED),
                ("exp__4", "d", ObservedValueRenderState.MISSING),
            ],
        ),
        (
            "mix 2",
            ["a", "b", "c", "d"],
            ["a", "c", "d", "b", "e"],
            [
                ("ov__0", "a", ObservedValueRenderState.EXPECTED),
                ("ov__1", "c", ObservedValueRenderState.UNEXPECTED),
                ("ov__2", "d", ObservedValueRenderState.UNEXPECTED),
                ("ov__3", "b", ObservedValueRenderState.UNEXPECTED),
                ("ov__4", "e", ObservedValueRenderState.UNEXPECTED),
            ],
        ),
    ],
)
def test_expect_table_columns_to_match_ordered_list_atomic_diagnostic_observed_value(
    description,
    expected_value,
    observed_value,
    expected_result,
    get_diagnostic_rendered_content,
):
    # arrange
    x = {
        "expectation_config": ExpectationConfiguration(
            type="expect_table_columns_to_match_ordered_list",
            kwargs={"column_list": expected_value},
        ),
        "result": {"observed_value": observed_value},
    }

    expected_template_string = " ".join([f"${name}" for name, _, _ in expected_result])

    # act
    res = get_diagnostic_rendered_content(x).to_json_dict()

    # assert
    assert res["value"]["template"] == expected_template_string

    for name, val, status in expected_result:
        assert name in res["value"]["params"]
        assert res["value"]["params"][name]["value"] == val
        assert res["value"]["params"][name]["render_state"] == status


@pytest.mark.unit
@pytest.mark.parametrize(
    "value_set, result, expected_template, expected_params",
    [
        (
            ["blue", "green"],
            {"observed_value": ["blue", "red"]},
            "$ov__0 $ov__1",
            {
                "observed_value": {"schema": {"type": "array"}, "value": ["blue", "red"]},
                "ov__0": {
                    "schema": {"type": "string"},
                    "value": "blue",
                    "render_state": "expected",
                },
                "ov__1": {
                    "schema": {"type": "string"},
                    "value": "red",
                    "render_state": "unexpected",
                },
                "value_set": {"schema": {"type": "array"}, "value": ["blue", "green"]},
            },
        ),
        (
            ["blue", "green"],
            {"observed_value": ["red"]},
            "$ov__0",
            {
                "observed_value": {"schema": {"type": "array"}, "value": ["red"]},
                "ov__0": {
                    "schema": {"type": "string"},
                    "value": "red",
                    "render_state": "unexpected",
                },
                "value_set": {"schema": {"type": "array"}, "value": ["blue", "green"]},
            },
        ),
    ],
)
def test_expect_column_most_common_value_to_be_in_set_atomic_diagnostic_observed_value(
    get_diagnostic_rendered_content, value_set, result, expected_template, expected_params
):
    # arrange
    x = {
        "expectation_config": ExpectationConfiguration(
            type="expect_column_most_common_value_to_be_in_set",
            kwargs={
                "column": "color",
                "value_set": value_set,
                # ties_okay parameter does not affect observed value rendering
                # the Expectation can pass and still have values with
                # render_state "unexpected" in the observed value set
                # if ties_okay is set to True
            },
        ),
        "result": result,
    }

    # act
    rendered_content = get_diagnostic_rendered_content(x)

    # assert
    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.diagnostic.observed_value",
        "value": {
            "params": expected_params,
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": expected_template,
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
@pytest.mark.parametrize(
    "description, value_set, observed_value, expected_result",
    [
        (
            "no violations (all values in set)",
            ["a", "b", "c"],
            [],  # observed_value now contains only unexpected values
            [],
        ),
        (
            "all unexpected (empty value_set)",
            [],
            ["a", "b", "c"],  # all values are unexpected
            [
                ("ov__0", "a", "unexpected"),
                ("ov__1", "b", "unexpected"),
                ("ov__2", "c", "unexpected"),
            ],
        ),
        (
            "empty column",
            ["a", "b", "c"],
            [],  # no values to be unexpected
            [],
        ),
        (
            "empty input and column",
            [],
            [],
            [],
        ),
        (
            "some unexpected values",
            ["a", "b", "c"],
            ["d", "e"],  # only unexpected values
            [
                ("ov__0", "d", "unexpected"),
                ("ov__1", "e", "unexpected"),
            ],
        ),
    ],
)
def test_expect_column_distinct_values_to_be_in_set_atomic_diagnostic_observed_value(
    description,
    value_set,
    observed_value,
    expected_result,
    get_diagnostic_rendered_content,
):
    # arrange
    x = {
        "expectation_config": ExpectationConfiguration(
            type="expect_column_distinct_values_to_be_in_set",
            kwargs={"value_set": value_set},
        ),
        "result": {"partial_unexpected_list": observed_value},
    }

    expected_template_string = " ".join([f"${name}" for name, _, _ in expected_result])

    # act
    res = get_diagnostic_rendered_content(x).to_json_dict()

    # assert
    assert res["value"]["template"] == expected_template_string

    for name, val, status in expected_result:
        assert name in res["value"]["params"]
        assert res["value"]["params"][name]["value"] == val
        assert res["value"]["params"][name]["render_state"] == status


@pytest.mark.unit
@pytest.mark.parametrize(
    "description, value_set, observed_value, expected_result",
    [
        (
            "no missing values (all expected values in column)",
            ["a", "b", "c"],
            [],  # observed_value now contains only missing values
            [],
        ),
        (
            "empty value_set (nothing expected)",
            [],
            [],  # no values to be missing
            [],
        ),
        (
            "all missing (empty column)",
            ["a", "b", "c"],
            ["a", "b", "c"],  # all expected values are missing
            [
                ("ov__0", "a", "missing"),
                ("ov__1", "b", "missing"),
                ("ov__2", "c", "missing"),
            ],
        ),
        (
            "empty input and column",
            [],
            [],
            [],
        ),
        (
            "some missing values",
            ["a", "b", "c"],
            ["c"],  # only c is missing
            [
                ("ov__0", "c", "missing"),
            ],
        ),
    ],
)
def test_expect_column_distinct_values_to_contain_set_atomic_diagnostic_observed_value(
    description,
    value_set,
    observed_value,
    expected_result,
    get_diagnostic_rendered_content,
):
    # arrange
    x = {
        "expectation_config": ExpectationConfiguration(
            type="expect_column_distinct_values_to_contain_set",
            kwargs={"value_set": value_set},
        ),
        "result": {"partial_missing_list": observed_value},
    }

    expected_template_string = " ".join([f"${name}" for name, _, _ in expected_result])

    # act
    res = get_diagnostic_rendered_content(x).to_json_dict()

    # assert
    assert res["value"]["template"] == expected_template_string

    for name, val, status in expected_result:
        assert name in res["value"]["params"]
        assert res["value"]["params"][name]["value"] == val
        assert res["value"]["params"][name]["render_state"] == status


def _create_result_details_from_expected_result(
    expected_result: List[Tuple[str, str, str]],
) -> Optional[Dict[str, Any]]:
    unexpected: List[str] = []
    missing: List[str] = []
    for _, col, state in expected_result:
        if state == "unexpected":
            unexpected.append(col)
        elif state == "missing":
            missing.append(col)
    if not unexpected and not missing:
        return None
    details: Dict[str, Any] = {"mismatched": {}}
    if unexpected:
        details["mismatched"]["unexpected"] = unexpected
    if missing:
        details["mismatched"]["missing"] = missing
    return details


@pytest.mark.unit
@pytest.mark.parametrize(
    "description, value_set, partial_unexpected_list, partial_missing_list, expected_result",
    [
        (
            "complete set (no violations)",
            ["a", "b", "c"],
            [],
            [],
            [],
        ),
        (
            "all unexpected (empty value_set)",
            [],
            ["a", "b", "c"],
            [],
            [
                ("ov__0", "a", "unexpected"),
                ("ov__1", "b", "unexpected"),
                ("ov__2", "c", "unexpected"),
            ],
        ),
        (
            "all missing (empty column)",
            ["a", "b", "c"],
            [],
            ["a", "b", "c"],
            [
                ("exp__0", "a", "missing"),
                ("exp__1", "b", "missing"),
                ("exp__2", "c", "missing"),
            ],
        ),
        (
            "empty input and observed",
            [],
            [],
            [],
            [],
        ),
        (
            "some missing",
            ["a", "b", "c"],
            [],
            ["c"],
            [
                ("exp__0", "c", "missing"),
            ],
        ),
        (
            "some unexpected",
            ["a", "b", "c"],
            ["d"],
            [],
            [
                ("ov__0", "d", "unexpected"),
            ],
        ),
        (
            "both unexpected and missing",
            ["a", "b", "c"],
            ["d", "e"],
            ["c"],
            [
                ("ov__0", "d", "unexpected"),
                ("ov__1", "e", "unexpected"),
                ("exp__0", "c", "missing"),
            ],
        ),
    ],
)
def test_expect_column_distinct_values_to_equal_set_atomic_diagnostic_observed_value(
    description,
    value_set,
    partial_unexpected_list,
    partial_missing_list,
    expected_result,
    get_diagnostic_rendered_content,
):
    # arrange
    x = {
        "expectation_config": ExpectationConfiguration(
            type="expect_column_distinct_values_to_equal_set",
            kwargs={"value_set": value_set},
        ),
        "result": {
            "partial_unexpected_list": partial_unexpected_list,
            "partial_missing_list": partial_missing_list,
        },
    }

    expected_template_string = " ".join([f"${name}" for name, _, _ in expected_result])

    # act
    res = get_diagnostic_rendered_content(x).to_json_dict()

    # assert
    assert res["value"]["template"] == expected_template_string

    for name, val, status in expected_result:
        assert name in res["value"]["params"]
        assert res["value"]["params"][name]["value"] == val
        assert res["value"]["params"][name]["render_state"] == status


@pytest.mark.unit
@pytest.mark.parametrize(
    "description, column_set, observed_value, expected_result",
    [
        (
            "complete set",
            ["a", "b", "c"],
            ["a", "b", "c"],
            [
                ("ov__0", "a", "expected"),
                ("ov__1", "b", "expected"),
                ("ov__2", "c", "expected"),
            ],
        ),
        (
            "empty input",
            [],
            ["a", "b", "c"],
            [
                ("ov__0", "a", "unexpected"),
                ("ov__1", "b", "unexpected"),
                ("ov__2", "c", "unexpected"),
            ],
        ),
        (
            "empty observed",
            ["a", "b", "c"],
            [],
            [
                ("exp__0", "a", "missing"),
                ("exp__1", "b", "missing"),
                ("exp__2", "c", "missing"),
            ],
        ),
        (
            "empty input and observed",
            [],
            [],
            [],
        ),
        (
            "subset observed",
            ["a", "b", "c"],
            ["a", "b"],
            [
                ("ov__0", "a", "expected"),
                ("ov__1", "b", "expected"),
                ("exp__2", "c", "missing"),
            ],
        ),
        (
            "superset observed",
            ["a", "b", "c"],
            ["a", "b", "c", "d"],
            [
                ("ov__0", "a", "expected"),
                ("ov__1", "b", "expected"),
                ("ov__2", "c", "expected"),
                ("ov__3", "d", "unexpected"),
            ],
        ),
        (
            "superset observed 2",
            ["a", "b", "c"],
            ["a", "d", "b", "c"],
            [
                ("ov__0", "a", "expected"),
                ("ov__1", "d", "unexpected"),
                ("ov__2", "b", "expected"),
                ("ov__3", "c", "expected"),
            ],
        ),
    ],
)
def test_expect_table_columns_to_match_set_atomic_diagnostic_observed_value(
    description,
    column_set,
    observed_value,
    expected_result,
    get_diagnostic_rendered_content,
):
    # arrange
    x = {
        "expectation_config": ExpectationConfiguration(
            type="expect_table_columns_to_match_set",
            kwargs={"column_set": column_set},
        ),
        "result": {"observed_value": observed_value},
    }
    details = _create_result_details_from_expected_result(expected_result)
    if details:
        x["result"]["details"] = details

    expected_template_string = " ".join([f"${name}" for name, _, _ in expected_result])

    # act
    res = get_diagnostic_rendered_content(x).to_json_dict()

    # assert
    assert res["value"]["template"] == expected_template_string

    for name, val, status in expected_result:
        assert name in res["value"]["params"]
        assert res["value"]["params"][name]["value"] == val
        assert res["value"]["params"][name]["render_state"] == status


@pytest.mark.unit
@pytest.mark.parametrize(
    "observed_value, expected_template_string",
    [
        (
            0,
            "$observed_value unexpected rows",
        ),
        (
            1,
            "$observed_value unexpected row",
        ),
        (
            100000,
            "$observed_value unexpected rows",
        ),
    ],
)
def test_unexpected_rows_expectation_atomic_diagnostic_observed_value(
    observed_value,
    expected_template_string,
    get_diagnostic_rendered_content,
):
    # arrange
    x = {
        "expectation_config": ExpectationConfiguration(
            type="unexpected_rows_expectation",
            kwargs={"description": "my description", "unexpected_rows_query": "valid query"},
        ),
        "result": {"observed_value": observed_value},
    }

    # act
    res = get_diagnostic_rendered_content(x).to_json_dict()

    # assert
    assert res["value"]["template"] == expected_template_string


@pytest.mark.unit
def test_unexpected_rows_expectation_atomic_diagnostic_observed_value_when_description_present(
    get_diagnostic_rendered_content,
):
    """Fixes regression where description overwrote the template"""

    x = {
        "expectation_config": ExpectationConfiguration(
            type="unexpected_rows_expectation",
            kwargs={"description": "my description", "unexpected_rows_query": "valid query"},
            description="plz ignore me",
        ),
        "result": {"observed_value": 123},
    }

    res = get_diagnostic_rendered_content(x).to_json_dict()

    assert res["value"]["template"] == "$observed_value unexpected rows"


@pytest.mark.unit
def test_atomic_prescriptive_summary_with_description(
    get_prescriptive_rendered_content,
):
    description = "I should overwite"
    update_dict = {
        "type": "expect_column_distinct_values_to_be_in_set",
        "description": description,
        "kwargs": {
            "column": "my_column",
            "value_set": [1, 2, 3],
        },
    }
    rendered_content = get_prescriptive_rendered_content(update_dict)

    res = rendered_content.to_json_dict()
    pprint(res)
    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "params": {
                "column": {"schema": {"type": "string"}, "value": "my_column"},
                "v__0": {"schema": {"type": "number"}, "value": 1},
                "v__1": {"schema": {"type": "number"}, "value": 2},
                "v__2": {"schema": {"type": "number"}, "value": 3},
                "value_set": {"schema": {"type": "array"}, "value": [1, 2, 3]},
            },
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": description,
        },
        "value_type": "StringValueType",
    }
