import re
from pprint import pprint
from typing import Callable, Dict, Union

import pytest

from great_expectations.core import ExpectationValidationResult
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.expectations.registry import get_renderer_impl
from great_expectations.render import RenderedAtomicContent


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
        config = ExpectationConfiguration(**expectation_configuration_kwargs)
        expectation_type = expectation_configuration_kwargs["type"]

        # Programatically determine the renderer implementations
        renderer_impl = get_renderer_impl(
            object_name=expectation_type,
            renderer_type="atomic.prescriptive.summary",
        )[1]

        # Determine RenderedAtomicContent output
        source_obj = {"configuration": config}
        res = renderer_impl(**source_obj)
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
        evr = ExpectationValidationResult(**evr_kwargs)
        expectation_config = evr_kwargs["expectation_config"]
        expectation_type = expectation_config["type"]

        # Programatically determine the renderer implementations
        renderer_impl = get_renderer_impl(
            object_name=expectation_type,
            renderer_type="atomic.diagnostic.observed_value",
        )[1]

        # Determine RenderedAtomicContent output
        source_obj = {"result": evr}
        res = renderer_impl(**source_obj)
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

    # replace version of vega-lite in res to match snapshot test
    res["value"]["graph"]["$schema"] = re.sub(
        r"v\d*\.\d*\.\d*", "v4.8.1", res["value"]["graph"]["$schema"]
    )

    assert res == {
        "name": "atomic.prescriptive.summary",
        "value": {
            "graph": {
                "$schema": "https://vega.github.io/schema/vega-lite/v4.8.1.json",
                "autosize": "fit",
                "config": {"view": {"continuousHeight": 300, "continuousWidth": 400}},
                "data": {"name": "data-1cb20570b53cc3e67cb4883fd45e64cb"},
                "datasets": {
                    "data-1cb20570b53cc3e67cb4883fd45e64cb": [
                        {"bin_max": 5, "bin_min": 0, "fraction": 0.2},
                        {"bin_max": 10, "bin_min": 5, "fraction": 0.3},
                        {"bin_max": 30, "bin_min": 10, "fraction": 0.1},
                        {"bin_max": 50, "bin_min": 30, "fraction": 0.4},
                    ]
                },
                "encoding": {
                    "tooltip": [
                        {"field": "bin_min", "type": "quantitative"},
                        {"field": "bin_max", "type": "quantitative"},
                        {"field": "fraction", "type": "quantitative"},
                    ],
                    "x": {"field": "bin_min", "type": "ordinal"},
                    "x2": {"field": "bin_max"},
                    "y": {"field": "fraction", "type": "quantitative"},
                },
                "height": 400,
                "mark": "bar",
                "width": 250,
            },
            "header": {
                "schema": {"type": "StringValueType"},
                "value": {
                    "params": {
                        "column": {"schema": {"type": "string"}, "value": "min_event_time"},
                        "threshold": {"schema": {"type": "number"}, "value": 0.1},
                    },
                    "template": "$column Kullback-Leibler (KL) divergence with respect to the following distribution must be lower than $threshold.",  # noqa: E501
                },
            },
            "schema": {"type": "GraphType"},
        },
        "value_type": "GraphType",
    }


@pytest.mark.unit
def test_atomic_diagnostic_observed_value_expect_column_kl_divergence_to_be_less_than(
    get_diagnostic_rendered_content,
):
    # Please note that the vast majority of Expectations are calling `Expectation._atomic_diagnostic_observed_value()`  # noqa: E501
    # As such, the specific expectation_type used here is irrelevant and is simply used to trigger the parent class.  # noqa: E501
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

    # replace version of vega-lite in res to match snapshot test
    res["value"]["graph"]["$schema"] = re.sub(
        r"v\d*\.\d*\.\d*", "v4.8.1", res["value"]["graph"]["$schema"]
    )
    assert res == {
        "name": "atomic.diagnostic.observed_value",
        "value": {
            "graph": {
                "$schema": "https://vega.github.io/schema/vega-lite/v4.8.1.json",
                "autosize": "fit",
                "config": {"view": {"continuousHeight": 300, "continuousWidth": 400}},
                "data": {"name": "data-c49f2d3d7bdab36f9ec81f5a314a430c"},
                "datasets": {
                    "data-c49f2d3d7bdab36f9ec81f5a314a430c": [
                        {"fraction": 0.3754, "values": 1},
                        {"fraction": 0.615, "values": 2},
                        {"fraction": 0.0096, "values": 4},
                    ]
                },
                "encoding": {
                    "tooltip": [
                        {"field": "values", "type": "quantitative"},
                        {"field": "fraction", "type": "quantitative"},
                    ],
                    "x": {"field": "values", "type": "nominal"},
                    "y": {"field": "fraction", "type": "quantitative"},
                },
                "height": 400,
                "mark": "bar",
                "width": 250,
            },
            "header": {
                "schema": {"type": "StringValueType"},
                "value": {
                    "params": {
                        "observed_value": {
                            "schema": {"type": "string"},
                            "value": "None (-infinity, infinity, or NaN)",
                        }
                    },
                    "template": "KL Divergence: $observed_value",
                },
            },
            "schema": {"type": "GraphType"},
        },
        "value_type": "GraphType",
    }


@pytest.mark.unit
def test_atomic_diagnostic_observed_value_with_boolean_column_expect_column_kl_divergence_to_be_less_than(  # noqa: E501
    get_diagnostic_rendered_content,
):
    # Please note that the vast majority of Expectations are calling `Expectation._atomic_diagnostic_observed_value()`  # noqa: E501
    # As such, the specific expectation_type used here is irrelevant and is simply used to trigger the parent class.  # noqa: E501
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

    # replace version of vega-lite in res to match snapshot test
    res["value"]["graph"]["$schema"] = re.sub(
        r"v\d*\.\d*\.\d*", "v4.8.1", res["value"]["graph"]["$schema"]
    )
    assert res == {
        "name": "atomic.diagnostic.observed_value",
        "value": {
            "graph": {
                "$schema": "https://vega.github.io/schema/vega-lite/v4.8.1.json",
                "autosize": "fit",
                "config": {"view": {"continuousHeight": 300, "continuousWidth": 400}},
                "data": {"name": "data-d8f1a1ab1f79e142d9ca399157673554"},
                "datasets": {
                    "data-d8f1a1ab1f79e142d9ca399157673554": [
                        {"fraction": 0.5, "values": "True"},
                        {"fraction": 0.5, "values": "False"},
                    ]
                },
                "encoding": {
                    "tooltip": [
                        {"field": "values", "type": "nominal"},
                        {"field": "fraction", "type": "quantitative"},
                    ],
                    "x": {"field": "values", "type": "nominal"},
                    "y": {"field": "fraction", "type": "quantitative"},
                },
                "height": 400,
                "mark": "bar",
                "width": 250,
            },
            "header": {
                "schema": {"type": "StringValueType"},
                "value": {
                    "params": {
                        "observed_value": {
                            "schema": {"type": "string"},
                            "value": "None (-infinity, infinity, or NaN)",
                        }
                    },
                    "template": "KL Divergence: $observed_value",
                },
            },
            "schema": {"type": "GraphType"},
        },
        "value_type": "GraphType",
    }


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
            "template": "$column maximum value must be greater than or equal to $min_value and less than or equal to $max_value.",  # noqa: E501
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
            "template": "$column mean must be greater than or equal to $min_value and less than or equal to $max_value.",  # noqa: E501
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
            "template": "$column median must be greater than or equal to $min_value and less than or equal to $max_value.",  # noqa: E501
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
            "template": "$column minimum value must be greater than or equal to $min_value and less than or equal to $max_value.",  # noqa: E501
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
            "template": "$column most common value must belong to this set: $v__0 $v__1 $v__2. Values outside this set that are as common (but not more common) are allowed.",  # noqa: E501
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
            "template": "Values in $column_A must be greater than or equal to those in $column_B, at least $mostly_pct % of the time.",  # noqa: E501
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
            "template": "Values in $column_A and $column_B must be equal, at least $mostly_pct % of the time.",  # noqa: E501
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
def test_atomic_prescriptive_summary_expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(  # noqa: E501
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
            "template": "$column fraction of unique values must be greater than or equal to $min_value and less than or equal to $max_value.",  # noqa: E501
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
    # Please note that the vast majority of Expectations are calling `Expectation._atomic_diagnostic_observed_value()`  # noqa: E501
    # As such, the specific expectation_type used here is irrelevant and is simply used to trigger the parent class.  # noqa: E501
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
            "template": "$column standard deviation must be greater than or equal to $min_value and less than or equal to $max_value.",  # noqa: E501
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
            "template": "$column sum must be greater than or equal to $min_value and less than or equal to $max_value.",  # noqa: E501
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
            "template": "$column must have greater than or equal to $min_value and less than or equal to $max_value unique values.",  # noqa: E501
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
            "template": "$column values must be greater than or equal to $min_value and less than or equal to $max_value characters long, at least $mostly_pct % of the time.",  # noqa: E501
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
            "template": "$column values must be $value characters long, at least $mostly_pct % of the time.",  # noqa: E501
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
            "template": "$column values must be greater than or equal to $min_value and less than or equal to $max_value, at least $mostly_pct % of the time.",  # noqa: E501
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
            "template": "$column values must be parseable by dateutil, at least $mostly_pct % of the time.",  # noqa: E501
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
            "template": "$column values must be strictly less than previous values, at least $mostly_pct % of the time.",  # noqa: E501
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
            "template": "$column values must belong to this set: $v__0 $v__1 $v__2 $v__3, at least $mostly_pct % of the time.",  # noqa: E501
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
            "template": "$column value types must belong to this set: $v__0 $v__1 $v__2, at least $mostly_pct % of the time.",  # noqa: E501
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
            "template": "$column values must be strictly greater than previous values, at least $mostly_pct % of the time.",  # noqa: E501
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
            "template": "$column values must be parseable as JSON, at least $mostly_pct % of the time.",  # noqa: E501
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
            "template": "$column values must be of type $type_, at least $mostly_pct % of the time.",  # noqa: E501
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
            "template": "$column values must match the following JSON Schema, at least $mostly_pct % of the time: $formatted_json",  # noqa: E501
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
            "template": "$column values must match this regular expression: $regex, at least $mostly_pct % of the time.",  # noqa: E501
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
            "template": "$column values must match all of the following regular expressions: $v__0 $v__1, at least $mostly_pct % of the time.",  # noqa: E501
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
            "template": "$column values must match the following strftime format: $strftime_format, at least $mostly_pct % of the time.",  # noqa: E501
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
            "template": "$column values must not belong to this set: $v__0 $v__1 $v__2, at least $mostly_pct % of the time.",  # noqa: E501
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
            "template": "$column values must not match this regular expression: $regex, at least $mostly_pct % of the time.",  # noqa: E501
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
            "template": "$column values must not match any of the following regular expressions: $v__0 $v__1 $v__2, at least $mostly_pct % of the time.",  # noqa: E501
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
            "template": "Values for given compound columns must be unique together, at least $mostly_pct % of the time: $column_list_0 $column_list_1 $column_list_2",  # noqa: E501
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
            "template": "Values must be unique across columns, at least $mostly_pct % of the time: $column_list_0 $column_list_1 $column_list_2",  # noqa: E501
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
            "template": "Values must be unique across columns, at least $mostly_pct % of the time: $column_list_0 $column_list_1",  # noqa: E501
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
            "template": "Must have these columns in this order: $column_list_0 $column_list_1 $column_list_2",  # noqa: E501
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
            "template": "Must have exactly these columns (in any order): $column_set_0 $column_set_1 $column_set_2",  # noqa: E501
        },
        "value_type": "StringValueType",
    }


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
    # Please note that the vast majority of Expectations are calling `Expectation._atomic_diagnostic_observed_value()`  # noqa: E501
    # As such, the specific expectation_type used here is irrelevant and is simply used to trigger the parent class.  # noqa: E501
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
            "params": {},
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "--",
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_diagnostic_observed_value_with_numeric_observed_value(
    get_diagnostic_rendered_content,
):
    # Please note that the vast majority of Expectations are calling `Expectation._atomic_diagnostic_observed_value()`  # noqa: E501
    # As such, the specific expectation_type used here is irrelevant and is simply used to trigger the parent class.  # noqa: E501
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
            "params": {},
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "1,776",
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_diagnostic_observed_value_with_str_observed_value(get_diagnostic_rendered_content):
    # Please note that the vast majority of Expectations are calling `Expectation._atomic_diagnostic_observed_value()`  # noqa: E501
    # As such, the specific expectation_type used here is irrelevant and is simply used to trigger the parent class.  # noqa: E501
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
            "params": {},
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "foo",
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_diagnostic_observed_value_with_unexpected_percent(get_diagnostic_rendered_content):
    # Please note that the vast majority of Expectations are calling `Expectation._atomic_diagnostic_observed_value()`  # noqa: E501
    # As such, the specific expectation_type used here is irrelevant and is simply used to trigger the parent class.  # noqa: E501
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
            "params": {},
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "10% unexpected",
        },
        "value_type": "StringValueType",
    }


@pytest.mark.unit
def test_atomic_diagnostic_observed_value_with_empty_result(get_diagnostic_rendered_content):
    # Please note that the vast majority of Expectations are calling `Expectation._atomic_diagnostic_observed_value()`  # noqa: E501
    # As such, the specific expectation_type used here is irrelevant and is simply used to trigger the parent class.  # noqa: E501
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
            "params": {},
            "schema": {"type": "com.superconductive.rendered.string"},
            "template": "--",
        },
        "value_type": "StringValueType",
    }
