from typing import List

import pytest

from great_expectations.core.batch import BatchRequest
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.data_context import DataContext
from great_expectations.render import (
    AtomicDiagnosticRendererType,
    AtomicPrescriptiveRendererType,
    RenderedAtomicContent,
)
from great_expectations.render.exceptions import InvalidRenderedContentError
from great_expectations.render.renderer.inline_renderer import InlineRenderer
from great_expectations.validator.validator import Validator


@pytest.mark.integration
def test_inline_renderer_error_message(basic_expectation_suite: ExpectationSuite):
    expectation_suite: ExpectationSuite = basic_expectation_suite
    with pytest.raises(InvalidRenderedContentError) as e:
        InlineRenderer(render_object=expectation_suite)  # type: ignore
    assert (
        str(e.value)
        == "InlineRenderer can only be used with an ExpectationConfiguration or ExpectationValidationResult, but <class 'great_expectations.core.expectation_suite.ExpectationSuite'> was used."
    )


@pytest.mark.integration
@pytest.mark.parametrize(
    "expectation_configuration,expected_serialized_expectation_configuration_rendered_atomic_content,expected_serialized_expectation_validation_result_rendered_atomic_content",
    [
        pytest.param(
            ExpectationConfiguration(
                expectation_type="expect_table_row_count_to_equal",
                kwargs={"value": 3},
            ),
            [
                {
                    "value_type": "StringValueType",
                    "name": AtomicPrescriptiveRendererType.SUMMARY,
                    "value": {
                        "header": None,
                        "template": "Must have exactly $value rows.",
                        "schema": {"type": "com.superconductive.rendered.string"},
                        "params": {
                            "value": {"schema": {"type": "number"}, "value": 3},
                        },
                    },
                }
            ],
            [
                {
                    "name": AtomicDiagnosticRendererType.OBSERVED_VALUE,
                    "value": {
                        "header": None,
                        "params": {},
                        "schema": {"type": "com.superconductive.rendered.string"},
                        "template": "3",
                    },
                    "value_type": "StringValueType",
                }
            ],
            id="expect_table_row_count_to_equal",
        ),
        pytest.param(
            ExpectationConfiguration(
                expectation_type="expect_column_min_to_be_between",
                kwargs={"column": "event_type", "min_value": 3, "max_value": 20},
            ),
            [
                {
                    "name": AtomicPrescriptiveRendererType.SUMMARY,
                    "value": {
                        "header": None,
                        "params": {
                            "column": {
                                "schema": {"type": "string"},
                                "value": "event_type",
                            },
                            "condition_parser": {
                                "schema": {"type": "string"},
                                "value": None,
                            },
                            "max_value": {"schema": {"type": "number"}, "value": 20},
                            "min_value": {"schema": {"type": "number"}, "value": 3},
                            "parse_strings_as_datetimes": {
                                "schema": {"type": "boolean"},
                                "value": None,
                            },
                            "row_condition": {
                                "schema": {"type": "string"},
                                "value": None,
                            },
                            "strict_max": {
                                "schema": {"type": "boolean"},
                                "value": None,
                            },
                            "strict_min": {
                                "schema": {"type": "boolean"},
                                "value": None,
                            },
                        },
                        "schema": {"type": "com.superconductive.rendered.string"},
                        "template": "$column minimum value must be greater than or equal "
                        "to $min_value and less than or equal to $max_value.",
                    },
                    "value_type": "StringValueType",
                }
            ],
            [
                {
                    "name": AtomicDiagnosticRendererType.OBSERVED_VALUE,
                    "value": {
                        "header": None,
                        "params": {},
                        "schema": {"type": "com.superconductive.rendered.string"},
                        "template": "19",
                    },
                    "value_type": "StringValueType",
                }
            ],
            id="expect_column_min_to_be_between",
        ),
        pytest.param(
            ExpectationConfiguration(
                expectation_type="expect_column_quantile_values_to_be_between",
                kwargs={
                    "column": "user_id",
                    "quantile_ranges": {
                        "quantiles": [0.0, 0.5, 1.0],
                        "value_ranges": [
                            [300000, 400000],
                            [2000000, 4000000],
                            [4000000, 10000000],
                        ],
                    },
                },
            ),
            [
                {
                    "name": AtomicPrescriptiveRendererType.SUMMARY,
                    "value": {
                        "header": {
                            "schema": {"type": "StringValueType"},
                            "value": {
                                "params": {
                                    "column": {
                                        "schema": {"type": "string"},
                                        "value": "user_id",
                                    },
                                    "condition_parser": {
                                        "schema": {"type": "string"},
                                        "value": None,
                                    },
                                    "mostly": {
                                        "schema": {"type": "number"},
                                        "value": None,
                                    },
                                    "row_condition": {
                                        "schema": {"type": "string"},
                                        "value": None,
                                    },
                                },
                                "template": "$column quantiles must be within "
                                "the following value ranges.",
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
                                {"schema": {"type": "string"}, "value": "0.00"},
                                {"schema": {"type": "number"}, "value": 300000},
                                {"schema": {"type": "number"}, "value": 400000},
                            ],
                            [
                                {"schema": {"type": "string"}, "value": "Median"},
                                {"schema": {"type": "number"}, "value": 2000000},
                                {"schema": {"type": "number"}, "value": 4000000},
                            ],
                            [
                                {"schema": {"type": "string"}, "value": "1.00"},
                                {"schema": {"type": "number"}, "value": 4000000},
                                {"schema": {"type": "number"}, "value": 10000000},
                            ],
                        ],
                    },
                    "value_type": "TableType",
                }
            ],
            [
                {
                    "name": AtomicDiagnosticRendererType.OBSERVED_VALUE,
                    "value": {
                        "header": None,
                        "header_row": [
                            {"schema": {"type": "string"}, "value": "Quantile"},
                            {"schema": {"type": "string"}, "value": "Value"},
                        ],
                        "schema": {"type": "TableType"},
                        "table": [
                            [
                                {"schema": {"type": "string"}, "value": "0.00"},
                                {"schema": {"type": "number"}, "value": 397433},
                            ],
                            [
                                {"schema": {"type": "string"}, "value": "Median"},
                                {"schema": {"type": "number"}, "value": 2388055},
                            ],
                            [
                                {"schema": {"type": "string"}, "value": "1.00"},
                                {"schema": {"type": "number"}, "value": 9488404},
                            ],
                        ],
                    },
                    "value_type": "TableType",
                }
            ],
            id="expect_column_quantile_values_to_be_between",
        ),
        pytest.param(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={"column": "event_type", "value_set": [19, 22, 73]},
            ),
            [
                {
                    "name": AtomicPrescriptiveRendererType.SUMMARY,
                    "value": {
                        "header": None,
                        "params": {
                            "column": {
                                "schema": {"type": "string"},
                                "value": "event_type",
                            },
                            "v__0": {"schema": {"type": "number"}, "value": 19},
                            "v__1": {"schema": {"type": "number"}, "value": 22},
                            "v__2": {"schema": {"type": "number"}, "value": 73},
                            "value_set": {
                                "schema": {"type": "array"},
                                "value": [19, 22, 73],
                            },
                        },
                        "schema": {"type": "com.superconductive.rendered.string"},
                        "template": "$column values must belong to this set: $v__0 $v__1 "
                        "$v__2.",
                    },
                    "value_type": "StringValueType",
                }
            ],
            [
                {
                    "name": AtomicDiagnosticRendererType.OBSERVED_VALUE,
                    "value": {
                        "header": None,
                        "params": {},
                        "schema": {"type": "com.superconductive.rendered.string"},
                        "template": "0% unexpected",
                    },
                    "value_type": "StringValueType",
                }
            ],
            id="expect_column_values_to_be_in_set",
        ),
        pytest.param(
            ExpectationConfiguration(
                expectation_type="expect_column_kl_divergence_to_be_less_than",
                kwargs={
                    "column": "user_id",
                    "partition_object": {
                        "values": [2000000, 6000000],
                        "weights": [0.3, 0.7],
                    },
                },
            ),
            [
                {
                    "name": AtomicPrescriptiveRendererType.SUMMARY,
                    "value": {
                        "graph": {
                            "autosize": "fit",
                            "config": {
                                "view": {
                                    "continuousHeight": 300,
                                    "continuousWidth": 400,
                                }
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
                                    "column": {
                                        "schema": {"type": "string"},
                                        "value": "user_id",
                                    },
                                    "condition_parser": {
                                        "schema": {"type": "string"},
                                        "value": None,
                                    },
                                    "mostly": {
                                        "schema": {"type": "number"},
                                        "value": None,
                                    },
                                    "row_condition": {
                                        "schema": {"type": "string"},
                                        "value": None,
                                    },
                                    "threshold": {
                                        "schema": {"type": "number"},
                                        "value": None,
                                    },
                                },
                                "template": "$column Kullback-Leibler (KL) "
                                "divergence with respect to the "
                                "following distribution must be "
                                "lower than $threshold.",
                            },
                        },
                        "schema": {"type": "GraphType"},
                    },
                    "value_type": "GraphType",
                }
            ],
            [
                {
                    "name": AtomicDiagnosticRendererType.OBSERVED_VALUE,
                    "value": {
                        "graph": {
                            "autosize": "fit",
                            "config": {
                                "view": {
                                    "continuousHeight": 300,
                                    "continuousWidth": 400,
                                }
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
                                        "value": "None "
                                        "(-infinity, "
                                        "infinity, "
                                        "or "
                                        "NaN)",
                                    }
                                },
                                "template": "KL Divergence: $observed_value",
                            },
                        },
                        "schema": {"type": "GraphType"},
                    },
                    "value_type": "GraphType",
                }
            ],
            id="expect_column_kl_divergence_to_be_less_than",
        ),
    ],
)
@pytest.mark.slow  # 5.82s
def test_inline_renderer_rendered_content_return_value(
    alice_columnar_table_single_batch_context: DataContext,
    expectation_configuration: ExpectationConfiguration,
    expected_serialized_expectation_configuration_rendered_atomic_content: dict,
    expected_serialized_expectation_validation_result_rendered_atomic_content: dict,
):
    context: DataContext = alice_columnar_table_single_batch_context
    batch_request: BatchRequest = BatchRequest(
        datasource_name="alice_columnar_table_single_batch_datasource",
        data_connector_name="alice_columnar_table_single_batch_data_connector",
        data_asset_name="alice_columnar_table_single_batch_data_asset",
    )
    suite: ExpectationSuite = context.create_expectation_suite("validating_alice_data")

    validator: Validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite=suite,
        include_rendered_content=True,
    )

    expectation_validation_result: ExpectationValidationResult = (
        validator.graph_validate(configurations=[expectation_configuration])
    )[0]

    inline_renderer: InlineRenderer = InlineRenderer(
        render_object=expectation_validation_result
    )

    expectation_validation_result_rendered_atomic_content: List[
        RenderedAtomicContent
    ] = inline_renderer.get_rendered_content()

    inline_renderer: InlineRenderer = InlineRenderer(
        render_object=expectation_validation_result.expectation_config
    )

    expectation_configuration_rendered_atomic_content: List[
        RenderedAtomicContent
    ] = inline_renderer.get_rendered_content()

    actual_serialized_expectation_configuration_rendered_atomic_content: List[dict] = [
        rendered_atomic_content.to_json_dict()
        for rendered_atomic_content in expectation_configuration_rendered_atomic_content
    ]
    if (
        actual_serialized_expectation_configuration_rendered_atomic_content[0][
            "value_type"
        ]
        == "GraphType"
    ):
        actual_serialized_expectation_configuration_rendered_atomic_content[0]["value"][
            "graph"
        ].pop("$schema")
        actual_serialized_expectation_configuration_rendered_atomic_content[0]["value"][
            "graph"
        ].pop("data")
        actual_serialized_expectation_configuration_rendered_atomic_content[0]["value"][
            "graph"
        ].pop("datasets")

    actual_serialized_expectation_validation_result_rendered_atomic_content: List[
        dict
    ] = [
        rendered_atomic_content.to_json_dict()
        for rendered_atomic_content in expectation_validation_result_rendered_atomic_content
    ]
    if (
        actual_serialized_expectation_validation_result_rendered_atomic_content[0][
            "value_type"
        ]
        == "GraphType"
    ):
        actual_serialized_expectation_validation_result_rendered_atomic_content[0][
            "value"
        ]["graph"].pop("$schema")
        actual_serialized_expectation_validation_result_rendered_atomic_content[0][
            "value"
        ]["graph"].pop("data")
        actual_serialized_expectation_validation_result_rendered_atomic_content[0][
            "value"
        ]["graph"].pop("datasets")

    assert (
        actual_serialized_expectation_configuration_rendered_atomic_content
        == expected_serialized_expectation_configuration_rendered_atomic_content
    )
    assert (
        actual_serialized_expectation_validation_result_rendered_atomic_content
        == expected_serialized_expectation_validation_result_rendered_atomic_content
    )
