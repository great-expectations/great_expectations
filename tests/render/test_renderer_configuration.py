from typing import TYPE_CHECKING, Union

import pytest

from great_expectations.compatibility.pydantic import (
    error_wrappers as pydantic_error_wrappers,
)
from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)
from great_expectations.expectations.expectation import Expectation
from great_expectations.render.renderer_configuration import (
    RendererConfiguration,
    RendererValueType,
)

if TYPE_CHECKING:
    from great_expectations.render.renderer_configuration import AddParamArgs


def mock_expectation_validation_result_from_expectation_configuration(
    expectation_configuration: ExpectationConfiguration,
    fake_result: dict,
) -> ExpectationValidationResult:
    return ExpectationValidationResult(
        exception_info={
            "raised_exception": False,
            "exception_traceback": None,
            "exception_message": None,
        },
        expectation_config=expectation_configuration,
        result=fake_result,
        success=True,
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    "expectation_type,kwargs,runtime_configuration,fake_result,include_column_name",
    [
        (
            "expect_table_row_count_to_equal",
            {"value": 3},
            None,
            {"observed_value": 3},
            True,
        ),
        (
            "expect_table_row_count_to_equal",
            {"value": 3},
            {},
            {"observed_value": 3},
            True,
        ),
        (
            "expect_table_row_count_to_equal",
            {"value": 3},
            {"include_column_name": False},
            {"observed_value": 3},
            False,
        ),
    ],
)
def test_successful_renderer_configuration_instantiation(
    expectation_type: str,
    kwargs: dict,
    runtime_configuration: dict,
    fake_result: dict,
    include_column_name: bool,
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type=expectation_type,
        kwargs=kwargs,
    )
    renderer_configuration = RendererConfiguration(
        configuration=expectation_configuration,
        runtime_configuration=runtime_configuration,
    )
    assert renderer_configuration.expectation_type == expectation_type
    assert renderer_configuration.kwargs == kwargs
    assert renderer_configuration.include_column_name is include_column_name

    expectation_validation_result = (
        mock_expectation_validation_result_from_expectation_configuration(
            expectation_configuration=expectation_configuration,
            fake_result=fake_result,
        )
    )
    renderer_configuration = RendererConfiguration(
        result=expectation_validation_result,
        runtime_configuration=runtime_configuration,
    )
    assert renderer_configuration.expectation_type == expectation_type
    assert renderer_configuration.kwargs == kwargs
    assert renderer_configuration.include_column_name is include_column_name


@pytest.mark.unit
@pytest.mark.xfail(
    reason="As of v0.15.46 test will fail until RendererConfiguration._validate_configuration_or_result is re-enabled.",
    strict=True,
)
def test_failed_renderer_configuration_instantiation():
    with pytest.raises(pydantic_error_wrappers.ValidationError) as e:
        RendererConfiguration(
            runtime_configuration={},
        )
    assert any(
        str(error_wrapper_exc)
        == "RendererConfiguration must be passed either configuration or result."
        for error_wrapper_exc in [
            error_wrapper.exc for error_wrapper in e.value.raw_errors
        ]
    )


class NotString:
    def __str__(self):
        raise TypeError("I'm not a string")


@pytest.mark.unit
@pytest.mark.parametrize(
    "param_type,value",
    [
        (RendererValueType.STRING, NotString()),
        (RendererValueType.NUMBER, "ABC"),
        (RendererValueType.BOOLEAN, 3),
        (RendererValueType.ARRAY, 3),
    ],
)
def test_renderer_configuration_add_param_validation(
    param_type: RendererValueType, value: Union[NotString, str, int]
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_table_row_count_to_equal",
        kwargs={"value": value},
    )
    renderer_configuration = RendererConfiguration(
        configuration=expectation_configuration
    )
    with pytest.raises(pydantic_error_wrappers.ValidationError) as e:
        renderer_configuration.add_param(name="value", param_type=param_type)

    if param_type is RendererValueType.STRING:
        exception_message = (
            "Value was unable to be represented as a string: I'm not a string"
        )
    else:
        exception_message = (
            f"Param type: <{param_type}> does not match value: <{value}>."
        )
    assert any(
        str(error_wrapper_exc) == exception_message
        for error_wrapper_exc in [
            error_wrapper.exc for error_wrapper in e.value.raw_errors
        ]
    )


@pytest.mark.unit
def test_add_param_args():
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_value_z_scores_to_be_less_than",
        kwargs={"column": "foo", "threshold": 2, "double_sided": False, "mostly": 1.0},
    )
    renderer_configuration = RendererConfiguration(
        configuration=expectation_configuration
    )

    add_param_args: AddParamArgs = (
        ("column", RendererValueType.STRING),
        ("threshold", RendererValueType.NUMBER),
        ("double_sided", RendererValueType.BOOLEAN),
        ("mostly", RendererValueType.NUMBER),
    )
    for name, param_type in add_param_args:
        renderer_configuration.add_param(name=name, param_type=param_type)

    params = renderer_configuration.params

    assert params.column
    assert params.threshold
    assert params.double_sided
    assert params.mostly


@pytest.mark.unit
def test_template_str_setter():
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_value_z_scores_to_be_less_than",
        kwargs={"column": "foo", "threshold": 2, "double_sided": False, "mostly": 1.0},
    )
    renderer_configuration = RendererConfiguration(
        configuration=expectation_configuration
    )

    template_str = "My rendered string"
    renderer_configuration.template_str = template_str

    assert renderer_configuration.template_str == template_str


@pytest.mark.unit
def test_add_array_params():
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_match_like_pattern_list",
        kwargs={
            "column": "foo",
            "like_pattern_list": ["%", "_"],
            "match_on": "any",
            "mostly": 1.0,
        },
    )
    renderer_configuration = RendererConfiguration(
        configuration=expectation_configuration
    )
    renderer_configuration.add_param(
        name="like_pattern_list", param_type=RendererValueType.ARRAY
    )

    array_param_name = "like_pattern_list"
    param_prefix = array_param_name + "_"

    renderer_configuration = Expectation._add_array_params(
        array_param_name=array_param_name,
        param_prefix=param_prefix,
        renderer_configuration=renderer_configuration,
    )
    params = renderer_configuration.params

    assert params.like_pattern_list_0
    assert params.like_pattern_list_1

    array_string = Expectation._get_array_string(
        array_param_name=array_param_name,
        param_prefix=param_prefix,
        renderer_configuration=renderer_configuration,
    )

    assert array_string == "$like_pattern_list_0 $like_pattern_list_1"
