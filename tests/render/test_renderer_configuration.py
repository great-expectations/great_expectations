from typing import Union

import pytest
from pydantic.error_wrappers import ValidationError

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)
from great_expectations.render.renderer_configuration import (
    RendererConfiguration,
    RendererValueType,
)


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
    with pytest.raises(ValidationError) as e:
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
    with pytest.raises(ValidationError) as e:
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
