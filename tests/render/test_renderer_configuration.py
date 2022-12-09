import pytest

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)
from great_expectations.render.renderer_configuration import RendererConfiguration


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


@pytest.mark.parametrize(
    "expectation_type,kwargs,fake_result",
    [("expect_table_row_count_to_equal", {"value": 3}, {"observed_value": 3})],
)
def test_renderer_configuration_instantiation(expectation_type, kwargs, fake_result):
    expectation_configuration = ExpectationConfiguration(
        expectation_type=expectation_type,
        kwargs=kwargs,
    )
    renderer_configuration = RendererConfiguration(
        configuration=expectation_configuration,
        runtime_configuration={},
    )
    assert renderer_configuration.expectation_type == expectation_type
    assert renderer_configuration.kwargs == kwargs
    assert renderer_configuration.include_column_name is True

    expectation_validation_result = (
        mock_expectation_validation_result_from_expectation_configuration(
            expectation_configuration=expectation_configuration,
            fake_result=fake_result,
        )
    )
    renderer_configuration = RendererConfiguration(
        result=expectation_validation_result,
        runtime_configuration={},
    )
    assert renderer_configuration.expectation_type == expectation_type
    assert renderer_configuration.kwargs == kwargs
    assert renderer_configuration.include_column_name is True
