from great_expectations.profiler.parameter_builder.parameter_container import (
    ParameterContainer,
    build_parameter_container,
)


def test_build_parameter_container(
    parameter_values_eight_parameters_multiple_depths, multi_part_parameter_container
):
    parameter_container: ParameterContainer = build_parameter_container(
        parameter_values=parameter_values_eight_parameters_multiple_depths
    )
    assert parameter_container == multi_part_parameter_container
