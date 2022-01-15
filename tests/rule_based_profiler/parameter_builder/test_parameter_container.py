from great_expectations.rule_based_profiler.parameter_builder import (
    ParameterContainer,
    build_parameter_container,
)


def test_build_parameter_container(
    parameters_with_different_depth_level_values,
    multi_part_name_parameter_container,
):
    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)
    build_parameter_container(
        parameter_container=parameter_container,
        parameter_values=parameters_with_different_depth_level_values,
    )
    assert parameter_container == multi_part_name_parameter_container
