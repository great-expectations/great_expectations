from typing import Union

import pytest

from great_expectations.exceptions import ProfilerConfigurationError
from great_expectations.rule_based_profiler.domain_builder.categorical_column_domain_builder import (
    AbsoluteCardinalityLimit,
    CardinalityChecker,
    CardinalityLimitMode,
    RelativeCardinalityLimit,
)


def test_cardinality_checker_instantiation_valid_limit_mode_parameter_absolute():
    cardinality_checker: CardinalityChecker = CardinalityChecker(
        limit_mode=CardinalityLimitMode.MANY
    )
    assert cardinality_checker.limit_mode == CardinalityLimitMode.MANY.value
    assert isinstance(cardinality_checker.limit_mode, AbsoluteCardinalityLimit)
    assert cardinality_checker.limit_mode == AbsoluteCardinalityLimit(
        name="MANY", max_unique_values=10000
    )


def test_cardinality_checker_instantiation_valid_limit_mode_parameter_relative():
    cardinality_checker: CardinalityChecker = CardinalityChecker(
        limit_mode=CardinalityLimitMode.REL_1
    )
    assert cardinality_checker.limit_mode == CardinalityLimitMode.REL_1.value
    assert isinstance(cardinality_checker.limit_mode, RelativeCardinalityLimit)
    assert cardinality_checker.limit_mode == RelativeCardinalityLimit(
        name="REL_1", max_proportion_unique=0.01
    )


def test_cardinality_checker_instantiation_valid_limit_mode_parameter_str_absolute():
    cardinality_checker: CardinalityChecker = CardinalityChecker(limit_mode="VERY_MANY")
    assert cardinality_checker.limit_mode == CardinalityLimitMode.VERY_MANY.value
    assert isinstance(cardinality_checker.limit_mode, AbsoluteCardinalityLimit)
    assert cardinality_checker.limit_mode == AbsoluteCardinalityLimit(
        name="VERY_MANY", max_unique_values=100000
    )


def test_cardinality_checker_instantiation_valid_limit_mode_parameter_str_relative():
    cardinality_checker: CardinalityChecker = CardinalityChecker(limit_mode="REL_0_1")
    assert cardinality_checker.limit_mode == CardinalityLimitMode.REL_0_1.value
    assert isinstance(cardinality_checker.limit_mode, RelativeCardinalityLimit)
    assert cardinality_checker.limit_mode == RelativeCardinalityLimit(
        name="REL_0_1", max_proportion_unique=0.001
    )


def test_cardinality_checker_instantiation_valid_max_unique_values_parameter():
    cardinality_checker: CardinalityChecker = CardinalityChecker(
        max_unique_values=12345
    )
    assert isinstance(cardinality_checker.limit_mode, AbsoluteCardinalityLimit)
    assert cardinality_checker.limit_mode == AbsoluteCardinalityLimit(
        name="CUSTOM_ABS_12345", max_unique_values=12345
    )


def test_cardinality_checker_instantiation_valid_max_proportion_unique_parameter_float():
    cardinality_checker: CardinalityChecker = CardinalityChecker(
        max_proportion_unique=0.42
    )
    assert isinstance(cardinality_checker.limit_mode, RelativeCardinalityLimit)
    assert cardinality_checker.limit_mode == RelativeCardinalityLimit(
        name="CUSTOM_REL_0.42", max_proportion_unique=0.42
    )


def test_cardinality_checker_instantiation_valid_max_proportion_unique_parameter_int():
    cardinality_checker: CardinalityChecker = CardinalityChecker(
        max_proportion_unique=1
    )
    assert isinstance(cardinality_checker.limit_mode, RelativeCardinalityLimit)
    assert cardinality_checker.limit_mode == RelativeCardinalityLimit(
        name="CUSTOM_REL_1", max_proportion_unique=1
    )


def test_cardinality_checker_instantiation_invalid_limit_mode_parameter():
    with pytest.raises(ProfilerConfigurationError) as excinfo:
        _: CardinalityChecker = CardinalityChecker(
            limit_mode="&^*%not_a_valid_mode^&*("
        )
    assert "specify a supported cardinality mode" in str(excinfo.value)
    assert "REL_1" in str(excinfo.value)
    assert "MANY" in str(excinfo.value)


def test_cardinality_checker_instantiation_invalid_max_unique_values_parameter():
    with pytest.raises(ProfilerConfigurationError) as excinfo:
        # noinspection PyTypeChecker
        _: CardinalityChecker = CardinalityChecker(
            max_unique_values="invalid_input_type_string"
        )
    assert "specify an int" in str(excinfo.value)
    assert "str" in str(excinfo.value)


def test_cardinality_checker_instantiation_invalid_max_proportion_unique_parameter():
    with pytest.raises(ProfilerConfigurationError) as excinfo:
        # noinspection PyTypeChecker
        _: CardinalityChecker = CardinalityChecker(
            max_proportion_unique="invalid_input_type_string"
        )
    assert "specify a float or int" in str(excinfo.value)
    assert "str" in str(excinfo.value)


def test_cardinality_checker_instantiation_invalid_multiple_parameters():
    with pytest.raises(ProfilerConfigurationError) as excinfo:
        _: CardinalityChecker = CardinalityChecker(
            limit_mode="REL_0_1", max_proportion_unique=0.42
        )
    assert "Please pass ONE of the following parameters" in str(excinfo.value)
    assert "you passed 2 parameters" in str(excinfo.value)


@pytest.mark.parametrize(
    "input_limit_mode",
    [member for member in CardinalityLimitMode]
    + [member.name for member in CardinalityLimitMode],
)
def test_exhaustively_cardinality_within_limit_for_all_supported_cardinality_limits(
    input_limit_mode,
):
    """What does this test and why?

    This test checks that the CardinalityChecker appropriately handles all types of limits,
    whether created using a CardinalityLimitMode or a string.
    """

    cardinality_checker: CardinalityChecker = CardinalityChecker(
        limit_mode=input_limit_mode
    )

    # Set up passing and failing cardinality measured values
    cardinality_limit: float
    limit_mode: Union[
        AbsoluteCardinalityLimit, RelativeCardinalityLimit
    ] = cardinality_checker.limit_mode
    if isinstance(limit_mode, AbsoluteCardinalityLimit):
        cardinality_limit = limit_mode.max_unique_values
        if cardinality_limit > 0:
            passing_cardinality = cardinality_limit - 1
        else:
            passing_cardinality = 0
        failing_cardinality = cardinality_limit + 1
    elif isinstance(limit_mode, RelativeCardinalityLimit):
        cardinality_limit = limit_mode.max_proportion_unique
        passing_cardinality = 0.0
        failing_cardinality = cardinality_limit + 0.01
    else:
        raise ValueError("limit_mode should be a supported type")

    assert cardinality_checker.cardinality_within_limit(passing_cardinality)
    assert not cardinality_checker.cardinality_within_limit(failing_cardinality)


def test_cardinality_checker_cardinality_within_limit_max_unique_values_parameter():
    input_max_unique_values: int = 12345
    cardinality_checker: CardinalityChecker = CardinalityChecker(
        max_unique_values=input_max_unique_values
    )
    passing_cardinality: int = input_max_unique_values - 1
    failing_cardinality: int = input_max_unique_values + 1
    assert cardinality_checker.cardinality_within_limit(passing_cardinality)
    assert not cardinality_checker.cardinality_within_limit(failing_cardinality)


def test_cardinality_checker_cardinality_within_limit_max_proportion_unique_parameter_float():
    input_max_proportion_unique: float = 0.42
    cardinality_checker: CardinalityChecker = CardinalityChecker(
        max_proportion_unique=input_max_proportion_unique
    )
    passing_cardinality: float = input_max_proportion_unique - 0.01
    failing_cardinality: float = input_max_proportion_unique + 0.01
    assert cardinality_checker.cardinality_within_limit(passing_cardinality)
    assert not cardinality_checker.cardinality_within_limit(failing_cardinality)


def test_cardinality_checker_cardinality_within_limit_max_proportion_unique_parameter_int():
    input_max_proportion_unique: int = 1
    cardinality_checker: CardinalityChecker = CardinalityChecker(
        max_proportion_unique=input_max_proportion_unique
    )
    passing_cardinality: float = input_max_proportion_unique - 0.01
    failing_cardinality: float = input_max_proportion_unique + 0.01
    assert cardinality_checker.cardinality_within_limit(passing_cardinality)
    assert not cardinality_checker.cardinality_within_limit(failing_cardinality)
