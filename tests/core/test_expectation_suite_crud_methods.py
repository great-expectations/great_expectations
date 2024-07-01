from copy import deepcopy

import pytest

from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.exceptions import (
    DataContextError,
    InvalidExpectationConfigurationError,
)
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)


@pytest.fixture
def empty_suite() -> ExpectationSuite:
    return ExpectationSuite(
        name="warning",
        expectations=[],
        meta={"notes": "This is an expectation suite."},
    )


@pytest.fixture
def baseline_suite(exp1, exp2) -> ExpectationSuite:
    return ExpectationSuite(
        name="warning",
        expectations=[exp1, exp2],
        meta={"notes": "This is an expectation suite."},
    )


@pytest.fixture
def exp1() -> ExpectationConfiguration:
    return ExpectationConfiguration(
        type="expect_column_values_to_be_in_set",
        kwargs={"column": "a", "value_set": [1, 2, 3], "result_format": "BASIC"},
        meta={"notes": "This is an expectation."},
    )


@pytest.fixture
def exp2() -> ExpectationConfiguration:
    return ExpectationConfiguration(
        type="expect_column_values_to_be_in_set",
        kwargs={"column": "b", "value_set": [-1, -2, -3], "result_format": "BASIC"},
        meta={"notes": "This is an expectation."},
    )


@pytest.fixture
def exp3() -> ExpectationConfiguration:
    return ExpectationConfiguration(
        type="expect_column_values_to_be_in_set",
        kwargs={"column": "b", "value_set": [-1, -2, -3], "result_format": "BASIC"},
        meta={"notes": "This is an expectation."},
    )


@pytest.fixture
def exp4() -> ExpectationConfiguration:
    return ExpectationConfiguration(
        type="expect_column_values_to_be_in_set",
        kwargs={"column": "b", "value_set": [1, 2, 3], "result_format": "BASIC"},
        meta={"notes": "This is an expectation."},
    )


@pytest.fixture
def exp5() -> ExpectationConfiguration:
    return ExpectationConfiguration(
        type="expect_column_values_to_be_in_set",
        kwargs={"column": "b", "value_set": [1, 2, 3], "result_format": "COMPLETE"},
        meta={"notes": "This is an expectation."},
    )


@pytest.fixture
def exp6() -> ExpectationConfiguration:
    return ExpectationConfiguration(
        type="expect_column_values_to_be_in_set",
        kwargs={"column": "b", "value_set": [1, 2]},
        meta={"notes": "This is an expectation."},
    )


@pytest.fixture
def exp7() -> ExpectationConfiguration:
    return ExpectationConfiguration(
        type="expect_column_values_to_be_in_set",
        kwargs={"column": "b", "value_set": [1, 2, 3, 4]},
        meta={"notes": "This is an expectation."},
    )


@pytest.fixture
def exp8() -> ExpectationConfiguration:
    return ExpectationConfiguration(
        type="expect_column_values_to_be_in_set",
        kwargs={"column": "b", "value_set": [1, 2, 3]},
        meta={"notes": "This is an expectation."},
    )


@pytest.fixture
def table_exp1() -> ExpectationConfiguration:
    return ExpectationConfiguration(
        type="expect_table_columns_to_match_ordered_list",
        kwargs={"column_list": ["a", "b", "c"]},
    )


@pytest.fixture
def table_exp2() -> ExpectationConfiguration:
    return ExpectationConfiguration(
        type="expect_table_row_count_to_be_between",
        kwargs={"min_value": 0, "max_value": 1},
    )


@pytest.fixture
def table_exp3() -> ExpectationConfiguration:
    return ExpectationConfiguration(type="expect_table_row_count_to_equal", kwargs={"value": 1})


@pytest.fixture
def column_pair_expectation() -> ExpectationConfiguration:
    return ExpectationConfiguration(
        type="expect_column_pair_values_to_be_in_set",
        kwargs={
            "column_A": "1",
            "column_B": "b",
            "value_pairs_set": [(1, 1), (2, 2)],
            "result_format": "BASIC",
        },
    )


@pytest.fixture
def single_expectation_suite(exp1) -> ExpectationSuite:
    return ExpectationSuite(
        name="warning",
        expectations=[exp1],
        meta={"notes": "This is an expectation suite."},
    )


@pytest.fixture
def single_expectation_suite_with_expectation_ge_cloud_id(exp1) -> ExpectationSuite:
    exp1_with_ge_cloud_id = deepcopy(exp1)
    exp1_with_ge_cloud_id.id = "0faf94a9-f53a-41fb-8e94-32f218d4a774"

    return ExpectationSuite(
        name="warning",
        expectations=[exp1_with_ge_cloud_id],
        meta={"notes": "This is an expectation suite."},
    )


@pytest.fixture
def different_suite(exp1, exp4) -> ExpectationSuite:
    return ExpectationSuite(
        name="warning",
        expectations=[exp1, exp4],
        meta={"notes": "This is an expectation suite."},
    )


@pytest.fixture
def domain_success_runtime_suite(exp1, exp2, exp3, exp4, exp5) -> ExpectationSuite:
    return ExpectationSuite(
        name="warning",
        expectations=[exp1, exp2, exp3, exp4, exp5],
        meta={"notes": "This is an expectation suite."},
    )


@pytest.fixture
def suite_with_table_and_column_expectations(
    exp1,
    exp2,
    exp3,
    exp4,
    column_pair_expectation,
    table_exp1,
    table_exp2,
    table_exp3,
) -> ExpectationSuite:
    suite = ExpectationSuite(
        name="warning",
        expectations=[
            exp1,
            exp2,
            exp3,
            exp4,
            column_pair_expectation,
            table_exp1,
            table_exp2,
            table_exp3,
        ],
        meta={"notes": "This is an expectation suite."},
    )
    return suite


@pytest.fixture
def suite_with_column_pair_and_table_expectations(
    table_exp1,
    table_exp2,
    table_exp3,
    column_pair_expectation,
) -> ExpectationSuite:
    suite = ExpectationSuite(
        name="warning",
        expectations=[
            column_pair_expectation,
            table_exp1,
            table_exp2,
            table_exp3,
        ],
        meta={"notes": "This is an expectation suite."},
    )
    assert suite.expectation_configurations == [
        column_pair_expectation,
        table_exp1,
        table_exp2,
        table_exp3,
    ]
    return suite


@pytest.fixture
def ge_cloud_suite(ge_cloud_id, exp1, exp2, exp3) -> ExpectationSuite:
    for exp in (exp1, exp2, exp3):
        exp.id = ge_cloud_id
    return ExpectationSuite(
        name="warning",
        expectations=[exp1, exp2, exp3],
        meta={"notes": "This is an expectation suite."},
        id=ge_cloud_id,
    )


@pytest.mark.filesystem
def test_remove_expectation(
    exp1, exp2, exp3, exp4, exp5, single_expectation_suite, domain_success_runtime_suite
):
    domain_success_runtime_suite.remove_expectation(
        exp5, match_type="runtime", remove_multiple_matches=False
    )  # remove one matching expectation

    with pytest.raises(ValueError):
        domain_success_runtime_suite.remove_expectation(exp5, match_type="runtime")

    assert len(domain_success_runtime_suite.expectations) == 4

    with pytest.raises(ValueError):
        domain_success_runtime_suite.remove_expectation(
            exp4, match_type="domain", remove_multiple_matches=False
        )

    # remove 3 matching expectations
    domain_success_runtime_suite.remove_expectation(
        exp4, match_type="domain", remove_multiple_matches=True
    )

    with pytest.raises(ValueError):
        domain_success_runtime_suite.remove_expectation(exp2, match_type="runtime")
    with pytest.raises(ValueError):
        domain_success_runtime_suite.remove_expectation(exp3, match_type="runtime")

    assert len(domain_success_runtime_suite.expectations) == 1
    assert domain_success_runtime_suite == single_expectation_suite


@pytest.mark.filesystem
def test_remove_expectation_without_necessary_args(single_expectation_suite):
    with pytest.raises(TypeError) as err:
        single_expectation_suite.remove_expectation(expectation_configuration=None, id=None)
    assert str(err.value) == "Must provide either expectation_configuration or id"


@pytest.mark.filesystem
def test_add_expectation_configurations(
    exp1,
    exp2,
    exp3,
    exp4,
    exp5,
    single_expectation_suite,
    different_suite,
):
    expectation_configurations = [exp1, exp2, exp3, exp4, exp5]
    assert len(single_expectation_suite.expectations) == 1
    assert single_expectation_suite != different_suite
    result = single_expectation_suite.add_expectation_configurations(
        expectation_configurations=expectation_configurations,
        match_type="domain",
        overwrite_existing=True,
    )
    assert len(result) == 5

    # Collisions/overrites due to same "match_type" value
    assert len(single_expectation_suite.expectations) == 2

    # Should raise if overwrite_existing=False and a matching expectation is found
    with pytest.raises(DataContextError):
        # noinspection PyUnusedLocal
        result = single_expectation_suite.add_expectation_configurations(
            expectation_configurations=expectation_configurations,
            match_type="domain",
            overwrite_existing=False,
        )

    assert len(single_expectation_suite.expectations) == len(different_suite.expectations)
    assert all(
        mine.isEquivalentTo(theirs)
        for (mine, theirs) in zip(
            single_expectation_suite.expectation_configurations,
            different_suite.expectation_configurations,
        )
    )


@pytest.mark.filesystem
def test_add_expectation(
    exp2,
    exp4,
    single_expectation_suite,
    baseline_suite,
    different_suite,
    domain_success_runtime_suite,
):
    assert len(single_expectation_suite.expectations) == 1
    assert single_expectation_suite != baseline_suite
    single_expectation_suite.add_expectation_configuration(
        exp2, match_type="runtime", overwrite_existing=False
    )
    assert single_expectation_suite == baseline_suite
    assert len(single_expectation_suite.expectations) == 2

    # Should raise if overwrite_existing=False and a matching expectation is found
    with pytest.raises(DataContextError):
        single_expectation_suite.add_expectation_configuration(
            exp4, match_type="domain", overwrite_existing=False
        )

    assert single_expectation_suite != different_suite
    single_expectation_suite.add_expectation_configuration(
        exp4, match_type="domain", overwrite_existing=True
    )
    assert single_expectation_suite == different_suite
    assert len(single_expectation_suite.expectations) == 2

    # Should raise if more than one matching expectation is found
    with pytest.raises(ValueError):
        domain_success_runtime_suite.add_expectation_configuration(
            exp2, match_type="success", overwrite_existing=False
        )

    config = ExpectationConfiguration(type="not an expectation", kwargs={})
    with pytest.raises(InvalidExpectationConfigurationError):
        single_expectation_suite.add_expectation_configuration(config)


@pytest.mark.cloud
def test_add_expectation_with_ge_cloud_id(
    single_expectation_suite_with_expectation_ge_cloud_id,
):
    """
    This test ensures that expectation does not lose ge_cloud_id attribute when updated
    """
    expectation_ge_cloud_id = (
        single_expectation_suite_with_expectation_ge_cloud_id.expectation_configurations[0].id
    )
    # updated expectation does not have ge_cloud_id
    updated_expectation = ExpectationConfiguration(
        type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "a",
            "value_set": [11, 22, 33, 44, 55],
            "result_format": "BASIC",
        },
        meta={"notes": "This is an expectation."},
    )
    single_expectation_suite_with_expectation_ge_cloud_id.add_expectation_configuration(
        updated_expectation, overwrite_existing=True
    )
    assert (
        single_expectation_suite_with_expectation_ge_cloud_id.expectation_configurations[0].id
        == expectation_ge_cloud_id
    )
    # make sure expectation config was actually updated
    assert single_expectation_suite_with_expectation_ge_cloud_id.expectation_configurations[
        0
    ].kwargs["value_set"] == [
        11,
        22,
        33,
        44,
        55,
    ]
