import pytest

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.render.renderer.renderer import Renderer


@pytest.mark.unit
def test_render():
    # noinspection PyUnusedLocal
    with pytest.raises(NotImplementedError):
        Renderer().render(**{})

    # noinspection PyUnusedLocal
    with pytest.raises(TypeError):
        # noinspection PyArgumentList
        Renderer().render({})

    # noinspection PyUnusedLocal
    with pytest.raises(TypeError):
        # noinspection PyArgumentList
        Renderer().render("wowza")


# TODO: Implement this test thoughtfully
# def test__id_from_configuration():
#     Renderer()._id_from_configuration(expectation_type, expectation_kwargs, data_asset_name=None)

# TODO: Implement this test thoughtfully
# def test__get_expectation_type():
#     Renderer()._get_expectation_type(ge_object)

# TODO: Implement this test thoughtfully
# def test__find_ge_object_type():
#     Renderer()._find_ge_object_type(ge_object)


def test__find_evr_by_type(titanic_profiled_evrs_1):
    # TODO: _find_all_evrs_by_type should accept an ValidationResultSuite, not ValidationResultSuite.results
    found_evr = Renderer()._find_evr_by_type(
        titanic_profiled_evrs_1.results, "expect_column_to_exist"
    )
    assert found_evr is None

    # TODO: _find_all_evrs_by_type should accept an ValidationResultSuite, not ValidationResultSuite.results
    found_evr = Renderer()._find_evr_by_type(
        titanic_profiled_evrs_1.results, "expect_column_distinct_values_to_be_in_set"
    )
    assert found_evr == ExpectationValidationResult(
        success=True,
        result={
            "observed_value": ["*", "1st", "2nd", "3rd"],
            "element_count": 1313,
            "missing_count": 0,
            "missing_percent": 0.0,
            "details": {
                "value_counts": [
                    {"value": "*", "count": 1},
                    {"value": "1st", "count": 322},
                    {"value": "2nd", "count": 279},
                    {"value": "3rd", "count": 711},
                ]
            },
        },
        exception_info={
            "raised_exception": False,
            "exception_message": None,
            "exception_traceback": None,
        },
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_column_distinct_values_to_be_in_set",
            kwargs={"column": "PClass", "value_set": None, "result_format": "SUMMARY"},
        ),
    )


def test__find_all_evrs_by_type(titanic_profiled_evrs_1):
    # TODO: _find_all_evrs_by_type should accept an ValidationResultSuite, not ValidationResultSuite.results
    found_evrs = Renderer()._find_all_evrs_by_type(
        titanic_profiled_evrs_1.results, "expect_column_to_exist", column_=None
    )
    assert found_evrs == []

    # TODO: _find_all_evrs_by_type should accept an ValidationResultSuite, not ValidationResultSuite.results
    found_evrs = Renderer()._find_all_evrs_by_type(
        titanic_profiled_evrs_1.results, "expect_column_to_exist", column_="SexCode"
    )
    assert found_evrs == []

    # TODO: _find_all_evrs_by_type should accept an ValidationResultSuite, not ValidationResultSuite.results
    found_evrs = Renderer()._find_all_evrs_by_type(
        titanic_profiled_evrs_1.results,
        "expect_column_distinct_values_to_be_in_set",
        column_=None,
    )
    assert len(found_evrs) == 4

    # TODO: _find_all_evrs_by_type should accept an ValidationResultSuite, not ValidationResultSuite.results
    found_evrs = Renderer()._find_all_evrs_by_type(
        titanic_profiled_evrs_1.results,
        "expect_column_distinct_values_to_be_in_set",
        column_="SexCode",
    )
    assert len(found_evrs) == 1


def test__get_column_list_from_evrs(titanic_profiled_evrs_1):
    column_list = Renderer()._get_column_list_from_evrs(titanic_profiled_evrs_1)
    assert column_list == [
        "Unnamed: 0",
        "Name",
        "PClass",
        "Age",
        "Sex",
        "Survived",
        "SexCode",
    ]
