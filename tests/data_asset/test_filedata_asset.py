import warnings

import pytest

import great_expectations as ge
from great_expectations.core import ExpectationConfiguration
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.data_context.util import file_relative_path


def test_autoinspect_filedata_asset():
    # Expect an error to be raised since a file object doesn't have a columns attribute
    warnings.simplefilter("always", UserWarning)
    file_path = file_relative_path(__file__, "../test_sets/toy_data_complete.csv")
    my_file_data = ge.data_asset.FileDataAsset(file_path)

    with pytest.raises(ge.exceptions.GreatExpectationsError) as exc:
        my_file_data.profile(ge.profile.ColumnsExistProfiler)
    assert "Invalid data_asset for profiler; aborting" in exc.value.message

    # with warnings.catch_warnings(record=True):
    #     warnings.simplefilter("error")
    #     try:
    #         my_file_data.profile(ge.profile.ColumnsExistProfiler)
    #     except:
    #         raise


def test_expectation_suite_filedata_asset():
    # Load in data files
    file_path = file_relative_path(__file__, "../test_sets/toy_data_complete.csv")

    # Create FileDataAsset objects
    f_dat = ge.data_asset.FileDataAsset(file_path)

    # Set up expectations
    f_dat.expect_file_line_regex_match_count_to_equal(
        regex=r",\S",
        expected_count=3,
        skip=1,
        result_format="BASIC",
        catch_exceptions=True,
    )

    f_dat.expect_file_line_regex_match_count_to_be_between(
        regex=r",\S",
        expected_max_count=2,
        skip=1,
        result_format="SUMMARY",
        include_config=True,
    )

    # Test basic config output
    complete_config = f_dat.get_expectation_suite()
    assert [
        ExpectationConfiguration(
            expectation_type="expect_file_line_regex_match_count_to_equal",
            kwargs={"expected_count": 3, "regex": ",\\S", "skip": 1},
        )
    ] == complete_config.expectations

    # Include result format kwargs
    complete_config2 = f_dat.get_expectation_suite(
        discard_result_format_kwargs=False, discard_failed_expectations=False
    )
    assert [
        ExpectationConfiguration(
            expectation_type="expect_file_line_regex_match_count_to_equal",
            kwargs={
                "expected_count": 3,
                "regex": ",\\S",
                "result_format": "BASIC",
                "skip": 1,
            },
        ),
        ExpectationConfiguration(
            expectation_type="expect_file_line_regex_match_count_to_be_between",
            kwargs={
                "expected_max_count": 2,
                "regex": ",\\S",
                "result_format": "SUMMARY",
                "skip": 1,
            },
        ),
    ] == complete_config2.expectations

    # Discard Failing Expectations
    complete_config3 = f_dat.get_expectation_suite(
        discard_result_format_kwargs=False, discard_failed_expectations=True
    )

    assert [
        ExpectationConfiguration(
            expectation_type="expect_file_line_regex_match_count_to_equal",
            kwargs={
                "expected_count": 3,
                "regex": ",\\S",
                "result_format": "BASIC",
                "skip": 1,
            },
        )
    ] == complete_config3.expectations


def test_file_format_map_output():
    incomplete_file_path = file_relative_path(
        __file__, "../test_sets/toy_data_incomplete.csv"
    )
    incomplete_file_dat = ge.data_asset.FileDataAsset(incomplete_file_path)
    null_file_path = file_relative_path(__file__, "../test_sets/null_file.csv")
    null_file_dat = ge.data_asset.FileDataAsset(null_file_path)
    white_space_path = file_relative_path(__file__, "../test_sets/white_space.txt")
    white_space_dat = ge.data_asset.FileDataAsset(white_space_path)

    # Boolean Expectation Output
    expectation = incomplete_file_dat.expect_file_line_regex_match_count_to_equal(
        regex=r",\S",
        expected_count=3,
        skip=1,
        result_format="BOOLEAN_ONLY",
        include_config=False,
    )
    expected_result = ExpectationValidationResult(success=False)
    assert expected_result == expectation

    # Empty File Expectations
    expectation = null_file_dat.expect_file_line_regex_match_count_to_equal(
        regex=r",\S",
        expected_count=3,
        skip=1,
        result_format="BASIC",
        include_config=False,
    )
    expected_result = ExpectationValidationResult(
        success=None,
        result={
            "element_count": 0,
            "missing_count": 0,
            "missing_percent": None,
            "unexpected_count": 0,
            "unexpected_percent": None,
            "unexpected_percent_nonmissing": None,
            "partial_unexpected_list": [],
        },
    )

    assert expected_result == expectation

    # White Space File
    expectation = white_space_dat.expect_file_line_regex_match_count_to_equal(
        regex=r",\S", expected_count=3, result_format="BASIC", include_config=False
    )
    expected_result = ExpectationValidationResult(
        success=None,
        result={
            "element_count": 11,
            "missing_count": 11,
            "missing_percent": 100.0,
            "unexpected_count": 0,
            "unexpected_percent": 0,
            "unexpected_percent_nonmissing": None,
            "partial_unexpected_list": [],
        },
    )

    assert expected_result == expectation

    # Complete Result Format
    expectation = incomplete_file_dat.expect_file_line_regex_match_count_to_equal(
        regex=r",\S",
        expected_count=3,
        skip=1,
        result_format="COMPLETE",
        include_config=False,
    )

    expected_result = ExpectationValidationResult(
        success=False,
        result={
            "element_count": 9,
            "missing_count": 2,
            "missing_percent": (2 / 9 * 100),
            "unexpected_count": 3,
            "unexpected_percent": (3 / 9 * 100),
            "unexpected_percent_nonmissing": (3 / 7 * 100),
            "partial_unexpected_list": ["A,C,1\n", "B,1,4\n", "A,1,4\n"],
            "partial_unexpected_counts": [
                {"value": "A,1,4\n", "count": 1},
                {"value": "A,C,1\n", "count": 1},
                {"value": "B,1,4\n", "count": 1},
            ],
            "partial_unexpected_index_list": [0, 3, 5],
            "unexpected_list": ["A,C,1\n", "B,1,4\n", "A,1,4\n"],
            "unexpected_index_list": [0, 3, 5],
        },
    )

    assert expected_result == expectation

    # Invalid Result Format
    with pytest.raises(ValueError):
        expectation = incomplete_file_dat.expect_file_line_regex_match_count_to_equal(
            regex=r",\S",
            expected_count=3,
            skip=1,
            result_format="JOKE",
            include_config=False,
        )
