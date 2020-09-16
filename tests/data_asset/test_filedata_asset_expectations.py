# Test File Expectations
import os
import platform

import pytest

import great_expectations as ge
from great_expectations.data_context.util import file_relative_path


def test_expect_file_line_regex_match_count_to_be_between():
    #####Invlaid File Path######
    joke_file_path = "joke.txt"
    assert not os.path.isfile(joke_file_path)
    joke_dat = ge.data_asset.FileDataAsset(joke_file_path)

    with pytest.raises(IOError):
        joke_dat.expect_file_line_regex_match_count_to_be_between(
            regex=r",\S", expected_min_count=0, expected_max_count=4, skip=1
        )

    complete_file_path = file_relative_path(
        __file__, "../test_sets/toy_data_complete.csv"
    )
    file_dat = ge.data_asset.FileDataAsset(complete_file_path)

    # Invalid Skip Parameter
    with pytest.raises(ValueError):
        file_dat.expect_file_line_regex_match_count_to_be_between(
            regex=r",\S", expected_min_count=0, expected_max_count=4, skip=2.4
        )

    # Invalid Regex
    with pytest.raises(ValueError):
        file_dat.expect_file_line_regex_match_count_to_be_between(
            regex=2, expected_min_count=1, expected_max_count=8, skip=2
        )

    # Non-integer min value
    with pytest.raises(ValueError):
        file_dat.expect_file_line_regex_match_count_to_be_between(
            regex=r",\S", expected_min_count=1.3, expected_max_count=8, skip=1
        )

    # Negative min value
    with pytest.raises(ValueError):
        file_dat.expect_file_line_regex_match_count_to_be_between(
            regex=r",\S", expected_min_count=-2, expected_max_count=8, skip=1
        )

    # Non-integer max value
    with pytest.raises(ValueError):
        file_dat.expect_file_line_regex_match_count_to_be_between(
            regex=r",\S", expected_min_count=0, expected_max_count="foo", skip=1
        )

    # Negative max value
    with pytest.raises(ValueError):
        file_dat.expect_file_line_regex_match_count_to_be_between(
            regex=r",\S", expected_min_count=0, expected_max_count=-1, skip=1
        )

    # Min count more than max count
    with pytest.raises(ValueError):
        file_dat.expect_file_line_regex_match_count_to_be_between(
            regex=r",\S", expected_min_count=4, expected_max_count=3, skip=1
        )

    # Count does not fall in range
    fail_trial = file_dat.expect_file_line_regex_match_count_to_be_between(
        regex=r",\S", expected_min_count=9, expected_max_count=12, skip=1
    )

    assert not fail_trial.success
    assert fail_trial.result["unexpected_percent"] == 100
    assert fail_trial.result["missing_percent"] == 0

    # Count does fall in range
    success_trial = file_dat.expect_file_line_regex_match_count_to_be_between(
        regex=r",\S", expected_min_count=0, expected_max_count=4, skip=1
    )

    assert success_trial.success
    assert success_trial.result["unexpected_percent"] == 0
    assert success_trial.result["missing_percent"] == 0


def test_expect_file_line_regex_match_count_to_equal():
    complete_file_path = file_relative_path(
        __file__, "../test_sets/toy_data_complete.csv"
    )
    incomplete_file_path = file_relative_path(
        __file__, "../test_sets/toy_data_incomplete.csv"
    )
    file_dat = ge.data_asset.FileDataAsset(complete_file_path)
    file_incomplete_dat = ge.data_asset.FileDataAsset(incomplete_file_path)

    # Invalid Regex Value
    with pytest.raises(ValueError):
        file_dat.expect_file_line_regex_match_count_to_equal(
            regex=True, expected_count=3, skip=1
        )

    # Non-integer expected_count
    with pytest.raises(ValueError):
        file_dat.expect_file_line_regex_match_count_to_equal(
            regex=r",\S", expected_count=6.3, skip=1
        )

    # Negative expected_count
    with pytest.raises(ValueError):
        file_dat.expect_file_line_regex_match_count_to_equal(
            regex=r",\S", expected_count=-6, skip=1
        )

    # Count does not equal expected count
    fail_trial = file_incomplete_dat.expect_file_line_regex_match_count_to_equal(
        regex=r",\S", expected_count=3, skip=1
    )

    assert not fail_trial.success
    assert fail_trial.result["unexpected_percent"] == (3 / 9 * 100)
    assert fail_trial.result["missing_percent"] == (2 / 9 * 100)
    assert fail_trial.result["unexpected_percent_nonmissing"] == (3 / 7 * 100)

    # Mostly success
    mostly_trial = file_incomplete_dat.expect_file_line_regex_match_count_to_equal(
        regex=r",\S", expected_count=3, skip=1, mostly=0.57
    )

    assert mostly_trial.success

    # Count does fall in range
    success_trial = file_dat.expect_file_line_regex_match_count_to_equal(
        regex=r",\S", expected_count=3, skip=1
    )

    assert success_trial.success
    assert success_trial.result["unexpected_percent"] == 0
    assert success_trial.result["unexpected_percent_nonmissing"] == 0
    assert success_trial.result["missing_percent"] == 0


def test_expect_file_hash_to_equal():
    # Test for non-existent file
    fake_file = ge.data_asset.FileDataAsset(file_path="abc")

    with pytest.raises(IOError):
        fake_file.expect_file_hash_to_equal(value="abc")

    # Test for non-existent hash algorithm
    titanic_path = file_relative_path(__file__, "../test_sets/Titanic.csv")
    titanic_file = ge.data_asset.FileDataAsset(titanic_path)

    with pytest.raises(ValueError):
        titanic_file.expect_file_hash_to_equal(hash_alg="md51", value="abc")

    # Test non-matching hash value
    fake_hash_value = titanic_file.expect_file_hash_to_equal(value="abc")
    assert not fake_hash_value.success

    # Test matching hash value with default algorithm
    hash_value = "63188432302f3a6e8c9e9c500ff27c8a"
    good_hash_default_alg = titanic_file.expect_file_hash_to_equal(value=hash_value)
    assert good_hash_default_alg.success

    # Test matching hash value with specified algorithm
    hash_value = "f89f46423b017a1fc6a4059d81bddb3ff64891e3c81250fafad6f3b3113ecc9b"
    good_hash_new_alg = titanic_file.expect_file_hash_to_equal(
        value=hash_value, hash_alg="sha256"
    )
    assert good_hash_new_alg.success


def test_expect_file_size_to_be_between():
    fake_file = ge.data_asset.FileDataAsset("abc")

    # Test for non-existent file
    with pytest.raises(OSError):
        fake_file.expect_file_size_to_be_between(0, 10000)

    titanic_path = file_relative_path(__file__, "../test_sets/Titanic.csv")
    titanic_file = ge.data_asset.FileDataAsset(titanic_path)

    # Test minsize not an integer
    with pytest.raises(ValueError):
        titanic_file.expect_file_size_to_be_between("a", 10000)

    # Test maxsize not an integer
    with pytest.raises(ValueError):
        titanic_file.expect_file_size_to_be_between(0, "10000a")

    # Test minsize less than 0
    with pytest.raises(ValueError):
        titanic_file.expect_file_size_to_be_between(-1, 10000)

    # Test maxsize less than 0
    with pytest.raises(ValueError):
        titanic_file.expect_file_size_to_be_between(0, -1)

    # Test minsize > maxsize
    with pytest.raises(ValueError):
        titanic_file.expect_file_size_to_be_between(10000, 0)

    # Test file size not in range
    bad_range = titanic_file.expect_file_size_to_be_between(0, 10000)
    assert not bad_range.success

    # Test file size in range
    lower, upper = (70000, 71000) if platform.system() != "Windows" else (71000, 72000)
    good_range = titanic_file.expect_file_size_to_be_between(lower, upper)
    assert good_range.success


def test_expect_file_to_exist():
    # Test for non-existent file
    fake_file = ge.data_asset.FileDataAsset("abc")
    fake_file_existence = fake_file.expect_file_to_exist()
    assert not fake_file_existence.success

    # Test for existing file
    real_file = ge.data_asset.FileDataAsset(
        file_relative_path(__file__, "../test_sets/Titanic.csv")
    )
    real_file_existence = real_file.expect_file_to_exist()
    assert real_file_existence.success


def test_expect_file_to_have_valid_table_header():
    # Test for non-existent file
    fake_file = ge.data_asset.FileDataAsset("abc")
    with pytest.raises(IOError):
        fake_file.expect_file_to_have_valid_table_header(regex="")

    # Test for non-unique column names
    invalid_header_dat = ge.data_asset.FileDataAsset(
        file_relative_path(__file__, "../test_sets/same_column_names.csv")
    )
    invalid_header_dat_expectation = (
        invalid_header_dat.expect_file_to_have_valid_table_header(regex=r"\|", skip=2)
    )
    assert not invalid_header_dat_expectation.success

    # Test for unique column names
    valid_header_dat = ge.data_asset.FileDataAsset(
        file_relative_path(__file__, "../test_sets/Titanic.csv")
    )
    valid_header_dat_expectation = (
        valid_header_dat.expect_file_to_have_valid_table_header(regex=",")
    )
    assert valid_header_dat_expectation.success


def test_expect_file_to_be_valid_json():
    # Test for non-existent file
    fake_file = ge.data_asset.FileDataAsset("abc")
    with pytest.raises(IOError):
        fake_file.expect_file_to_be_valid_json()

    # Test invalid JSON file
    invalid_JSON_file = ge.data_asset.FileDataAsset(
        file_relative_path(__file__, "../test_sets/invalid_json_file.json")
    )
    invalid_JSON_expectation = invalid_JSON_file.expect_file_to_be_valid_json()
    assert not invalid_JSON_expectation.success

    # Test valid JSON file
    valid_JSON_file = ge.data_asset.FileDataAsset(
        file_relative_path(__file__, "../test_sets/titanic_expectations.json")
    )
    valid_JSON_expectation = valid_JSON_file.expect_file_to_be_valid_json()
    assert valid_JSON_expectation.success

    # Test valid JSON file with non-matching schema
    schema_file = file_relative_path(__file__, "../test_sets/sample_schema.json")
    test_file = ge.data_asset.FileDataAsset(
        file_relative_path(__file__, "../test_sets/json_test1_against_schema.json")
    )
    test_file_expectation = test_file.expect_file_to_be_valid_json(schema=schema_file)
    assert not test_file_expectation.success

    # Test valid JSON file with valid schema
    test_file = ge.data_asset.FileDataAsset(
        file_relative_path(__file__, "../test_sets/json_test2_against_schema.json")
    )
    schema_file = file_relative_path(__file__, "../test_sets/sample_schema.json")
    test_file_expectation = test_file.expect_file_to_be_valid_json(schema=schema_file)
    assert test_file_expectation.success
