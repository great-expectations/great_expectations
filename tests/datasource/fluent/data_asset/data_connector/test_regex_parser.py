from __future__ import annotations

import logging
import pathlib
import re

import pytest

from great_expectations.datasource.fluent.data_asset.data_connector.regex_parser import (
    RegExParser,
)

logger = logging.getLogger(__file__)


@pytest.fixture
def csv_path() -> pathlib.Path:
    relative_path = pathlib.Path(
        "..",
        "..",
        "..",
        "..",
        "test_sets",
        "taxi_yellow_tripdata_samples",
        "yellow_tripdata_sample_2020-03.csv",
    )
    abs_csv_path = (
        pathlib.Path(__file__).parent.joinpath(relative_path).resolve(strict=True)
    )
    return abs_csv_path


@pytest.fixture
def regex_pattern_two_named_groups() -> re.Pattern:
    return re.compile(r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv")


@pytest.fixture
def regex_pattern_first_named_group_second_common_group() -> re.Pattern:
    return re.compile(r"yellow_tripdata_sample_(?P<year>\d{4})-(\d{2})\.csv")


@pytest.fixture
def regex_pattern_first_common_group_second_named_group() -> re.Pattern:
    return re.compile(r"yellow_tripdata_sample_(\d{4})-(?P<month>\d{2})\.csv")


@pytest.fixture
def regex_pattern_two_common_groups() -> re.Pattern:
    return re.compile(r"yellow_tripdata_sample_(\d{4})-(\d{2})\.csv")


@pytest.fixture
def regex_pattern_no_groups() -> re.Pattern:
    return re.compile(r"yellow_tripdata_sample_\d{4}-\d{2}\.csv")


@pytest.mark.unit
def test_regex_pattern_two_named_groups(
    regex_pattern_two_named_groups: re.Pattern, csv_path: pathlib.Path
):
    regex_pattern: re.Pattern = regex_pattern_two_named_groups
    target: str = csv_path.name
    regex_parser = RegExParser(
        regex_pattern=regex_pattern, unnamed_regex_group_prefix="batch_request_param_"
    )

    assert regex_parser.get_num_all_matched_group_values() == 2
    assert regex_parser.get_num_named_matched_group_values() == 2
    assert regex_parser.get_named_group_name_to_group_index_mapping() == {
        "year": 1,
        "month": 2,
    }
    assert regex_parser.get_named_group_name_to_group_value_mapping(target=target) == {
        "year": "2020",
        "month": "03",
    }
    assert regex_parser.get_all_matched_group_values(target=target) == ["2020", "03"]
    assert (
        regex_parser.get_all_group_names_to_group_indexes_bidirectional_mappings()
        == ({"year": 1, "month": 2}, {1: "year", 2: "month"})
    )
    assert regex_parser.get_all_group_name_to_group_index_mapping() == {
        "year": 1,
        "month": 2,
    }
    assert regex_parser.get_all_group_index_to_group_name_mapping() == {
        1: "year",
        2: "month",
    }
    assert regex_parser.get_all_group_names() == ["year", "month"]
    assert regex_parser.get_all_group_indexes() == [1, 2]
    assert regex_parser.get_group_name_to_group_value_mapping(target=target) == {
        "year": "2020",
        "month": "03",
    }
    assert regex_parser.get_group_index_to_group_value_mapping(target=target) == {
        1: "2020",
        2: "03",
    }


@pytest.mark.unit
def test_regex_pattern_first_named_group_second_common_group(
    regex_pattern_first_named_group_second_common_group: re.Pattern,
    csv_path: pathlib.Path,
):
    regex_pattern: re.Pattern = regex_pattern_first_named_group_second_common_group
    target: str = csv_path.name
    regex_parser = RegExParser(
        regex_pattern=regex_pattern, unnamed_regex_group_prefix="batch_request_param_"
    )

    assert regex_parser.get_num_all_matched_group_values() == 2
    assert regex_parser.get_num_named_matched_group_values() == 1
    assert regex_parser.get_named_group_name_to_group_index_mapping() == {"year": 1}
    assert regex_parser.get_named_group_name_to_group_value_mapping(target=target) == {
        "year": "2020"
    }
    assert regex_parser.get_all_matched_group_values(target=target) == ["2020", "03"]
    assert (
        regex_parser.get_all_group_names_to_group_indexes_bidirectional_mappings()
        == (
            {"year": 1, "batch_request_param_2": 2},
            {1: "year", 2: "batch_request_param_2"},
        )
    )
    assert regex_parser.get_all_group_name_to_group_index_mapping() == {
        "year": 1,
        "batch_request_param_2": 2,
    }
    assert regex_parser.get_all_group_index_to_group_name_mapping() == {
        1: "year",
        2: "batch_request_param_2",
    }
    assert regex_parser.get_all_group_names() == ["year", "batch_request_param_2"]
    assert regex_parser.get_all_group_indexes() == [1, 2]
    assert regex_parser.get_group_name_to_group_value_mapping(target=target) == {
        "year": "2020",
        "batch_request_param_2": "03",
    }
    assert regex_parser.get_group_index_to_group_value_mapping(target=target) == {
        1: "2020",
        2: "03",
    }


@pytest.mark.unit
def test_regex_pattern_first_common_group_second_named_group(
    regex_pattern_first_common_group_second_named_group: re.Pattern,
    csv_path: pathlib.Path,
):
    regex_pattern: re.Pattern = regex_pattern_first_common_group_second_named_group
    target: str = csv_path.name
    regex_parser = RegExParser(
        regex_pattern=regex_pattern, unnamed_regex_group_prefix="batch_request_param_"
    )

    assert regex_parser.get_num_all_matched_group_values() == 2
    assert regex_parser.get_num_named_matched_group_values() == 1
    assert regex_parser.get_named_group_name_to_group_index_mapping() == {"month": 2}
    assert regex_parser.get_named_group_name_to_group_value_mapping(target=target) == {
        "month": "03"
    }
    assert regex_parser.get_all_matched_group_values(target=target) == ["2020", "03"]
    assert (
        regex_parser.get_all_group_names_to_group_indexes_bidirectional_mappings()
        == (
            {"batch_request_param_1": 1, "month": 2},
            {1: "batch_request_param_1", 2: "month"},
        )
    )
    assert regex_parser.get_all_group_name_to_group_index_mapping() == {
        "batch_request_param_1": 1,
        "month": 2,
    }
    assert regex_parser.get_all_group_index_to_group_name_mapping() == {
        1: "batch_request_param_1",
        2: "month",
    }
    assert regex_parser.get_all_group_names() == ["batch_request_param_1", "month"]
    assert regex_parser.get_all_group_indexes() == [1, 2]
    assert regex_parser.get_group_name_to_group_value_mapping(target=target) == {
        "batch_request_param_1": "2020",
        "month": "03",
    }
    assert regex_parser.get_group_index_to_group_value_mapping(target=target) == {
        1: "2020",
        2: "03",
    }


@pytest.mark.unit
def test_regex_pattern_two_common_groups(
    regex_pattern_two_common_groups: re.Pattern, csv_path: pathlib.Path
):
    regex_pattern: re.Pattern = regex_pattern_two_common_groups
    target: str = csv_path.name
    regex_parser = RegExParser(
        regex_pattern=regex_pattern, unnamed_regex_group_prefix="batch_request_param_"
    )

    assert regex_parser.get_num_all_matched_group_values() == 2
    assert regex_parser.get_num_named_matched_group_values() == 0
    assert regex_parser.get_named_group_name_to_group_index_mapping() == {}
    assert regex_parser.get_named_group_name_to_group_value_mapping(target=target) == {}
    assert regex_parser.get_all_matched_group_values(target=target) == ["2020", "03"]
    assert (
        regex_parser.get_all_group_names_to_group_indexes_bidirectional_mappings()
        == (
            {"batch_request_param_1": 1, "batch_request_param_2": 2},
            {1: "batch_request_param_1", 2: "batch_request_param_2"},
        )
    )
    assert regex_parser.get_all_group_name_to_group_index_mapping() == {
        "batch_request_param_1": 1,
        "batch_request_param_2": 2,
    }
    assert regex_parser.get_all_group_index_to_group_name_mapping() == {
        1: "batch_request_param_1",
        2: "batch_request_param_2",
    }
    assert regex_parser.get_all_group_names() == [
        "batch_request_param_1",
        "batch_request_param_2",
    ]
    assert regex_parser.get_all_group_indexes() == [1, 2]
    assert regex_parser.get_group_name_to_group_value_mapping(target=target) == {
        "batch_request_param_1": "2020",
        "batch_request_param_2": "03",
    }
    assert regex_parser.get_group_index_to_group_value_mapping(target=target) == {
        1: "2020",
        2: "03",
    }


@pytest.mark.unit
def test_regex_pattern_no_groups(
    regex_pattern_no_groups: re.Pattern, csv_path: pathlib.Path
):
    regex_pattern: re.Pattern = regex_pattern_no_groups
    target: str = csv_path.name
    regex_parser = RegExParser(
        regex_pattern=regex_pattern, unnamed_regex_group_prefix="batch_request_param_"
    )

    assert regex_parser.get_num_all_matched_group_values() == 0
    assert regex_parser.get_num_named_matched_group_values() == 0
    assert regex_parser.get_named_group_name_to_group_index_mapping() == {}
    assert regex_parser.get_named_group_name_to_group_value_mapping(target=target) == {}
    assert regex_parser.get_all_matched_group_values(target=target) == []
    assert (
        regex_parser.get_all_group_names_to_group_indexes_bidirectional_mappings()
        == ({}, {})
    )
    assert regex_parser.get_all_group_name_to_group_index_mapping() == {}
    assert regex_parser.get_all_group_index_to_group_name_mapping() == {}
    assert regex_parser.get_all_group_names() == []
    assert regex_parser.get_all_group_indexes() == []
    assert regex_parser.get_group_name_to_group_value_mapping(target=target) == {}
    assert regex_parser.get_group_index_to_group_value_mapping(target=target) == {}
