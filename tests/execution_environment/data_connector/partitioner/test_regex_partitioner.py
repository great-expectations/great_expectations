import pytest

from great_expectations.core.id_dict import PartitionDefinition
from great_expectations.execution_environment.data_connector.partitioner import (
    RegexPartitioner,
    Partition,
)
import great_expectations.exceptions.exceptions as ge_exceptions


def test_regex_partitioner_instantiation():
    partitioner = RegexPartitioner(name="test_regex_partitioner")
    # defaults
    assert partitioner.name == "test_regex_partitioner"
    assert partitioner.sorters is None
    assert not partitioner.allow_multipart_partitions
    assert partitioner.config_params is None
    # without regex configured, you will get a default pattern
    assert partitioner.regex == {"pattern": r"(.*)", "group_names": ["group_0"]}


def test_regex_partitioner_regex_is_not_a_dict():
    config_params = {"regex": "i_am_not_a_dictionary"}

    with pytest.raises(ge_exceptions.PartitionerError):
        # noinspection PyUnusedLocal
        partitioner = RegexPartitioner(
            name="test_regex_partitioner",
            config_params=config_params
        )


def test_regex_partitioner_regex_missing_pattern():
    # missing pattern
    config_params = {"regex": {"not pattern": "not pattern either"}}
    with pytest.raises(ge_exceptions.PartitionerError):
        RegexPartitioner(name="test_regex_partitioner", config_params=config_params)


def test_regex_partitioner_regex_no_groups_named():
    # adding pattern (no groups named)
    config_params = {"regex": {"pattern": r".+\/(.+)_(.+)_(.+)\.csv"}}
    regex_partitioner = RegexPartitioner(
        name="test_regex_partitioner",
        config_params=config_params
    )
    assert regex_partitioner.regex == {"pattern": r".+\/(.+)_(.+)_(.+)\.csv", "group_names": []}


def test_regex_partitioner_regex_groups_named():
    # adding pattern with named groups
    config_params = {"regex": {"pattern": r".+\/(.+)_(.+)_(.+)\.csv", "group_names": ["name", "timestamp", "price"]}}
    regex_partitioner = RegexPartitioner(
        name="test_regex_partitioner",
        config_params=config_params
    )
    assert regex_partitioner.regex == {
        "pattern": r".+\/(.+)_(.+)_(.+)\.csv",
        "group_names": ["name", "timestamp", "price"]
    }


def test_regex_partitioner_find_or_create_partitions_with_no_params():
    config_params = {"regex": {"pattern": r".+\/(.+)_(.+)_(.+)\.csv", "group_names": ["name", "timestamp", "price"]}}
    regex_partitioner = RegexPartitioner(
        name="test_regex_partitioner",
        config_params=config_params
    )
    # No file paths, nothing comes back
    assert regex_partitioner.find_or_create_partitions() == []


def test_regex_partitioner_regex_does_not_match_paths():
    config_params = {"regex": {"pattern": r".+\/(.+)_(.+)_(.+)\.csv", "group_names": ["name", "timestamp", "price"]}}
    regex_partitioner = RegexPartitioner(
        name="test_regex_partitioner",
        config_params=config_params
    )
    paths: list = [
        "my_dir/hi.csv",
        "my_dir/i_wont.csv",
        "my_dir/work.csv",
    ]
    # Nothing configured
    partitions = regex_partitioner.find_or_create_partitions(paths=paths, data_asset_name="test_asset_0")
    assert partitions == []


def test_regex_partitioner_compute_partitions_paths_with_default_regex_config_no_data_asset_name():
    regex_partitioner = RegexPartitioner(name="test_regex_partitioner")
    paths: list = [
        "alex_20200809_1000.csv",
        "eugene_20200810_1500.csv",
        "abe_20200831_1040.csv",
    ]
    # auto_discover_assets is set to True, which means the data_asset_name will come from the filename
    # no sorters configured
    partitions = regex_partitioner.find_or_create_partitions(paths=paths)
    assert partitions == [
        Partition(name="alex_20200809_1000.csv",
                  definition=PartitionDefinition({"group_0": "alex_20200809_1000.csv"}),
                  data_reference="alex_20200809_1000.csv", data_asset_name=None),
        Partition(name="eugene_20200810_1500.csv",
                  definition=PartitionDefinition({"group_0": "eugene_20200810_1500.csv"}),
                  data_reference="eugene_20200810_1500.csv", data_asset_name=None),
        Partition(name="abe_20200831_1040.csv",
                  definition=PartitionDefinition({"group_0": "abe_20200831_1040.csv"}),
                  data_reference="abe_20200831_1040.csv", data_asset_name=None),
    ]


def test_regex_partitioner_compute_partitions_paths_with_default_regex_config_autodiscover_assets():
    regex_partitioner = RegexPartitioner(name="test_regex_partitioner")
    paths: list = [
        "alex_20200809_1000.csv",
        "eugene_20200810_1500.csv",
        "abe_20200831_1040.csv",
    ]
    # auto_discover_assets is set to True, which means the data_asset_name will come from the filename
    # no sorters configured
    partitions = regex_partitioner.find_or_create_partitions(paths=paths, auto_discover_assets=True)
    assert partitions == [
        Partition(name="alex_20200809_1000.csv",
                  definition=PartitionDefinition({"group_0": "alex_20200809_1000.csv"}),
                  data_reference="alex_20200809_1000.csv", data_asset_name="alex_20200809_1000"),
        Partition(name="eugene_20200810_1500.csv",
                  definition=PartitionDefinition({"group_0": "eugene_20200810_1500.csv"}),
                  data_reference="eugene_20200810_1500.csv", data_asset_name="eugene_20200810_1500"),
        Partition(name="abe_20200831_1040.csv",
                  definition=PartitionDefinition({"group_0": "abe_20200831_1040.csv"}),
                  data_reference="abe_20200831_1040.csv", data_asset_name="abe_20200831_1040"),
    ]


def test_regex_partitioner_compute_partitions_paths_with_default_regex_config_data_asset_name_configured():
    regex_partitioner = RegexPartitioner(name="test_regex_partitioner")
    paths: list = [
        "alex_20200809_1000.csv",
        "eugene_20200810_1500.csv",
        "abe_20200831_1040.csv",
    ]
    # auto_discover_assets is set to True, which means the data_asset_name will come from the filename
    # no sorters configured
    partitions = regex_partitioner.find_or_create_partitions(paths=paths, data_asset_name="test_asset_0")
    assert partitions == [
        Partition(name="alex_20200809_1000.csv",
                  definition=PartitionDefinition({"group_0": "alex_20200809_1000.csv"}),
                  data_reference="alex_20200809_1000.csv", data_asset_name="test_asset_0"),
        Partition(name="eugene_20200810_1500.csv",
                  definition=PartitionDefinition({"group_0": "eugene_20200810_1500.csv"}),
                  data_reference="eugene_20200810_1500.csv", data_asset_name="test_asset_0"),
        Partition(name="abe_20200831_1040.csv",
                  definition=PartitionDefinition({"group_0": "abe_20200831_1040.csv"}),
                  data_reference="abe_20200831_1040.csv", data_asset_name="test_asset_0"),
    ]


def test_regex_partitioner_compute_partitions_auto_discover_assets_true():
    config_params = {"regex": {"pattern": r".+\/(.+)_(.+)_(.+)\.csv", "group_names": ["name", "timestamp", "price"]}}
    regex_partitioner = RegexPartitioner(
        name="test_regex_partitioner",
        config_params=config_params
    )
    paths: list = [
        "my_dir/alex_20200809_1000.csv",
        "my_dir/eugene_20200810_1500.csv",
        "my_dir/abe_20200831_1040.csv",
    ]
    # auto_discover_assets is set to True, which means the data_asset_name will come from the filename
    # no sorters configured
    partitions = regex_partitioner.find_or_create_partitions(paths=paths, auto_discover_assets=True)
    assert partitions == [
        Partition(name="alex-20200809-1000",
                  definition=PartitionDefinition({"name": "alex", "timestamp": "20200809", "price": "1000"}),
                  data_reference="my_dir/alex_20200809_1000.csv", data_asset_name="alex_20200809_1000"),
        Partition(name="eugene-20200810-1500",
                  definition=PartitionDefinition({"name": "eugene", "timestamp": "20200810", "price": "1500"}),
                  data_reference="my_dir/eugene_20200810_1500.csv", data_asset_name="eugene_20200810_1500"),
        Partition(name="abe-20200831-1040",
                  definition=PartitionDefinition({"name": "abe", "timestamp": "20200831", "price": "1040"}),
                  data_reference="my_dir/abe_20200831_1040.csv", data_asset_name="abe_20200831_1040"),
    ]


def test_regex_partitioner_compute_partitions_auto_discover_assets_false_no_data_asset_name():
    config_params = {"regex": {"pattern": r".+\/(.+)_(.+)_(.+)\.csv", "group_names": ["name", "timestamp", "price"]}}
    regex_partitioner = RegexPartitioner(
        name="test_regex_partitioner",
        config_params=config_params
    )
    paths: list = [
        "my_dir/alex_20200809_1000.csv",
        "my_dir/eugene_20200810_1500.csv",
        "my_dir/abe_20200831_1040.csv",
    ]
    partitions = regex_partitioner.find_or_create_partitions(paths=paths, auto_discover_assets=False)
    assert partitions == [
        Partition(name="alex-20200809-1000",
                  definition=PartitionDefinition({"name": "alex", "timestamp": "20200809", "price": "1000"}),
                  data_reference="my_dir/alex_20200809_1000.csv", data_asset_name=None),
        Partition(name="eugene-20200810-1500",
                  definition=PartitionDefinition({"name": "eugene", "timestamp": "20200810", "price": "1500"}),
                  data_reference="my_dir/eugene_20200810_1500.csv", data_asset_name=None),
        Partition(name="abe-20200831-1040",
                  definition=PartitionDefinition({"name": "abe", "timestamp": "20200831", "price": "1040"}),
                  data_reference="my_dir/abe_20200831_1040.csv", data_asset_name=None),
    ]


def test_regex_partitioner_compute_partitions_auto_discover_assets_false_data_asset_name_included():
    config_params = {"regex": {"pattern": r".+\/(.+)_(.+)_(.+)\.csv", "group_names": ["name", "timestamp", "price"]}}
    regex_partitioner = RegexPartitioner(
        name="test_regex_partitioner",
        config_params=config_params
    )
    paths: list = [
        "my_dir/alex_20200809_1000.csv",
        "my_dir/eugene_20200810_1500.csv",
        "my_dir/abe_20200831_1040.csv",
    ]
    # auto_discover_assets is set to True, which means the data_asset_name will come from the filename
    # no sorters configured
    partitions = regex_partitioner.find_or_create_partitions(paths=paths, data_asset_name="test_asset_0")
    assert partitions == [
        Partition(name="alex-20200809-1000",
                  definition=PartitionDefinition({"name": "alex", "timestamp": "20200809", "price": "1000"}),
                  data_reference="my_dir/alex_20200809_1000.csv", data_asset_name="test_asset_0"),
        Partition(name="eugene-20200810-1500",
                  definition=PartitionDefinition({"name": "eugene", "timestamp": "20200810", "price": "1500"}),
                  data_reference="my_dir/eugene_20200810_1500.csv", data_asset_name="test_asset_0"),
        Partition(name="abe-20200831-1040",
                  definition=PartitionDefinition({"name": "abe", "timestamp": "20200831", "price": "1040"}),
                  data_reference="my_dir/abe_20200831_1040.csv", data_asset_name="test_asset_0"),
    ]


def test_regex_partitioner_compute_partitions_adding_sorters():
    sorters = [
                {
                    "name": "name",
                    "module_name": "great_expectations.execution_environment.data_connector.partitioner.sorter",
                    "class_name": "LexicographicSorter",
                    "orderby": "asc",
                },
                {
                    "name": "timestamp",
                    "module_name": "great_expectations.execution_environment.data_connector.partitioner.sorter",
                    "class_name": "DateTimeSorter",
                    "orderby": "desc",
                    "config_params": {
                        "datetime_format": "%Y%m%d",
                    }
                },
                {
                    "name": "price",
                    "module_name": "great_expectations.execution_environment.data_connector.partitioner.sorter",
                    "class_name": "NumericSorter",
                    "orderby": "desc",
                },
            ]

    config_params = {"regex": {"pattern": r".+\/(.+)_(.+)_(.+)\.csv", "group_names": ["name", "timestamp", "price"]}}
    regex_partitioner = RegexPartitioner(
        name="test_regex_partitioner",
        sorters=sorters,
        config_params=config_params
    )
    paths: list = [
        "my_dir/alex_20200809_1000.csv",
        "my_dir/eugene_20200810_1500.csv",
        "my_dir/abe_20200831_1040.csv",
    ]

    partitions = regex_partitioner.find_or_create_partitions(paths=paths, data_asset_name="test_asset_0")
    assert partitions == [
        Partition(name="abe-20200831-1040",
                  definition=PartitionDefinition({"name": "abe", "timestamp": "20200831", "price": "1040"}),
                  data_reference="my_dir/abe_20200831_1040.csv", data_asset_name="test_asset_0"),
        Partition(name="alex-20200809-1000",
                  definition=PartitionDefinition({"name": "alex", "timestamp": "20200809", "price": "1000"}),
                  data_reference="my_dir/alex_20200809_1000.csv", data_asset_name="test_asset_0"),
        Partition(name="eugene-20200810-1500",
                  definition=PartitionDefinition({"name": "eugene", "timestamp": "20200810", "price": "1500"}),
                  data_reference="my_dir/eugene_20200810_1500.csv", data_asset_name="test_asset_0"),
    ]


def test_regex_partitioner_compute_partitions_sorters_and_groups_names_do_not_match():
    sorters = [
                {
                    "name": "name",
                    "module_name": "great_expectations.execution_environment.data_connector.partitioner.sorter",
                    "class_name": "LexicographicSorter",
                    "orderby": "asc",
                },
                {
                    "name": "timestamp",
                    "module_name": "great_expectations.execution_environment.data_connector.partitioner.sorter",
                    "class_name": "DateTimeSorter",
                    "orderby": "desc",
                    "config_params": {
                        "datetime_format": "%Y%m%d",
                    }
                },
                {
                    "name": "price",
                    "module_name": "great_expectations.execution_environment.data_connector.partitioner.sorter",
                    "class_name": "NumericSorter",
                    "orderby": "desc",
                },
            ]
    # the group named price -> not_price
    config_params = {
        "regex": {"pattern": r".+\/(.+)_(.+)_(.+)\.csv", "group_names": ["name", "timestamp", "not_price"]}
    }
    regex_partitioner = RegexPartitioner(
        name="test_regex_partitioner",
        sorters=sorters,
        config_params=config_params
    )
    paths: list = [
        "my_dir/alex_20200809_1000.csv",
        "my_dir/eugene_20200810_1500.csv",
        "my_dir/abe_20200831_1040.csv",
    ]
    with pytest.raises(ge_exceptions.PartitionerError):
        regex_partitioner.find_or_create_partitions(paths=paths, data_asset_name="test_asset_0")


def test_regex_partitioner_compute_partitions_sorters_too_many_sorters():
    sorters = [
                {
                    "name": "name",
                    "module_name": "great_expectations.execution_environment.data_connector.partitioner.sorter",
                    "class_name": "LexicographicSorter",
                    "orderby": "asc",
                },
                {
                    "name": "timestamp",
                    "module_name": "great_expectations.execution_environment.data_connector.partitioner.sorter",
                    "class_name": "DateTimeSorter",
                    "orderby": "desc",
                    "config_params": {
                        "datetime_format": "%Y%m%d",
                    }
                },
                {
                    "name": "price",
                    "module_name": "great_expectations.execution_environment.data_connector.partitioner.sorter",
                    "class_name": "NumericSorter",
                    "orderby": "desc",
                },
                {
                    "name": "extra_sorter",
                    "module_name": "great_expectations.execution_environment.data_connector.partitioner.sorter",
                    "class_name": "NumericSorter",
                    "orderby": "desc",
                },
            ]

    config_params = {"regex": {"pattern": r".+\/(.+)_(.+)_(.+)\.csv", "group_names": ["name", "timestamp", "price"]}}
    regex_partitioner = RegexPartitioner(
        name="test_regex_partitioner",
        sorters=sorters,
        config_params=config_params
    )
    paths: list = [
        "my_dir/alex_20200809_1000.csv",
        "my_dir/eugene_20200810_1500.csv",
        "my_dir/abe_20200831_1040.csv",
    ]
    with pytest.raises(ge_exceptions.PartitionerError):
        regex_partitioner.find_or_create_partitions(paths=paths, data_asset_name="test_asset_0")

