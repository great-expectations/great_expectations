import pytest
import great_expectations.exceptions.exceptions as ge_exceptions
from great_expectations.execution_environment.data_connector.data_connector import DataConnector
from great_expectations.execution_environment.data_connector.partitioner import(
    RegexPartitioner,
    Partition,
    )


def test_regex_partitioner_instantiation():
    data_connector = DataConnector(name="test")
    partitioner = RegexPartitioner(name="test_regex_partitioner", data_connector=data_connector)
    # defaults
    assert partitioner.name == "test_regex_partitioner"
    assert partitioner.data_connector == data_connector
    assert partitioner.sorters == None
    assert partitioner.allow_multipart_partitions == False
    assert partitioner.config_params == None
    # without regex configured, you will get a default pattern
    assert partitioner.regex == {"pattern": r"(.*)", "group_names": ["group_0"]}


def test_regex_partitioner_regex_not_dict():
    data_connector = DataConnector(name="test")
    # bad regex configuration
    config_params = {"regex": 'i_am_not_a_dictionary'}
    partitioner = RegexPartitioner(name="test_regex_partitioner", data_connector=data_connector, config_params=config_params)
    # if regex is not a dict, then you will get default configuration
    # <WILL> should there be a more informative message
    assert partitioner.regex == {"pattern": r"(.*)", "group_names": ["group_0"]}


def test_regex_partitioner_regex_missing_pattern():
    data_connector = DataConnector(name="test")
    # missing pattern
    config_params = {"regex": {"not pattern": "not pattern either"}}
    with pytest.raises(AssertionError):
        RegexPartitioner(name="test_regex_partitioner", data_connector=data_connector, config_params=config_params)


def test_regex_partitioner_regex_no_groups_named():
    data_connector = DataConnector(name="test")
    # adding pattern (no groups named)
    config_params = {"regex": {"pattern": r".+\/(.+)_(.+)_(.+)\.csv"}}
    regex_partitioner = RegexPartitioner(name="test_regex_partitioner", data_connector=data_connector, config_params=config_params)
    assert regex_partitioner.regex == {'pattern': '.+\\/(.+)_(.+)_(.+)\\.csv', 'group_names': []}


def test_regex_partitioner_regex_groups_named():
    data_connector = DataConnector(name="test")
    # adding pattern with named groups
    config_params = {"regex": {"pattern": r".+\/(.+)_(.+)_(.+)\.csv", "group_names": ['name', 'timestamp', 'price']}}
    regex_partitioner = RegexPartitioner(name="test_regex_partitioner", data_connector=data_connector,
                                         config_params=config_params)
    assert regex_partitioner.regex == {'pattern': '.+\\/(.+)_(.+)_(.+)\\.csv', 'group_names': ['name', 'timestamp', 'price']}


def test_regex_partitioner_compute_partitions_for_data_asset_with_no_configuration():
    data_connector = DataConnector(name="test")
    config_params = {"regex": {"pattern": r".+\/(.+)_(.+)_(.+)\.csv", "group_names": ['name', 'timestamp', 'price']}}
    regex_partitioner = RegexPartitioner(name="test_regex_partitioner", data_connector=data_connector,
                                         config_params=config_params)
    # Nothing configured
    assert regex_partitioner.get_available_partitions() == []


def test_regex_partitioner_bad_regex():
    data_connector = DataConnector(name="test")
    config_params = {"regex": {"pattern": r".+\/(.+)_(.+)_(.+)\.csv", "group_names": ['name', 'timestamp', 'price']}}
    regex_partitioner = RegexPartitioner(name="test_regex_partitioner", data_connector=data_connector,
                                         config_params=config_params)
    batch_paths_simple: list = [
        "my_dir/hi.csv",
        "my_dir/i_wont.csv",
        "my_dir/work.csv",
    ]
    # Nothing configured
    partitions = regex_partitioner.get_available_partitions(paths=batch_paths_simple, data_asset_name="test_asset_0")
    assert partitions == []


def test_regex_partitioner_compute_partitions_auto_discover_assets_true():
    data_connector = DataConnector(name="test")
    config_params = {"regex": {"pattern": r".+\/(.+)_(.+)_(.+)\.csv", "group_names": ['name', 'timestamp', 'price']}}
    regex_partitioner = RegexPartitioner(name="test_regex_partitioner", data_connector=data_connector,
                                         config_params=config_params)
    batch_paths_simple: list = [
        "my_dir/alex_20200809_1000.csv",
        "my_dir/eugene_20200809_1500.csv",
        "my_dir/abe_20200809_1040.csv",
    ]
    # auto_discover_assets is set to True, which means the data_asset_name will come from the filename
    # no sorters configured
    partitions = regex_partitioner.get_available_partitions(paths=batch_paths_simple, auto_discover_assets=True)
    assert partitions == [
        Partition(name='alex-20200809-1000',
                  definition={'name': 'alex', 'timestamp': '20200809', 'price': '1000'},
                  data_reference="my_dir/alex_20200809_1000.csv", data_asset_name="alex_20200809_1000"),
        Partition(name='eugene-20200809-1500',
                  definition={'name': 'eugene', 'timestamp': '20200809', 'price': '1500'},
                  data_reference="my_dir/eugene_20200809_1500.csv", data_asset_name="eugene_20200809_1500"),
        Partition(name='abe-20200809-1040',
                  definition={'name': 'abe', 'timestamp': '20200809', 'price': '1040'},
                  data_reference="my_dir/abe_20200809_1040.csv", data_asset_name="abe_20200809_1040"),
    ]


def test_regex_partitioner_compute_partitions_auto_discover_assets_false_no_data_asset_name():
    data_connector = DataConnector(name="test")
    config_params = {"regex": {"pattern": r".+\/(.+)_(.+)_(.+)\.csv", "group_names": ['name', 'timestamp', 'price']}}
    regex_partitioner = RegexPartitioner(name="test_regex_partitioner", data_connector=data_connector,
                                         config_params=config_params)
    batch_paths_simple: list = [
        "my_dir/alex_20200809_1000.csv",
        "my_dir/eugene_20200809_1500.csv",
        "my_dir/abe_20200809_1040.csv",
    ]
    # <WILL> Should this be a default value?
    partitions = regex_partitioner.get_available_partitions(paths=batch_paths_simple, auto_discover_assets=False)
    assert partitions == [
        Partition(name='alex-20200809-1000',
                  definition={'name': 'alex', 'timestamp': '20200809', 'price': '1000'},
                  data_reference="my_dir/alex_20200809_1000.csv", data_asset_name=None),
        Partition(name='eugene-20200809-1500',
                  definition={'name': 'eugene', 'timestamp': '20200809', 'price': '1500'},
                  data_reference="my_dir/eugene_20200809_1500.csv", data_asset_name=None),
        Partition(name='abe-20200809-1040',
                  definition={'name': 'abe', 'timestamp': '20200809', 'price': '1040'},
                  data_reference="my_dir/abe_20200809_1040.csv", data_asset_name=None),
    ]


def test_regex_partitioner_compute_partitions_auto_discover_assets_false_data_asset_name_included():
    data_connector = DataConnector(name="test")
    config_params = {"regex": {"pattern": r".+\/(.+)_(.+)_(.+)\.csv", "group_names": ['name', 'timestamp', 'price']}}
    regex_partitioner = RegexPartitioner(name="test_regex_partitioner", data_connector=data_connector,
                                         config_params=config_params)
    batch_paths_simple: list = [
        "my_dir/alex_20200809_1000.csv",
        "my_dir/eugene_20200809_1500.csv",
        "my_dir/abe_20200809_1040.csv",
    ]
    # auto_discover_assets is set to True, which means the data_asset_name will come from the filename
    # no sorters configured
    partitions = regex_partitioner.get_available_partitions(paths=batch_paths_simple, data_asset_name="test_asset_0")
    assert partitions == [
        Partition(name='alex-20200809-1000',
                  definition={'name': 'alex', 'timestamp': '20200809', 'price': '1000'},
                  data_reference="my_dir/alex_20200809_1000.csv", data_asset_name="test_asset_0"),
        Partition(name='eugene-20200809-1500',
                  definition={'name': 'eugene', 'timestamp': '20200809', 'price': '1500'},
                  data_reference="my_dir/eugene_20200809_1500.csv", data_asset_name="test_asset_0"),
        Partition(name='abe-20200809-1040',
                  definition={'name': 'abe', 'timestamp': '20200809', 'price': '1040'},
                  data_reference="my_dir/abe_20200809_1040.csv", data_asset_name="test_asset_0"),
    ]


def test_regex_partitioner_compute_partitions_adding_sorters():
    data_connector = DataConnector(name="test")
    sorters = [
                {
                    'name': 'name',
                    'module_name': 'great_expectations.execution_environment.data_connector.partitioner.sorter',
                    'class_name': 'LexicographicSorter',
                    'orderby': 'asc',
                },
                {
                    'name': 'timestamp',
                    'module_name': 'great_expectations.execution_environment.data_connector.partitioner.sorter',
                    'class_name': 'DateTimeSorter',
                    'orderby': 'desc',
                    'config_params': {
                        'datetime_format': '%Y%m%d',
                    }
                },
                {
                    'name': 'price',
                    'module_name': 'great_expectations.execution_environment.data_connector.partitioner.sorter',
                    'class_name': 'NumericSorter',
                    'orderby': 'desc',
                },
            ]

    config_params = {"regex": {"pattern": r".+\/(.+)_(.+)_(.+)\.csv", "group_names": ['name', 'timestamp', 'price']}}
    regex_partitioner = RegexPartitioner(name="test_regex_partitioner", data_connector=data_connector, sorters=sorters,
                                         config_params=config_params)
    batch_paths_simple: list = [
        "my_dir/alex_20200809_1000.csv",
        "my_dir/eugene_20200809_1500.csv",
        "my_dir/abe_20200809_1040.csv",
    ]

    partitions = regex_partitioner.get_available_partitions(paths=batch_paths_simple, data_asset_name="test_asset_0")
    assert partitions == [
        Partition(name='abe-20200809-1040',
                  definition={'name': 'abe', 'timestamp': '20200809', 'price': '1040'},
                  data_reference="my_dir/abe_20200809_1040.csv", data_asset_name="test_asset_0"),
        Partition(name='alex-20200809-1000',
                  definition={'name': 'alex', 'timestamp': '20200809', 'price': '1000'},
                  data_reference="my_dir/alex_20200809_1000.csv", data_asset_name="test_asset_0"),
        Partition(name='eugene-20200809-1500',
                  definition={'name': 'eugene', 'timestamp': '20200809', 'price': '1500'},
                  data_reference="my_dir/eugene_20200809_1500.csv", data_asset_name="test_asset_0"),
    ]



def test_regex_partitioner_compute_partitions_sorters_and_groups_do_not_match():
    data_connector = DataConnector(name="test")
    sorters = [
                {
                    'name': 'name',
                    'module_name': 'great_expectations.execution_environment.data_connector.partitioner.sorter',
                    'class_name': 'LexicographicSorter',
                    'orderby': 'asc',
                },
                {
                    'name': 'timestamp',
                    'module_name': 'great_expectations.execution_environment.data_connector.partitioner.sorter',
                    'class_name': 'DateTimeSorter',
                    'orderby': 'desc',
                    'config_params': {
                        'datetime_format': '%Y%m%d',
                    }
                },
                {
                    'name': 'price',
                    'module_name': 'great_expectations.execution_environment.data_connector.partitioner.sorter',
                    'class_name': 'NumericSorter',
                    'orderby': 'desc',
                },
            ]
    # the group named price -> not_price
    config_params = {"regex": {"pattern": r".+\/(.+)_(.+)_(.+)\.csv", "group_names": ['name', 'timestamp', 'not_price']}}
    regex_partitioner = RegexPartitioner(name="test_regex_partitioner", data_connector=data_connector, sorters=sorters,
                                         config_params=config_params)
    batch_paths_simple: list = [
        "my_dir/alex_20200809_1000.csv",
        "my_dir/eugene_20200809_1500.csv",
        "my_dir/abe_20200809_1040.csv",
    ]
    with pytest.raises(ge_exceptions.PartitionerError):
        regex_partitioner.get_available_partitions(paths=batch_paths_simple, data_asset_name="test_asset_0")


def test_regex_partitioner_compute_partitions_sorters_too_many_sorters():
    data_connector = DataConnector(name="test")
    sorters = [
                {
                    'name': 'name',
                    'module_name': 'great_expectations.execution_environment.data_connector.partitioner.sorter',
                    'class_name': 'LexicographicSorter',
                    'orderby': 'asc',
                },
                {
                    'name': 'timestamp',
                    'module_name': 'great_expectations.execution_environment.data_connector.partitioner.sorter',
                    'class_name': 'DateTimeSorter',
                    'orderby': 'desc',
                    'config_params': {
                        'datetime_format': '%Y%m%d',
                    }
                },
                {
                    'name': 'price',
                    'module_name': 'great_expectations.execution_environment.data_connector.partitioner.sorter',
                    'class_name': 'NumericSorter',
                    'orderby': 'desc',
                },
                {
                    'name': 'extra_sorter',
                    'module_name': 'great_expectations.execution_environment.data_connector.partitioner.sorter',
                    'class_name': 'NumericSorter',
                    'orderby': 'desc',
                },
            ]
    # the group named price -> not_price
    config_params = {"regex": {"pattern": r".+\/(.+)_(.+)_(.+)\.csv", "group_names": ['name', 'timestamp', 'price']}}
    regex_partitioner = RegexPartitioner(name="test_regex_partitioner", data_connector=data_connector, sorters=sorters,
                                         config_params=config_params)
    batch_paths_simple: list = [
        "my_dir/alex_20200809_1000.csv",
        "my_dir/eugene_20200809_1500.csv",
        "my_dir/abe_20200809_1040.csv",
    ]
    with pytest.raises(ge_exceptions.PartitionerError):
        regex_partitioner.get_available_partitions(paths=batch_paths_simple, data_asset_name="test_asset_0")
