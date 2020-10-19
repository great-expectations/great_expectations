import pytest
# from string import Template
import re

from great_expectations.core.id_dict import PartitionDefinition
from great_expectations.execution_environment.data_connector.partitioner import (
    RegexPartitioner,
    Partition,
)
from great_expectations.core.batch import (
    BatchRequest,
    PartitionRequest,
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


def test_convert_batch_request_to_data_reference():

    # Test an example with only capturing groups in the regex pattern
    regex_partitioner = RegexPartitioner(
        name="test_regex_partitioner",
        config_params={
            "regex": {
                "pattern": r"^(.+)_(\d+)_(\d+)\.csv$",
                "group_names": ["name", "timestamp", "price"]
            }
        }
    )

    assert regex_partitioner.convert_batch_request_to_data_reference(
        BatchRequest(
            partition_request=PartitionRequest(**{
                "name": "alex",
                "timestamp": "20200809",
                "price": "1000",
            })
        )
    ) == "alex_20200809_1000.csv"

    # Test an example with an uncaptured regex group (should return a WildcardDataReference)
    regex_partitioner = RegexPartitioner(
        name="test_regex_partitioner",
        config_params={
            "regex": {
                "pattern": r"^(.+)_(\d+)_\d+\.csv$",
                "group_names": ["name", "timestamp"]
            }
        }
    )

    assert regex_partitioner.convert_batch_request_to_data_reference(
        BatchRequest(
            partition_request=PartitionDefinition(**{
                "name": "alex",
                "timestamp": "20200809",
                "price": "1000",
            })
        )
    ) == "alex_20200809_*.csv"

    # Test an example with an uncaptured regex group that's not at the end (should return a WildcardDataReference)
    regex_partitioner = RegexPartitioner(
        name="test_regex_partitioner",
        config_params={
            "regex": {
                "pattern": r"^.+_(\d+)_(\d+)\.csv$",
                "group_names": ["timestamp", "price"]
            }
        }
    )

    assert regex_partitioner.convert_batch_request_to_data_reference(
        BatchRequest(
            partition_request=PartitionDefinition(**{
                "timestamp": "20200809",
                "price": "1000",
            })
        )
    ) == "*_20200809_1000.csv"

def test__invert_regex_to_data_reference_template():
    regex_partitioner = RegexPartitioner(
        name="test_regex_partitioner",
        config_params={
            "regex": {
                "pattern": r"^(.+)_(\d+)_(\d+)\.csv$",
                "group_names": ["name", "timestamp", "price"]
            }
        }
    )
    assert regex_partitioner._invert_regex_to_data_reference_template() == "{name}_{timestamp}_{price}.csv"

    regex_partitioner = RegexPartitioner(
        name="test_regex_partitioner",
        config_params={
            "regex": {
                "pattern": r"^(.+)_(\d+)_\d+\.csv$",
                "group_names": ["name", "timestamp",]
            }
        }
    )
    assert regex_partitioner._invert_regex_to_data_reference_template() == "{name}_{timestamp}_*.csv"

    regex_partitioner = RegexPartitioner(
        name="test_regex_partitioner",
        config_params={
            "regex": {
                "pattern": r"^.+_(\d+)_(\d+)\.csv$",
                "group_names": ["timestamp", "price"]
            }
        }
    )
    assert regex_partitioner._invert_regex_to_data_reference_template() == "*_{timestamp}_{price}.csv"

    regex_partitioner = RegexPartitioner(
        name="test_regex_partitioner",
        config_params={
            "regex": {
                "pattern": r"(^.+)_(\d+)_.\d\W\w[a-z](?!.*::.*::)\d\.csv$",
                "group_names": ["name", "timestamp"]
            }
        }
    )
    assert regex_partitioner._invert_regex_to_data_reference_template() == "{name}_{timestamp}_*.csv"


    regex_partitioner = RegexPartitioner(
        name="test_regex_partitioner",
        config_params={
            "regex": {
                "pattern": r"(.*)-([ABC])\.csv",
                "group_names": ["name", "type"]
            }
        }
    )
    assert regex_partitioner._invert_regex_to_data_reference_template() == "{name}-{type}.csv"

    regex_partitioner = RegexPartitioner(
        name="test_regex_partitioner",
        config_params={
            "regex": {
                "pattern": r"(.*)-[A|B|C]\.csv",
                "group_names": ["name"]
            }
        }
    )
    assert regex_partitioner._invert_regex_to_data_reference_template() == "{name}-*.csv"

    # From https://github.com/madisonmay/CommonRegex/blob/master/commonregex.py

    date             = r'(?:(?<!\:)(?<!\:\d)[0-3]?\d(?:st|nd|rd|th)?\s+(?:of\s+)?(?:jan\.?|january|feb\.?|february|mar\.?|march|apr\.?|april|may|jun\.?|june|jul\.?|july|aug\.?|august|sep\.?|september|oct\.?|october|nov\.?|november|dec\.?|december)|(?:jan\.?|january|feb\.?|february|mar\.?|march|apr\.?|april|may|jun\.?|june|jul\.?|july|aug\.?|august|sep\.?|september|oct\.?|october|nov\.?|november|dec\.?|december)\s+(?<!\:)(?<!\:\d)[0-3]?\d(?:st|nd|rd|th)?)(?:\,)?\s*(?:\d{4})?|[0-3]?\d[-\./][0-3]?\d[-\./]\d{2,4}'
    time             = r'\d{1,2}:\d{2} ?(?:[ap]\.?m\.?)?|\d[ap]\.?m\.?'
    phone            = r'''((?:(?<![\d-])(?:\+?\d{1,3}[-.\s*]?)?(?:\(?\d{3}\)?[-.\s*]?)?\d{3}[-.\s*]?\d{4}(?![\d-]))|(?:(?<![\d-])(?:(?:\(\+?\d{2}\))|(?:\+?\d{2}))\s*\d{2}\s*\d{3}\s*\d{4}(?![\d-])))'''
    phones_with_exts = r'((?:(?:\+?1\s*(?:[.-]\s*)?)?(?:\(\s*(?:[2-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9])\s*\)|(?:[2-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9]))\s*(?:[.-]\s*)?)?(?:[2-9]1[02-9]|[2-9][02-9]1|[2-9][02-9]{2})\s*(?:[.-]\s*)?(?:[0-9]{4})(?:\s*(?:#|x\.?|ext\.?|extension)\s*(?:\d+)?))'
    link             = r'(?i)((?:https?://|www\d{0,3}[.])?[a-z0-9.\-]+[.](?:(?:international)|(?:construction)|(?:contractors)|(?:enterprises)|(?:photography)|(?:immobilien)|(?:management)|(?:technology)|(?:directory)|(?:education)|(?:equipment)|(?:institute)|(?:marketing)|(?:solutions)|(?:builders)|(?:clothing)|(?:computer)|(?:democrat)|(?:diamonds)|(?:graphics)|(?:holdings)|(?:lighting)|(?:plumbing)|(?:training)|(?:ventures)|(?:academy)|(?:careers)|(?:company)|(?:domains)|(?:florist)|(?:gallery)|(?:guitars)|(?:holiday)|(?:kitchen)|(?:recipes)|(?:shiksha)|(?:singles)|(?:support)|(?:systems)|(?:agency)|(?:berlin)|(?:camera)|(?:center)|(?:coffee)|(?:estate)|(?:kaufen)|(?:luxury)|(?:monash)|(?:museum)|(?:photos)|(?:repair)|(?:social)|(?:tattoo)|(?:travel)|(?:viajes)|(?:voyage)|(?:build)|(?:cheap)|(?:codes)|(?:dance)|(?:email)|(?:glass)|(?:house)|(?:ninja)|(?:photo)|(?:shoes)|(?:solar)|(?:today)|(?:aero)|(?:arpa)|(?:asia)|(?:bike)|(?:buzz)|(?:camp)|(?:club)|(?:coop)|(?:farm)|(?:gift)|(?:guru)|(?:info)|(?:jobs)|(?:kiwi)|(?:land)|(?:limo)|(?:link)|(?:menu)|(?:mobi)|(?:moda)|(?:name)|(?:pics)|(?:pink)|(?:post)|(?:rich)|(?:ruhr)|(?:sexy)|(?:tips)|(?:wang)|(?:wien)|(?:zone)|(?:biz)|(?:cab)|(?:cat)|(?:ceo)|(?:com)|(?:edu)|(?:gov)|(?:int)|(?:mil)|(?:net)|(?:onl)|(?:org)|(?:pro)|(?:red)|(?:tel)|(?:uno)|(?:xxx)|(?:ac)|(?:ad)|(?:ae)|(?:af)|(?:ag)|(?:ai)|(?:al)|(?:am)|(?:an)|(?:ao)|(?:aq)|(?:ar)|(?:as)|(?:at)|(?:au)|(?:aw)|(?:ax)|(?:az)|(?:ba)|(?:bb)|(?:bd)|(?:be)|(?:bf)|(?:bg)|(?:bh)|(?:bi)|(?:bj)|(?:bm)|(?:bn)|(?:bo)|(?:br)|(?:bs)|(?:bt)|(?:bv)|(?:bw)|(?:by)|(?:bz)|(?:ca)|(?:cc)|(?:cd)|(?:cf)|(?:cg)|(?:ch)|(?:ci)|(?:ck)|(?:cl)|(?:cm)|(?:cn)|(?:co)|(?:cr)|(?:cu)|(?:cv)|(?:cw)|(?:cx)|(?:cy)|(?:cz)|(?:de)|(?:dj)|(?:dk)|(?:dm)|(?:do)|(?:dz)|(?:ec)|(?:ee)|(?:eg)|(?:er)|(?:es)|(?:et)|(?:eu)|(?:fi)|(?:fj)|(?:fk)|(?:fm)|(?:fo)|(?:fr)|(?:ga)|(?:gb)|(?:gd)|(?:ge)|(?:gf)|(?:gg)|(?:gh)|(?:gi)|(?:gl)|(?:gm)|(?:gn)|(?:gp)|(?:gq)|(?:gr)|(?:gs)|(?:gt)|(?:gu)|(?:gw)|(?:gy)|(?:hk)|(?:hm)|(?:hn)|(?:hr)|(?:ht)|(?:hu)|(?:id)|(?:ie)|(?:il)|(?:im)|(?:in)|(?:io)|(?:iq)|(?:ir)|(?:is)|(?:it)|(?:je)|(?:jm)|(?:jo)|(?:jp)|(?:ke)|(?:kg)|(?:kh)|(?:ki)|(?:km)|(?:kn)|(?:kp)|(?:kr)|(?:kw)|(?:ky)|(?:kz)|(?:la)|(?:lb)|(?:lc)|(?:li)|(?:lk)|(?:lr)|(?:ls)|(?:lt)|(?:lu)|(?:lv)|(?:ly)|(?:ma)|(?:mc)|(?:md)|(?:me)|(?:mg)|(?:mh)|(?:mk)|(?:ml)|(?:mm)|(?:mn)|(?:mo)|(?:mp)|(?:mq)|(?:mr)|(?:ms)|(?:mt)|(?:mu)|(?:mv)|(?:mw)|(?:mx)|(?:my)|(?:mz)|(?:na)|(?:nc)|(?:ne)|(?:nf)|(?:ng)|(?:ni)|(?:nl)|(?:no)|(?:np)|(?:nr)|(?:nu)|(?:nz)|(?:om)|(?:pa)|(?:pe)|(?:pf)|(?:pg)|(?:ph)|(?:pk)|(?:pl)|(?:pm)|(?:pn)|(?:pr)|(?:ps)|(?:pt)|(?:pw)|(?:py)|(?:qa)|(?:re)|(?:ro)|(?:rs)|(?:ru)|(?:rw)|(?:sa)|(?:sb)|(?:sc)|(?:sd)|(?:se)|(?:sg)|(?:sh)|(?:si)|(?:sj)|(?:sk)|(?:sl)|(?:sm)|(?:sn)|(?:so)|(?:sr)|(?:st)|(?:su)|(?:sv)|(?:sx)|(?:sy)|(?:sz)|(?:tc)|(?:td)|(?:tf)|(?:tg)|(?:th)|(?:tj)|(?:tk)|(?:tl)|(?:tm)|(?:tn)|(?:to)|(?:tp)|(?:tr)|(?:tt)|(?:tv)|(?:tw)|(?:tz)|(?:ua)|(?:ug)|(?:uk)|(?:us)|(?:uy)|(?:uz)|(?:va)|(?:vc)|(?:ve)|(?:vg)|(?:vi)|(?:vn)|(?:vu)|(?:wf)|(?:ws)|(?:ye)|(?:yt)|(?:za)|(?:zm)|(?:zw))(?:/[^\s()<>]+[^\s`!()\[\]{};:\'".,<>?\xab\xbb\u201c\u201d\u2018\u2019])?)'
    email            = r"([a-z0-9!#$%&'*+\/=?^_`{|.}~-]+@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)"
    ip               = r'(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)'
    ipv6             = r'\s*(?!.*::.*::)(?:(?!:)|:(?=:))(?:[0-9a-f]{0,4}(?:(?<=::)|(?<!::):)){6}(?:[0-9a-f]{0,4}(?:(?<=::)|(?<!::):)[0-9a-f]{0,4}(?:(?<=::)|(?<!:)|(?<=:)(?<!::):)|(?:25[0-4]|2[0-4]\d|1\d\d|[1-9]?\d)(?:\.(?:25[0-4]|2[0-4]\d|1\d\d|[1-9]?\d)){3})\s*'
    price            = r'[$]\s?[+-]?[0-9]{1,3}(?:(?:,?[0-9]{3}))*(?:\.[0-9]{1,2})?'
    hex_color        = r'(#(?:[0-9a-fA-F]{8})|#(?:[0-9a-fA-F]{3}){1,2})\\b'
    credit_card      = r'((?:(?:\\d{4}[- ]?){3}\\d{4}|\\d{15,16}))(?![\\d])'
    btc_address      = r'(?<![a-km-zA-HJ-NP-Z0-9])[13][a-km-zA-HJ-NP-Z0-9]{26,33}(?![a-km-zA-HJ-NP-Z0-9])'
    street_address   = r'\d{1,4} [\w\s]{1,20}(?:street|st|avenue|ave|road|rd|highway|hwy|square|sq|trail|trl|drive|dr|court|ct|park|parkway|pkwy|circle|cir|boulevard|blvd)\W?(?=\s|$)'
    zip_code         = r'\b\d{5}(?:[-\s]\d{4})?\b'
    po_box           = r'P\.? ?O\.? Box \d+'
    ssn              = r'(?!000|666|333)0*(?:[0-6][0-9][0-9]|[0-7][0-6][0-9]|[0-7][0-7][0-2])[- ](?!00)[0-9]{2}[- ](?!0000)[0-9]{4}'

    regexes = {
        "dates"            : date,
        "times"            : time,
        "phones"           : phone,
        "phones_with_exts" : phones_with_exts,
        "links"            : link,
        "emails"           : email,
        "ips"              : ip,
        "ipv6s"            : ipv6,
        "prices"           : price,
        "hex_colors"       : hex_color,
        "credit_cards"     : credit_card,
        "btc_addresses"    : btc_address,
        "street_addresses" : street_address,
        "zip_codes"        : zip_code,
        "po_boxes"         : po_box,
        "ssn_number"       : ssn
    }

    # This is a scattershot approach to making sure that our regex parsing has good coverage.
    # It does not guarantee perfect coverage of all regex patterns.
    for name, regex in regexes.items():
        print(name)
        regex_partitioner = RegexPartitioner(
            name="test_regex_partitioner",
            config_params={
                "regex": {
                    "pattern": regex,
                    "group_names": ["name", "timestamp"]
                }
            }
        )
        name, regex, regex_partitioner._invert_regex_to_data_reference_template()




def test_convert_data_reference_to_batch_request():
    regex_partitioner = RegexPartitioner(
        name="test_regex_partitioner",
        config_params={
            "regex": {
                "pattern": r"^(.+)_(\d+)_(\d+)\.csv$",
                "group_names": ["name", "timestamp", "price"]
            }
        }
    )

    print(regex_partitioner.convert_data_reference_to_batch_request("alex_20200809_1000.csv"))
    assert regex_partitioner.convert_data_reference_to_batch_request("alex_20200809_1000.csv") == BatchRequest(
        partition_request=PartitionDefinition(**{
            "name": "alex",
            "timestamp": "20200809",
            "price": "1000",
        })
    )

    assert regex_partitioner.convert_data_reference_to_batch_request("eugene_20200810_1500.csv") == BatchRequest(
        partition_request=PartitionDefinition(**{
            "name": "eugene",
            "timestamp": "20200810",
            "price": "1500",
        })
    )

    assert regex_partitioner.convert_data_reference_to_batch_request("DOESNT_MATCH_CAPTURING_GROUPS.csv")) == None

    assert regex_partitioner.convert_data_reference_to_batch_request("eugene_DOESNT_MATCH_ALL_CAPTURING_GROUPS_1500.csv") == None

    # TODO ABE 20201017 : Future case to handle
    # with pytest.raises(ValueError):
    #     regex_partitioner._convert_data_reference_to_batch_request("NOT_THE_RIGHT_DIR/eugene_20200810_1500.csv")
