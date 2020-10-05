import logging
import pytest

from great_expectations.execution_environment.data_connector.partitioner.regex_partitioner import RegexPartitioner
from great_expectations.exceptions import SorterError

try:
    from unittest import mock
except ImportError:
    import mock

logger = logging.getLogger(__name__)

sorters_config = [
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

def test_regex_partitioner_instantiation(data_context_with_data_connector_and_partitioner):

    # builds data_context from yml file under tests/test_fixtures/great_expectations_data_connector_and_partitioner.yml
    ge_context = data_context_with_data_connector_and_partitioner
    my_execution_environment = ge_context.get_execution_environment("my_test_execution_environment")
    my_data_connector = my_execution_environment.get_data_connector("general_filesystem_data_connector")

    config_params = {"regex": {"pattern": r".+\/(.+)_(.+)_(.+)\.csv", "group_names": ["name", "timestamp", "price"]}}

    my_partitioner = RegexPartitioner(data_connector=my_data_connector,
                                      name="mine_all_mine",
                                      allow_multipart_partitions=False,
                                      sorters=sorters_config,
                                      config_params=config_params
                                      )
    # properties
    assert my_partitioner.name == "mine_all_mine"
    assert my_partitioner.data_connector == my_data_connector

    # building sorters
    price_sorter = my_partitioner.get_sorter(name="price")
    assert price_sorter.__repr__() == "{'name': 'price', 'reverse': True}"
    # cache
    price_sorter = my_partitioner.get_sorter(name="price")
    assert price_sorter.__repr__() == "{'name': 'price', 'reverse': True}"

    with pytest.raises(SorterError):
        price_sorter = my_partitioner.get_sorter(name="fake")


