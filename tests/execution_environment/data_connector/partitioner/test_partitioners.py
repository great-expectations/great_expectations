import pytest

from great_expectations.execution_environment.data_connector.data_connector import DataConnector
from great_expectations.execution_environment.data_connector.partitioner.sorter.lexicographic_sorter import LexicographicSorter

from great_expectations.exceptions import SorterError

import logging

logger = logging.getLogger(__name__)


def test_partitioner_instantiation(data_context_with_data_connector_and_partitioner):

    # builds data_context from yml file under tests/test_fixtures/great_expectations_data_connector_and_partitioner.yml
    ge_context = data_context_with_data_connector_and_partitioner
    my_execution_environment = ge_context.get_execution_environment("my_test_execution_environment")
    my_data_connector = my_execution_environment.get_data_connector("general_filesystem_data_connector")
    my_partitioner = my_data_connector.get_partitioner("my_standard_partitioner")

    # properties
    assert my_partitioner.name == "my_standard_partitioner"
    assert isinstance(my_partitioner.data_connector, DataConnector)
    assert my_partitioner.config_params == {'regex': {'pattern': '.+\\/(.+)_(.+)_(.+)\\.csv', 'group_names': ['name', 'timestamp', 'price']}}
    assert my_partitioner.allow_multipart_partitions == False
    # <WILL> missing my_partitions.sorters

    # with real lexicographic sorter
    assert isinstance(my_partitioner.get_sorter("name"), LexicographicSorter)
    # with cached sorter
    assert isinstance(my_partitioner.get_sorter("name"), LexicographicSorter)

    # with fake sorter name
    with pytest.raises(SorterError):
        my_partitioner.get_sorter("fake")

