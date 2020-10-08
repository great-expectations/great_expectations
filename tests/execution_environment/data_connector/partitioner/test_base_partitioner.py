import pytest
from great_expectations.marshmallow__shade.exceptions import ValidationError


from great_expectations.execution_environment.data_connector.data_connector import DataConnector
from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
from great_expectations.execution_environment.data_connector.partitioner import Partitioner
from great_expectations.execution_environment.data_connector.partitioner.sorter.sorter import Sorter

import great_expectations.exceptions.exceptions as ge_exceptions


def test_base_partitioner():
    temp_data_connector = DataConnector(name="test")
    test_partitioner = Partitioner(name="test_base_partitioner", data_connector=temp_data_connector)
    # properties
    assert test_partitioner.name == "test_base_partitioner"
    assert test_partitioner.data_connector == temp_data_connector
    assert test_partitioner.sorters == None
    assert test_partitioner.allow_multipart_partitions == False
    assert test_partitioner.config_params == None
    # no sorters
    with pytest.raises(ge_exceptions.SorterError):
        test_partitioner.get_sorter("i_dont_exist")


def test_base_partitioner_with_sorter():
    temp_data_connector = DataConnector(name="test")
    # test sorter config
    price_sorter_config = [
        {"module_name": "great_expectations.execution_environment.data_connector.partitioner.sorter", "orderby": "desc",
         "class_name": "NumericSorter", "name": "price"}]
    test_partitioner_with_sorter = Partitioner(name="test_base_partitioner", data_connector=temp_data_connector,
                                               sorters=price_sorter_config)
    # configured sorter exists
    assert test_partitioner_with_sorter.sorters.__repr__() == str([{"name": "price", "reverse": True}])
    assert test_partitioner_with_sorter.get_sorter("price").__repr__() == str({"name": "price", "reverse": True})

    # from cache
    assert isinstance(test_partitioner_with_sorter._sorters_cache["price"], Sorter)

    assert str(test_partitioner_with_sorter.get_sorter("price")) == str({"name": "price", "reverse": True})

    # no sorters by name of i_dont_exist
    with pytest.raises(ge_exceptions.SorterError):
        test_partitioner_with_sorter.get_sorter("i_dont_exist")


def test_base_partitioner_with_bad_sorter_config():
    temp_data_connector = DataConnector(name="test")

    # 1. class_name is bad
    price_sorter_config = [{"orderby": "desc", "class_name": "IDontExist", "name": "price"}]
    test_partitioner_with_sorter = Partitioner(name="test_base_partitioner", data_connector=temp_data_connector, sorters=price_sorter_config)
    with pytest.raises(ge_exceptions.PluginClassNotFoundError):
        test_partitioner_with_sorter.get_sorter("price")

    # 2. module_name is bad
    price_sorter_config = [{"orderby": "desc", "module_name": "not_a_real_module", "name": "price"}]
    test_partitioner_with_sorter = Partitioner(name="test_base_partitioner", data_connector=temp_data_connector, sorters=price_sorter_config)
    with pytest.raises(ValidationError):
        test_partitioner_with_sorter.get_sorter("price")

    # 3. orderby : not a real order
    price_sorter_config = [{"orderby": "not_a_real_order", "class_name": "NumericSorter", "name": "price"}]
    test_partitioner_with_sorter = Partitioner(name="test_base_partitioner", data_connector=temp_data_connector, sorters=price_sorter_config)
    with pytest.raises(ge_exceptions.SorterError):
        test_partitioner_with_sorter.get_sorter("price")


def test_base_partitioner_get_available_partitions():
    temp_data_connector = DataConnector(name="test")
    # test sorter config
    price_sorter_config = [{"module_name": "great_expectations.execution_environment.data_connector.partitioner.sorter", "orderby": "desc",
     "class_name": "NumericSorter", "name": "price"}]
    test_partitioner_with_sorter = Partitioner(name="test_base_partitioner", data_connector=temp_data_connector, sorters=price_sorter_config)
    # on its own this will return a NotImplementedError.
    # get_available_partitions() calls _compute_partitions_for_data_asset() which is implemented by subclass of base Partitioner
    with pytest.raises(NotImplementedError):
        test_partitioner_with_sorter.get_available_partitions()
    # with repartition
    with pytest.raises(NotImplementedError):
        test_partitioner_with_sorter.get_available_partitions(repartition=True)
