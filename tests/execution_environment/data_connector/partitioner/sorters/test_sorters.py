
import logging

try:
    from unittest import mock
except ImportError:
    import mock

logger = logging.getLogger(__name__)

from great_expectations.execution_environment.data_connector.partitioner.sorter import Sorter


# necessary parameter 2
sorter_config = {
                "name":{
                    "class_name": "LexicographicSorter",
                    "module_name": "great_expectations.execution_environment.data_connector.partitioner.sorter.lexicographic_sorter",
                    "orderby": "desc",
                },
                "date": {
                    "class_name": "DateTimeSorter",
                    "module_name": "great_expectations.execution_environment.data_connector.partitioner.sorter.date_time_sorter",
                    "timeformatstring": 'yyyymmdd',
                    "orderby": "desc",
                },
                "price": {
                    "class_name": "NumericSorter",
                    "module_name": "great_expectations.execution_environment.data_connector.partitioner.sorter.numeric_sorter",
                    "orderby": "asc",
                }
            }


def test_sorter_instantiation_simple():
    # base
    my_sorter = Sorter(name="base", class_name="Sorter")
    assert isinstance(my_sorter, Sorter)
    assert my_sorter.name == "base"

    # defaults
    print(my_sorter.orderby)
    print(my_sorter.reverse)


    # base
    #my_sorter = Sorter(name="base", class_name="Sorter")
    #assert isinstance(my_sorter, Sorter)
    #assert my_sorter.name == "base"




def test_sorter_instantiation_by_classsname_config():
    pass



