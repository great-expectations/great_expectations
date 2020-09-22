import logging
import pytest


# TODO: <Alex>Will: This is temporary -- just to get a crude test going.</Alex>
from great_expectations.execution_environment.data_connector.data_connector import DataConnector
from great_expectations.execution_environment.data_connector.partitioner.regex_partitioner import RegexPartitioner
from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
from great_expectations.execution_environment.data_connector.partitioner.sorter.lexicographic_sorter import LexicographicSorter
from great_expectations.execution_environment.data_connector.partitioner.sorter.date_time_sorter import DateTimeSorter
from great_expectations.execution_environment.data_connector.partitioner.sorter.numeric_sorter import NumericSorter
from great_expectations.execution_environment.data_connector.partitioner.sorter.custom_list_sorter import CustomListSorter

from .reference_list import ReferenceListForTests


try:
    from unittest import mock
except ImportError:
    import mock

logger = logging.getLogger(__name__)


# TODO: <Alex>We might wish to invent more cool paths to test and different column types and sort orders...</Alex>
# TODO: <WILL> Is the order always going to be from "left-to-right"? in the example below, what happens if you want the date-time to take priority?
# TODO: more tests :  more complex custom lists, and playing with asc desc combinations
def test_regex_partitioner_only_regex_configured():
    batch_paths: list = [
        "my_dir/alex_20200809_1000.csv",
        "my_dir/eugene_20200809_1500.csv",
        "my_dir/james_20200811_1009.csv",
        "my_dir/abe_20200809_1040.csv",
        "my_dir/will_20200809_1002.csv",
        "my_dir/james_20200713_1567.csv",
        "my_dir/eugene_20201129_1900.csv",
        "my_dir/will_20200810_1001.csv",
        "my_dir/james_20200810_1003.csv",
        "my_dir/alex_20200819_1300.csv",
    ]
    regex: str = r".*/(.*)_(.*)_(.*).csv"

    # TODO: <Alex>Will, we do need a data_connector here...</Alex>
    my_partitioner = RegexPartitioner(
        # TODO: <Alex>Will: This is temporary -- just to get a crude test going.</Alex>
        data_connector=DataConnector(name='alex_temp_test', execution_environment={}),
        name="mine_all_mine",
        paths=batch_paths,
        regex=regex,
        # TODO: <Alex>Will, could you please conjure up a test for allow_multifile_partitions=True as well?</Alex>
        allow_multifile_partitions=False,
    )

    # # test 1: no regex configured. we  raise error
    # with pytest.raises(ValueError) as exc:
    #     partitions = my_partitioner.get_available_partitions()

    my_partitioner.regex = regex
    returned_partitions = my_partitioner.get_available_partitions()
    assert returned_partitions == [
         Partition(name='alex-20200809-1000', definition={'group_0': 'alex', 'group_1': '20200809', 'group_2': '1000'}, source="my_dir/alex_20200809_1000.csv"),
         Partition(name='eugene-20200809-1500', definition={'group_0': 'eugene', 'group_1': '20200809', 'group_2': '1500'}, source="my_dir/eugene_20200809_1500.csv"),
         Partition(name='james-20200811-1009', definition={'group_0': 'james', 'group_1': '20200811', 'group_2': '1009'}, source="my_dir/james_20200811_1009.csv"),
         Partition(name='abe-20200809-1040', definition={'group_0': 'abe', 'group_1': '20200809', 'group_2': '1040'}, source="my_dir/abe_20200809_1040.csv"),
         Partition(name='will-20200809-1002', definition={'group_0': 'will', 'group_1': '20200809', 'group_2': '1002'}, source="my_dir/will_20200809_1002.csv"),
         Partition(name='james-20200713-1567', definition={'group_0': 'james', 'group_1': '20200713', 'group_2': '1567'}, source="my_dir/james_20200713_1567.csv"),
         Partition(name='eugene-20201129-1900', definition={'group_0': 'eugene', 'group_1': '20201129', 'group_2': '1900'}, source="my_dir/eugene_20201129_1900.csv"),
         Partition(name='will-20200810-1001', definition={'group_0': 'will', 'group_1': '20200810', 'group_2': '1001'}, source="my_dir/will_20200810_1001.csv"),
         Partition(name='james-20200810-1003', definition={'group_0': 'james', 'group_1': '20200810', 'group_2': '1003'}, source="my_dir/james_20200810_1003.csv"),
         Partition(name='alex-20200819-1300', definition={'group_0': 'alex', 'group_1': '20200819', 'group_2': '1300'}, source="my_dir/alex_20200819_1300.csv"),
    ]
    # partition names
    returned_partition_names = my_partitioner.get_available_partition_names()
    assert returned_partition_names == [
        'alex-20200809-1000',
        'eugene-20200809-1500',
        'james-20200811-1009',
        'abe-20200809-1040',
        'will-20200809-1002',
        'james-20200713-1567',
        'eugene-20201129-1900',
        'will-20200810-1001',
        'james-20200810-1003',
        'alex-20200819-1300'
    ]


def regex_partitioner_regex_configured_and_sorters_defined_and_named():
    batch_paths: list = [
        "my_dir/alex_20200809_1000.csv",
        "my_dir/eugene_20200809_1500.csv",
        "my_dir/james_20200811_1009.csv",
        "my_dir/abe_20200809_1040.csv",
        "my_dir/will_20200809_1002.csv",
        "my_dir/james_20200713_1567.csv",
        "my_dir/eugene_20201129_1900.csv",
        "my_dir/will_20200810_1001.csv",
        "my_dir/james_20200810_1003.csv",
        "my_dir/alex_20200819_1300.csv",
    ]
    regex: str = r".*/(.*)_(.*)_(.*).csv"

    # TODO: <Alex>Will, we do need a data_connector here...</Alex>
    my_partitioner = RegexPartitioner(
        # TODO: <Alex>Will: This is temporary -- just to get a crude test going.</Alex>
        data_connector=DataConnector(name='alex_temp_test', execution_environment={}),
        name="mine_all_mine",
        paths=batch_paths,
        regex=regex,
        # TODO: <Alex>Will, could you please conjure up a test for allow_multifile_partitions=True as well?</Alex>
        allow_multifile_partitions=False,
        sorters=[
            LexicographicSorter(name='name', orderby='asc'),
            DateTimeSorter(name='timestamp', orderby='desc', datetime_format='%Y%m%d'),
            NumericSorter(name='price', orderby='desc')
        ]
    )

    my_partitioner.regex = regex
    returned_partitions = my_partitioner.get_available_partitions()

    assert returned_partitions == [
        Partition(name='abe-20200809-1040', definition={'name': 'abe', 'timestamp': '20200809', 'price': '1040'}, source="my_dir/abe_20200809_1040.csv"),
        Partition(name='alex-20200819-1300', definition={'name': 'alex', 'timestamp': '20200819', 'price': '1300'}, source="my_dir/alex_20200819_1300.csv"),
        Partition(name='alex-20200809-1000', definition={'name': 'alex', 'timestamp': '20200809', 'price': '1000'}, source="my_dir/alex_20200809_1000.csv"),
        Partition(name='eugene-20201129-1900', definition={'name': 'eugene', 'timestamp': '20201129', 'price': '1900'}, source="my_dir/eugene_20201129_1900.csv"),
        Partition(name='eugene-20200809-1500', definition={'name': 'eugene', 'timestamp': '20200809', 'price': '1500'}, source="my_dir/eugene_20200809_1500.csv"),
        Partition(name='james-20200811-1009', definition={'name': 'james', 'timestamp': '20200811', 'price': '1009'}, source="my_dir/james_20200811_1009.csv"),
        Partition(name='james-20200810-1003', definition={'name': 'james', 'timestamp': '20200810', 'price': '1003'}, source="my_dir/james_20200810_1003.csv"),
        Partition(name='james-20200713-1567', definition={'name': 'james', 'timestamp': '20200713', 'price': '1567'}, source="my_dir/james_20200713_1567.csv"),
        Partition(name='will-20200810-1001', definition={'name': 'will', 'timestamp': '20200810', 'price': '1001'}, source="my_dir/will_20200810_1001.csv"),
        Partition(name='will-20200809-1002', definition={'name': 'will', 'timestamp': '20200809', 'price': '1002'}, source="my_dir/will_20200809_1002.csv"),
    ]

    # partition names
    returned_partition_names = my_partitioner.get_available_partition_names()
    assert returned_partition_names == [
        'abe-20200809-1040',
        'alex-20200819-1300',
        'alex-20200809-1000',
        'eugene-20201129-1900',
        'eugene-20200809-1500',
        'james-20200811-1009',
        'james-20200810-1003',
        'james-20200713-1567',
        'will-20200810-1001',
        'will-20200809-1002',
    ]

    returned_partitions = my_partitioner.get_available_partitions(
        partition_name='james-20200713-1567',
        data_asset_name=None
    )
    assert returned_partitions == [
        Partition(name='james-20200713-1567', definition={'name': 'james', 'timestamp': '20200713', 'price': '1567'}, source="my_dir/james_20200713_1567.csv"),
    ]



# <WILL> I know there is a better way to do this...
ref_periodic_table = ReferenceListForTests().ref_list

def test_regex_partitioner_with_periodic_table():

    batch_paths: list = [
        "my_dir/Boron.csv",
        "my_dir/Oxygen.csv",
        "my_dir/Hydrogen.csv",
    ]
    regex: str = r".*/(.*).csv"

    # TODO: <Alex>Will, we do need a data_connector here...</Alex>
    my_partitioner = RegexPartitioner(
        # TODO: <Alex>Will: This is temporary -- just to get a crude test going.</Alex>
        data_connector=DataConnector(name='alex_temp_test', execution_environment={}),
        name="mine_all_mine",
        paths=batch_paths,
        regex=regex,
        # TODO: <Alex>Will, could you please conjure up a test for allow_multifile_partitions=True as well?</Alex>
        allow_multifile_partitions=False,
        sorters=[
            CustomListSorter(name='element', orderby='asc', reference_list=ref_periodic_table)
        ]
    )
    my_partitioner.regex = regex

    # simple test : let's order the partitions by element number
    returned_partitions = my_partitioner.get_available_partitions()

    assert returned_partitions == [
        Partition(name="Hydrogen", definition={'element': 'Hydrogen'}, source="my_dir/Hydrogen.csv"),
        Partition(name="Boron", definition={'element': 'Boron'}, source="my_dir/Boron.csv"),
        Partition(name="Oxygen", definition={'element': 'Oxygen'}, source="my_dir/Oxygen.csv"),
    ]

    returned_partition_names = my_partitioner.get_available_partition_names()
    assert returned_partition_names == [
        'Hydrogen',
        'Boron',
        'Oxygen'
    ]

    # slightly more complex test. Now the directory has an extra element, Vibranium
    # <WILL> This test didn't run unless I instantiated a new RegexPartitioner like below. Check to see why this is the case.
    # TODO: <Alex>Will: Do we want it to be batch_paths (plural) everywhere in this test module?</Alex>
    batch_path: list = [
        "my_dir/Boron.csv",
        "my_dir/Oxygen.csv",
        "my_dir/Hydrogen.csv",
        "my_dir/Vibranium.csv",
    ]

    # TODO: <Alex>Will, we do need a data_connector here...</Alex>
    my_partitioner = RegexPartitioner(
        # TODO: <Alex>Will: This is temporary -- just to get a crude test going.</Alex>
        data_connector=DataConnector(name='alex_temp_test', execution_environment={}),
        name="mine_all_mine",
        paths=batch_paths,
        regex=regex,
        # TODO: <Alex>Will, could you please conjure up a test for allow_multifile_partitions=True as well?</Alex>
        allow_multifile_partitions=False,
        sorters=[
            CustomListSorter(name='element', orderby='asc', reference_list=ref_periodic_table)
        ]
    )
    my_partitioner.regex = regex
    # catch the ValueError
    with pytest.raises(ValueError):
        returned_partitions = my_partitioner.get_available_partitions()


def test_regex_partitioner_with_periodic_table_allow_multifile_partitions_flag():
    batch_paths: list = [
        "my_dir/Boron_0.csv",
        "my_dir/Oxygen_0.csv",
        "my_dir/Hydrogen_1.csv",
        "my_dir/Titanium_1.csv",
        "my_dir/Titanium_2.csv",
    ]
    regex: str = r".*/(.*)_.*.csv"

    # TODO: <Alex>Will, we do need a data_connector here...</Alex>
    my_partitioner = RegexPartitioner(
        # TODO: <Alex>Will: This is temporary -- just to get a crude test going.</Alex>
        data_connector=DataConnector(name='alex_temp_test', execution_environment={}),
        name="mine_all_mine",
        paths=batch_paths,
        regex=regex,
        allow_multifile_partitions=False,
        sorters=[
            CustomListSorter(name='element', orderby='asc', reference_list=ref_periodic_table)
        ]
    )
    my_partitioner.regex = regex

    with pytest.raises(ValueError):
        my_partitioner.get_available_partitions()


    #with pytest.raises(ValueError):
    #    my_partitioner.get_available_partitions(partition_name="Titanium")


    # now testing the allow_multifile_partitions flag == True
    # TODO: <Alex>Will, we do need a data_connector here...</Alex>
    my_new_partitioner = RegexPartitioner(
        # TODO: <Alex>Will: This is temporary -- just to get a crude test going.</Alex>
        data_connector=DataConnector(name='alex_temp_test', execution_environment={}),
        name="mine_all_mine",
        paths=batch_paths,
        regex=regex,
        allow_multifile_partitions=True,
        sorters=[
            CustomListSorter(name='element', orderby='asc', reference_list=ref_periodic_table)
        ]
    )
    my_new_partitioner.regex = regex


    # getting all
    returned_partitions = my_new_partitioner.get_available_partitions()
    assert returned_partitions == [
        Partition(name="Hydrogen", definition={'element': 'Hydrogen'}, source="my_dir/Hydrogen_1.csv"),
        Partition(name="Boron", definition={'element': 'Boron'}, source="my_dir/Boron_0.csv"),
        Partition(name="Oxygen", definition={'element': 'Oxygen'}, source="my_dir/Oxygen_0.csv"),
        Partition(name="Titanium", definition={'element': 'Titanium'}, source="my_dir/Titanium_1.csv"),
        Partition(name="Titanium", definition={'element': 'Titanium'}, source="my_dir/Titanium_2.csv"),
    ]

    # getting just the ones with repeat
    returned_partitions = my_new_partitioner.get_available_partitions(partition_name="Titanium")
    assert returned_partitions == [
        Partition(name="Titanium", definition={'element': 'Titanium'}, source="my_dir/Titanium_1.csv"),
        Partition(name="Titanium", definition={'element': 'Titanium'}, source="my_dir/Titanium_2.csv"),
    ]

