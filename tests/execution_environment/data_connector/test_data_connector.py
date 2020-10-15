import pytest
import pandas as pd
import yaml
import json

from great_expectations.execution_environment.data_connector import (
    FilesDataConnector,
    DictDataConnector,
)
from great_expectations.data_context.util import (
    instantiate_class_from_config,
)

@pytest.fixture
def basic_data_connector(tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("basic_data_connector__filesystem_data_connector"))

    basic_data_connector = instantiate_class_from_config(yaml.load(f"""
class_name: FilesDataConnector
base_directory: {base_directory}
glob_directive: '*.csv'
    
default_partitioner: my_regex_partitioner
    """, Loader=yaml.FullLoader),
        runtime_environment={
            "name": "my_data_connector"
        },
        config_defaults={
            "module_name": "great_expectations.execution_environment.data_connector"
        }
    )
    return basic_data_connector

def test_basic_instantiation(tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("basic_data_connector__filesystem_data_connector"))

    my_data_connector = FilesDataConnector(
        name="my_data_connector",
        base_directory=base_directory,
        glob_directive='*.csv',
    )
    
# default_partitioner: my_regex_partitioner
# partitioners:
#     my_regex_partitioner:
#         class_name: RegexPartitioner
#         config_params:
#             regex:
#                 group_names:
#                     - letter
#                     - number
#                 pattern: {base_directory}/(.+)(\d+)\.csv

def test__get_instantiation_through_instantiate_class_from_config(basic_data_connector):
    data_objects = basic_data_connector._get_data_object_list()
    assert data_objects == []

def create_fake_data_frame():
    return pd.DataFrame({
        "x": range(10),
        "y": list("ABCDEFGHIJ"),
    })

def test__get_data_object_list():
    data_object_dict = {
        "pretend/path/A-100.csv" : create_fake_data_frame(),
        "pretend/path/A-101.csv" : create_fake_data_frame(),
        "pretend/directory/B-1.csv" : create_fake_data_frame(),
        "pretend/directory/B-2.csv" : create_fake_data_frame(),
    }

    my_data_connector = DictDataConnector(
        name="my_data_connector",
        data_object_dict=data_object_dict
    )

    # Peer into internals to make sure things have loaded properly
    data_objects = my_data_connector._get_data_object_list()
    assert data_objects == [
        "pretend/directory/B-1.csv",
        "pretend/directory/B-2.csv",
        "pretend/path/A-100.csv",
        "pretend/path/A-101.csv",
    ]

    with pytest.raises(ValueError):
        set(my_data_connector.get_unmatched_data_objects()) == data_object_dict.keys()


    my_data_connector.refresh_data_object_cache()

    # Since we don't have a Partitioner yet, all keys should be unmatched
    assert set(my_data_connector.get_unmatched_data_objects()) == data_object_dict.keys()

    my_data_connector.add_partitioner(
        "my_partitioner",
        yaml.load("""
class_name: RegexPartitioner
config_params:
    regex:
        group_names:
            - first_dir
            - second_dir
            - letter
            - number
        pattern: (.+)/(.+)/(.+)-(\\d+)\\.csv
        """, Loader=yaml.FullLoader)
    )
    my_data_connector._default_partitioner = "my_partitioner"
    
    my_data_connector.refresh_data_object_cache()

    assert set(my_data_connector.get_unmatched_data_objects()) == set([])

    # print(json.dumps(my_data_connector._cached_data_object_to_batch_definition_map, indent=2))