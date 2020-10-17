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
from tests.test_utils import (
    create_files_in_directory,
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
    data_references = basic_data_connector._get_data_reference_list()
    assert data_references == []

def create_fake_data_frame():
    return pd.DataFrame({
        "x": range(10),
        "y": list("ABCDEFGHIJ"),
    })

def test__DictDataConnector():
    data_reference_dict = {
        "pretend/path/A-100.csv" : create_fake_data_frame(),
        "pretend/path/A-101.csv" : create_fake_data_frame(),
        "pretend/directory/B-1.csv" : create_fake_data_frame(),
        "pretend/directory/B-2.csv" : create_fake_data_frame(),
    }

    my_data_connector = DictDataConnector(
        name="my_data_connector",
        data_reference_dict=data_reference_dict
    )

    # Peer into internals to make sure things have loaded properly
    data_references = my_data_connector._get_data_reference_list()
    assert data_references == [
        "pretend/directory/B-1.csv",
        "pretend/directory/B-2.csv",
        "pretend/path/A-100.csv",
        "pretend/path/A-101.csv",
    ]

    with pytest.raises(ValueError):
        set(my_data_connector.get_unmatched_data_references()) == data_reference_dict.keys()


    my_data_connector.refresh_data_reference_cache()

    # Since we don't have a Partitioner yet, all keys should be unmatched
    assert set(my_data_connector.get_unmatched_data_references()) == data_reference_dict.keys()

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
    
    my_data_connector.refresh_data_reference_cache()

    assert set(my_data_connector.get_unmatched_data_references()) == set([])

    # print(json.dumps(my_data_connector._cached_data_reference_to_batch_definition_map, indent=2))

def test__file_object_caching_for_FileDataConnector(tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("basic_data_connector__filesystem_data_connector"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list= [
            "pretend/path/A-100.csv",
            "pretend/path/A-101.csv",
            "pretend/directory/B-1.csv",
            "pretend/directory/B-2.csv",            
        ]
    )

    my_data_connector = FilesDataConnector(
        name="my_data_connector",
        base_directory=base_directory,
        glob_directive='*/*/*.csv',
    )

    # assert my_data_connector.get_data_reference_list_count() == 0
    # with pytest.raises(ValueError):
    #     set(my_data_connector.get_unmatched_data_references()) == data_reference_dict.keys()

    my_data_connector.refresh_data_reference_cache()

    # Since we don't have a Partitioner yet, all keys should be unmatched
    assert len(my_data_connector.get_unmatched_data_references()) == 4

    my_data_connector.add_partitioner(
        "my_first_partitioner",
        yaml.load("""
class_name: RegexPartitioner
config_params:
    regex:
        group_names:
            - letter
            - number
        pattern: pretend/path/(.+)-(\\d+)\\.csv
        """, Loader=yaml.FullLoader)
    )
    my_data_connector._default_partitioner = "my_first_partitioner"

    my_data_connector.refresh_data_reference_cache()

    assert len(my_data_connector.get_unmatched_data_references()) == 2

    my_data_connector.add_partitioner(
        "my_second_partitioner",
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
    my_data_connector._default_partitioner = "my_second_partitioner"
    
    my_data_connector.refresh_data_reference_cache()

    assert set(my_data_connector.get_unmatched_data_references()) == set([])

    print(my_data_connector._cached_data_reference_to_batch_definition_map)


def test_get_batch_definition_list_from_batch_request():
    pass

def test_build_batch_spec_from_batch_definition():
    pass

def test_get_batch_data_and_metadata_from_batch_definition():
    pass

def test_convert_in_memory_dataset_to_batch():
    pass

def test_refresh_data_reference_cache():
    pass

def test_get_unmatched_data_references():
    pass

def test_get_cached_data_reference_count():
    pass

def test_available_data_asset_names():
    pass
