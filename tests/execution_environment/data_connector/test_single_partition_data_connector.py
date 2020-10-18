import yaml

from great_expectations.execution_environment.data_connector import (
    SinglePartitionDictDataConnector,
)
from great_expectations.core.batch import (
    BatchRequest,
    BatchDefinition,
    PartitionDefinition,
)
from great_expectations.data_context.util import (
    instantiate_class_from_config
)
from tests.test_utils import (
    create_fake_data_frame,
)

def test_basic_instantiation(tmp_path_factory):
    data_reference_dict = {
        "path/A-100.csv" : create_fake_data_frame(),
        "path/A-101.csv" : create_fake_data_frame(),
        "directory/B-1.csv" : create_fake_data_frame(),
        "directory/B-2.csv" : create_fake_data_frame(),
    }

    my_data_connector = SinglePartitionDictDataConnector(
        name="my_data_connector",
        partitioner={
            "class_name": "RegexPartitioner",
            "config_params": {
                "regex": {
                    "group_names": ["data_asset_name", "letter","number"],
                    "pattern": "(.*)/(.+)-(\d+)\.csv"
                }
            }
        },
        data_reference_dict = data_reference_dict
    )

    my_data_connector.refresh_data_references_cache("FAKE_EXECUTION_ENVIRONMENT_NAME")
    assert my_data_connector.get_data_reference_list_count() == 4
    assert my_data_connector.get_unmatched_data_references() == []

    print(my_data_connector.get_batch_definition_list_from_batch_request(BatchRequest(
        execution_environment_name="something",
        data_connector_name="my_data_connector",
        data_asset_name="something",
    )))


def test_example_with_implicit_data_asset_names():
    data_reference_dict = dict([
        (data_reference, create_fake_data_frame)
        for data_reference in [
            "2020/01/alpha-1001.csv",
            "2020/01/beta-1002.csv",
            "2020/02/alpha-1003.csv",
            "2020/02/beta-1004.csv",
            "2020/03/alpha-1005.csv",
            "2020/03/beta-1006.csv",
            "2020/04/beta-1007.csv",
        ]
    ])

    yaml_string = """
class_name: SinglePartitionDictDataConnector
base_directory: my_base_directory/
    
partitioner:
    class_name: RegexPartitioner
    config_params:
        regex:
            group_names:
                - year_dir
                - month_dir
                - data_asset_name
            pattern: (\\d{4})/(\\d{2})/(.+)-\\d+\\.csv
    """
    config = yaml.load(yaml_string, Loader=yaml.FullLoader)
    config["data_reference_dict"] = data_reference_dict
    my_data_connector = instantiate_class_from_config(
        config,
        config_defaults={"module_name": "great_expectations.execution_environment.data_connector"},
        runtime_environment={"name": "my_data_connector"},
    )

    my_data_connector.refresh_data_references_cache(
        execution_environment_name="FAKE_EXECUTION_ENVIRONMENT_NAME"
    )
    assert len(my_data_connector.get_batch_definition_list_from_batch_request(BatchRequest(
        execution_environment_name="FAKE_EXECUTION_ENVIRONMENT_NAME",
        data_connector_name="my_data_connector",
        data_asset_name="alpha",
    ))) == 3
    assert len(my_data_connector.get_batch_definition_list_from_batch_request(BatchRequest(
        data_connector_name="my_data_connector",
        data_asset_name="alpha",
    ))) == 3
    assert len(my_data_connector.get_batch_definition_list_from_batch_request(BatchRequest(
        data_connector_name="my_data_connector",
        data_asset_name="beta",
    ))) == 4

    assert my_data_connector.get_batch_definition_list_from_batch_request(BatchRequest(
        execution_environment_name="FAKE_EXECUTION_ENVIRONMENT_NAME",
        data_connector_name="my_data_connector",
        data_asset_name="alpha",
        partition_request={
            "year_dir": "2020",
            "month_dir": "03",
        }
    )) == [BatchDefinition(
        execution_environment_name="FAKE_EXECUTION_ENVIRONMENT_NAME",
        data_connector_name="my_data_connector",
        data_asset_name="alpha",
        partition_definition=PartitionDefinition(
            year_dir="2020",
            month_dir="03",
        )
    )]

def test_example_with_explicit_data_asset_names(tmp_path_factory):
    data_reference_dict = dict([
        (data_reference, create_fake_data_frame)
        for data_reference in [
            "my_base_directory/alpha/files/go/here/alpha-202001.csv",
            "my_base_directory/alpha/files/go/here/alpha-202002.csv",
            "my_base_directory/alpha/files/go/here/alpha-202003.csv",
            "my_base_directory/beta_here/beta-202001.txt",
            "my_base_directory/beta_here/beta-202002.txt",
            "my_base_directory/beta_here/beta-202003.txt",
            "my_base_directory/beta_here/beta-202004.txt",
            "my_base_directory/gamma-202001.csv",
            "my_base_directory/gamma-202002.csv",
            "my_base_directory/gamma-202003.csv",
            "my_base_directory/gamma-202004.csv",
            "my_base_directory/gamma-202005.csv",
        ]
    ])

    yaml_string = """
class_name: SinglePartitionDictDataConnector
base_directory: my_base_directory/
# glob_directive: '*.csv'
    
partitioner:
    class_name: RegexPartitioner
    config_params:
        regex:
            group_names:
                - data_asset_name
                - year_dir
                - month_dir
            pattern: ^(.+)-(\\d{4})(\\d{2})\\.[csv|txt]$

data_assets:
    - alpha:
        directory: alpha/files/go/here/

    - beta:
        directory: beta_here/
        # glob_directive: '*.txt'

    - gamma:
        # glob_directive: '*.txt'

    """
    config = yaml.load(yaml_string, Loader=yaml.FullLoader)
    config["data_reference_dict"] = data_reference_dict
    my_data_connector = instantiate_class_from_config(
        config,
        config_defaults={"module_name": "great_expectations.execution_environment.data_connector"},
        runtime_environment={"name": "my_data_connector"},
    )

    my_data_connector.refresh_data_references_cache(
        execution_environment_name="FAKE_EXECUTION_ENVIRONMENT_NAME"
    )

    # FIXME: Abe 20201017 : These tests don't pass yet.
    # I'm starting to think we might want to separate out this behavior into a different class.
    assert len(my_data_connector.get_unmatched_data_references()) == 0
    assert len(my_data_connector.get_batch_definition_list_from_batch_request(BatchRequest(
        data_connector_name="my_data_connector",
        data_asset_name="alpha",
    ))) == 3
    assert len(my_data_connector.get_batch_definition_list_from_batch_request(BatchRequest(
        data_connector_name="my_data_connector",
        data_asset_name="beta",
    ))) == 4
    assert len(my_data_connector.get_batch_definition_list_from_batch_request(BatchRequest(
        data_connector_name="my_data_connector",
        data_asset_name="gamma",
    ))) == 5

    # print(my_data_connector.get_batch_definition_list_from_batch_request(BatchRequest(
    #     execution_environment_name="FAKE_EXECUTION_ENVIRONMENT_NAME",
    #     data_connector_name="my_data_connector",
    #     data_asset_name="alpha",
    # )))