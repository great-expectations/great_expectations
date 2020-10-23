import yaml
from great_expectations.execution_environment.data_connector import (
    SinglePartitionerDictDataConnector,
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
    create_files_in_directory,
)


def test_basic_instantiation():
    data_reference_dict = {
        "path/A-100.csv" : create_fake_data_frame(),
        "path/A-101.csv" : create_fake_data_frame(),
        "directory/B-1.csv" : create_fake_data_frame(),
        "directory/B-2.csv" : create_fake_data_frame(),
    }

    my_data_connector = SinglePartitionerDictDataConnector(
        name="my_data_connector",
        execution_environment_name="FAKE_EXECUTION_ENVIRONMENT_NAME",
        partitioner={
            "class_name": "RegexPartitioner",
            "pattern": "(.*)/(.+)-(\\d+)\\.csv",
            "group_names": ["data_asset_name", "letter", "number"],
    },
        data_reference_dict = data_reference_dict,
    )

    my_data_connector.refresh_data_references_cache()
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
class_name: SinglePartitionerDictDataConnector
base_directory: my_base_directory/
execution_environment_name: FAKE_EXECUTION_ENVIRONMENT_NAME
    
partitioner:
    class_name: RegexPartitioner
    pattern: (\\d{4})/(\\d{2})/(.+)-\\d+\\.csv
    group_names:
        - year_dir
        - month_dir
        - data_asset_name
    """
    config = yaml.load(yaml_string, Loader=yaml.FullLoader)
    config["data_reference_dict"] = data_reference_dict
    my_data_connector = instantiate_class_from_config(
        config,
        config_defaults={"module_name": "great_expectations.execution_environment.data_connector"},
        runtime_environment={"name": "my_data_connector"},
    )

    my_data_connector.refresh_data_references_cache()
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
class_name: SinglePartitionerDictDataConnector
execution_environment_name: FAKE_EXECUTION_ENVIRONMENT_NAME
base_directory: my_base_directory/
# glob_directive: '*.csv'
partitioner:
    class_name: RegexPartitioner
    pattern: ^(.+)-(\\d{4})(\\d{2})\\.[csv|txt]$
    group_names:
        - data_asset_name
        - year_dir
        - month_dir

assets:
    alpha:
        base_directory: alpha/files/go/here/

    beta:
        base_directory: beta_here/
        # glob_directive: '*.txt'

    gamma:
        # glob_directive: '*.txt'

    """
    config = yaml.load(yaml_string, Loader=yaml.FullLoader)
    config["data_reference_dict"] = data_reference_dict
    my_data_connector = instantiate_class_from_config(
        config,
        config_defaults={"module_name": "great_expectations.execution_environment.data_connector"},
        runtime_environment={"name": "my_data_connector"},
    )

    my_data_connector.refresh_data_references_cache()

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


def test_test_yaml_config_(empty_data_context, tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("test_test_yaml_config"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list= [
            "2020/01/alpha-1001.csv",
            "2020/01/beta-1002.csv",
            "2020/02/alpha-1003.csv",
            "2020/02/beta-1004.csv",
            "2020/03/alpha-1005.csv",
            "2020/03/beta-1006.csv",
            "2020/04/beta-1007.csv",
        ]
    )

    return_object = empty_data_context.test_yaml_config(f"""
    module_name: great_expectations.execution_environment.data_connector
    class_name: SinglePartitionerFileDataConnector
    execution_environment_name: FAKE_EXECUTION_ENVIRONMENT
    name: TEST_DATA_CONNECTOR
    base_directory: {base_directory}/
    glob_directive: "*/*/*.csv"
    partitioner:
        class_name: RegexPartitioner
        config_params:
            regex:
                group_names:
                    - year_dir
                    - month_dir
                    - data_asset_name
                pattern: (\\d{{4}})/(\\d{{2}})/(.*)-.*\\.csv
        """, return_mode="return_object")

    assert return_object == {
        'class_name': 'SinglePartitionerFileDataConnector',
        'data_asset_count': 2,
        'example_data_asset_names': [
            'alpha',
            'beta'
        ],
        'assets': {
            'alpha': {
                'example_data_references': ['2020/01/alpha-*.csv', '2020/02/alpha-*.csv', '2020/03/alpha-*.csv'],
                'batch_definition_count': 3
            },
            'beta': {
                'example_data_references': ['2020/02/beta-*.csv', '2020/03/beta-*.csv', '2020/04/beta-*.csv'],
                'batch_definition_count': 4
            }
        },
        'example_unmatched_data_references' : [],
        'unmatched_data_reference_count': 0,
    }


def test_test_yaml_config_excluding_non_regex_matching_files(empty_data_context, tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("test_something_needs_a_better_name"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "2020/01/alpha-1001.csv",
            "2020/01/beta-1002.csv",
            "2020/02/alpha-1003.csv",
            "2020/02/beta-1004.csv",
            "2020/03/alpha-1005.csv",
            "2020/03/beta-1006.csv",
            "2020/04/beta-1007.csv",
            "gamma-202001.csv",
            "gamma-202002.csv",
        ]
    )

    # gamma-202001.csv and gamma-202002.csv do not match regex (which includes 2020/month directory).

    return_object = empty_data_context.test_yaml_config(f"""
module_name: great_expectations.execution_environment.data_connector
class_name: SinglePartitionerFileDataConnector
execution_environment_name: FAKE_EXECUTION_ENVIRONMENT
name: TEST_DATA_CONNECTOR

base_directory: {base_directory}/
glob_directive: "*/*/*.csv"

partitioner:
    class_name: RegexPartitioner
    pattern: (\\d{{4}})/(\\d{{2}})/(.*)-.*\\.csv
    group_names:
        - year_dir
        - month_dir
        - data_asset_name
    """, return_mode="return_object")

    assert return_object == {
        'class_name': 'SinglePartitionerFileDataConnector',
        'data_asset_count': 2,
        'example_data_asset_names': [
            'alpha',
            'beta'
        ],
        'assets': {
            'alpha': {
                'example_data_references': ['2020/01/alpha-*.csv', '2020/02/alpha-*.csv', '2020/03/alpha-*.csv'],
                'batch_definition_count': 3
            },
            'beta': {
                'example_data_references': ['2020/02/beta-*.csv', '2020/03/beta-*.csv', '2020/04/beta-*.csv'],
                'batch_definition_count': 4
            }
        },
        'example_unmatched_data_references' : [],
        'unmatched_data_reference_count': 0,
    }


def test_self_check():
    data_reference_dict = {
        "A-100.csv" : create_fake_data_frame(),
        "A-101.csv" : create_fake_data_frame(),
        "B-1.csv" : create_fake_data_frame(),
        "B-2.csv" : create_fake_data_frame(),
    }

    my_data_connector = SinglePartitionerDictDataConnector(
        name="my_data_connector",
        data_reference_dict=data_reference_dict,
        execution_environment_name="FAKE_EXECUTION_ENVIRONMENT",
        partitioner={
            "class_name": "RegexPartitioner",
            "pattern": "(.+)-(\\d+)\\.csv",
            "group_names": ["data_asset_name", "number"]
        }
    )

    self_check_return_object = my_data_connector.self_check()

    assert self_check_return_object == {
        'class_name': 'SinglePartitionerDictDataConnector',
        'data_asset_count': 2,
        'example_data_asset_names': [
            'A',
            'B'
        ],
        'assets': {
            'A': {
                'example_data_references': ['A-100.csv', 'A-101.csv'],
                'batch_definition_count': 2
            },
            'B': {
                'example_data_references': ['B-1.csv', 'B-2.csv'],
                'batch_definition_count': 2
            }
        },
        'example_unmatched_data_references' : [],
        'unmatched_data_reference_count': 0,
    }


def test_that_needs_a_better_name():
    data_reference_dict = {
        "A-100.csv": create_fake_data_frame(),
        "A-101.csv": create_fake_data_frame(),
        "B-1.csv": create_fake_data_frame(),
        "B-2.csv": create_fake_data_frame(),
        "CCC.csv": create_fake_data_frame(),
    }

    my_data_connector = SinglePartitionerDictDataConnector(
        name="my_data_connector",
        data_reference_dict=data_reference_dict,
        execution_environment_name="FAKE_EXECUTION_ENVIRONMENT",
        partitioner={
            "class_name": "RegexPartitioner",
            "pattern": "(.+)-(\\d+)\\.csv",
            "group_names": ["data_asset_name", "number"]
        }
    )

    self_check_return_object = my_data_connector.self_check()

    assert self_check_return_object == {
        'class_name': 'SinglePartitionerDictDataConnector',
        'data_asset_count': 2,
        'example_data_asset_names': [
            'A',
            'B'
        ],
        'assets': {
            'A': {
                'example_data_references': ['A-100.csv', 'A-101.csv'],
                'batch_definition_count': 2
            },
            'B': {
                'example_data_references': ['B-1.csv', 'B-2.csv'],
                'batch_definition_count': 2
            }
        },
        'example_unmatched_data_references': ['CCC.csv'],
        'unmatched_data_reference_count': 1,
    }
