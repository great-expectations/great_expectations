from great_expectations.execution_environment.data_connector import (
    SinglePartitionDictDataConnector,
)
from great_expectations.core.batch import (
    BatchRequest,
)
from tests.test_utils import (
    create_fake_data_frame,
)

def test_basic_instantiation(tmp_path_factory):
    data_reference_dict = {
        "pretend/path/A-100.csv" : create_fake_data_frame(),
        "pretend/path/A-101.csv" : create_fake_data_frame(),
        "pretend/directory/B-1.csv" : create_fake_data_frame(),
        "pretend/directory/B-2.csv" : create_fake_data_frame(),
    }

    my_data_connector = SinglePartitionDictDataConnector(
        name="my_data_connector",
        partitioner={
            "class_name": "RegexPartitioner",
            "config_params": {
                "regex": {
                    "group_names": ["letter","number"],
                    "pattern": "(.+)(\d+)\.csv"
                }
            }
        },
        data_reference_dict = data_reference_dict
    )

    my_data_connector.refresh_data_reference_cache()
    assert my_data_connector.get_unmatched_data_references() == []
    assert my_data_connector.get_data_reference_list_count() == 4

    print(my_data_connector.get_batch_definition_list_from_batch_request(BatchRequest(
        execution_environment_name="something",
        data_connector_name="my_data_connector",
        data_asset_name="something",
    )))    
