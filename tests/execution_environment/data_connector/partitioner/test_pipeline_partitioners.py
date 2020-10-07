import pytest
import great_expectations.exceptions.exceptions as ge_exceptions
import pandas as pd
from great_expectations.execution_environment.data_connector.data_connector import DataConnector
from great_expectations.execution_environment.data_connector.partitioner import Partitioner, PipelinePartitioner
from great_expectations.execution_environment.data_connector.partitioner.partition import Partition

def test_pipeline_partitioner():
    temp_data_connector = DataConnector(name="test")
    test_partitioner = PipelinePartitioner(name="test_pipeline_partitioner", data_connector=temp_data_connector)
    # properties
    assert test_partitioner.name == "test_pipeline_partitioner"
    assert test_partitioner.data_connector == temp_data_connector
    assert test_partitioner.sorters == None
    assert test_partitioner.allow_multipart_partitions == False
    assert test_partitioner.config_params == None

    # no sorters
    with pytest.raises(ge_exceptions.SorterError):
        test_partitioner.get_sorter("i_dont_exist")

    returned_partitions = test_partitioner.get_available_partitions(pipeline_data_asset_name="test_asset_0",
                                                                      pipeline_datasets=None)
    assert returned_partitions == []


    # test df
    d = {'col1': [1, 2], 'col2': [3, 4]}
    test_df = pd.DataFrame(data=d)
    pipeline_dataset = [{"partition_name": "test_asset_0", "data_reference": test_df}]
    returned_partitions = test_partitioner.get_available_partitions(pipeline_data_asset_name="test_asset_0",
                                                                    pipeline_datasets=pipeline_dataset)

    partition_to_compare = Partition(name="test_asset_0", data_asset_name="test_asset_0", definition={"test_asset_0": test_df}, data_reference=test_df)
    assert returned_partitions == [partition_to_compare]
