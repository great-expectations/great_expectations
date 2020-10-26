import pytest
import pandas as pd

from great_expectations.execution_environment.data_connector.partitioner import PipelinePartitioner
from great_expectations.core.id_dict import PartitionDefinition
from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
import great_expectations.exceptions.exceptions as ge_exceptions


# TODO: <Alex>Partitioner.find_or_create_partitions() has been deprecated.  We must develop a test for an equivalent functionality (e.g., "get_batch_list_from_batch_request()").</Alex>
def test_pipeline_partitioner():
    test_partitioner = PipelinePartitioner(name="test_pipeline_partitioner")
    # properties
    assert test_partitioner.name == "test_pipeline_partitioner"
    assert test_partitioner.sorters is None
    assert not test_partitioner.allow_multipart_partitions
    # no sorters
    with pytest.raises(ge_exceptions.SorterError):
        test_partitioner.get_sorter("i_dont_exist")

    partition_config: dict = {
        "name": "my_test_partition",
        "data_asset_name": "test_asset_0",
        "definition": {},
        "data_reference": None
    }
    # no pipeline_datasets configured, so no partitions returned
    returned_partitions = test_partitioner.find_or_create_partitions(
        data_asset_name=None,
        partition_request=None,
        runtime_parameters=None,
        repartition=False,
        partition_config=partition_config
    )

    assert returned_partitions == []


# TODO: <Alex>Partitioner.find_or_create_partitions() has been deprecated.  We must develop a test for an equivalent functionality (e.g., "get_batch_list_from_batch_request()").</Alex>
def test_pipeline_partitioner_single_df():
    test_partitioner = PipelinePartitioner(name="test_pipeline_partitioner")
    # test df
    d = {'col1': [1, 2], 'col2': [3, 4]}
    test_df = pd.DataFrame(data=d)

    partition_config: dict = {
        "name": "partition_1",
        "data_asset_name": "test_asset_0",
        "definition": {"run_id": 1234567890},
        "data_reference": test_df
    }
    returned_partitions = test_partitioner.find_or_create_partitions(
        data_asset_name=None,
        partition_request=None,
        runtime_parameters=None,
        repartition=False,
        partition_config=partition_config
    )

    expected_partition = Partition(
        name="partition_1",
        data_asset_name="test_asset_0",
        definition=PartitionDefinition({"run_id": 1234567890}),
        data_reference=test_df
    )

    assert returned_partitions == [expected_partition]
