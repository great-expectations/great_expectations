import pytest
import great_expectations.exceptions.exceptions as ge_exceptions
import pandas as pd
from great_expectations.data_context import DataContext
from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
from great_expectations.execution_environment.data_connector.partitioner import Partitioner, PipelinePartitioner
from great_expectations.marshmallow__shade.exceptions import ValidationError

def test_pipeline_partitioner_instantiation(
        execution_environment_pipeline_data_connector_pipeline_partitioner_data_context
):
    # simple set up from fixture
    execution_environment_name: str = "test_execution_environment"
    data_connector_name: str = "test_pipeline_data_connector"
    data_context: DataContext = \
        execution_environment_pipeline_data_connector_pipeline_partitioner_data_context
    execution_environment = data_context.get_execution_environment(execution_environment_name)

    data_connector = execution_environment.get_data_connector(data_connector_name)

    test_df = data_connector.in_memory_dataset
    test_partitioner = PipelinePartitioner(name="test_pipeline_partitioner", data_connector=data_connector)
    # properties
    assert test_partitioner.name == "test_pipeline_partitioner"
    assert test_partitioner.data_connector == data_connector
    assert test_partitioner.sorters == None
    assert test_partitioner.allow_multipart_partitions == False
    assert test_partitioner.config_params == None

    # no sorters
    with pytest.raises(ge_exceptions.SorterError):
        test_partitioner.get_sorter("i_dont_exist")

    # <WILL> : should this be a little bit more descriptive in how it complains?
    # PipelineDataConnector does not require `data_asset_name` it requires `pipeline_data_asset_name
    # PipelineDataConnector : requires an in_memory_dataset:
    #test_partitioner.get_sorted_partitions(pipe)
    assert test_partitioner.get_available_partitions(pipeline_data_asset_name="") == []

    #configure pipeline_dataset
    # test dataframe
    d = {'col1': [1, 2], 'col2': [3, 4]}
    test_df = pd.DataFrame(data=d)

    # <WILL> This is going to be a design question :
    # HI S

    """
    self =    col1  col2
    0     1     3
    1     2     4
        def __hash__(self):
            raise TypeError(
    >           f"{repr(type(self).__name__)} objects are mutable, "
                f"thus they cannot be hashed"
            )
    E       TypeError: 'DataFrame' objects are mutable, thus they cannot be hashed
    """
    pipeline_dataset = [{"partition_name": "test_asset_0", "data_reference": test_df}]
    print(test_partitioner.get_available_partitions(pipeline_data_asset_name="test_asset_0", pipeline_datasets=pipeline_dataset))
