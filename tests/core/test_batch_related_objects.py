import pytest

from great_expectations.core.batch import (
    Batch,
    BatchRequest,
    BatchDefinition,
    BatchSpec,
    BatchMarkers,
    PartitionRequest,
    PartitionDefinition,
)

def test_batch_definition_id():
    A = BatchDefinition(
        "A",
        "a",
        "aaa",
        PartitionDefinition({
            "id": "A"
        })
    )
    print(A.id)

    B = BatchDefinition(
        "B",
        "b",
        "bbb",
        PartitionDefinition({
            "id": "B"
        })
    )
    print(B.id)

    assert A.id != B.id

def test_batch_definition_instantiation():
    with pytest.raises(TypeError):
        A = BatchDefinition(
            "A",
            "a",
            "aaa",
            {
                "id": "A"
            }
        )

    A = BatchDefinition(
        "A",
        "a",
        "aaa",
        PartitionDefinition({
            "id": "A"
        })
    )

    print(A.id)


def test_batch_definition_equality():
    A = BatchDefinition(
        "A",
        "a",
        "aaa",
        PartitionDefinition({
            "id": "A"
        })
    )

    B = BatchDefinition(
        "B",
        "b",
        "bbb",
        PartitionDefinition({
            "id": "B"
        })
    )

    assert A != B

    A2 = BatchDefinition(
        "A",
        "a",
        "aaa",
        PartitionDefinition({
            "id": "A"
        })
    )

    assert A == A2

def test_batch__str__method():
    batch = Batch(
        data=None,
        batch_request=BatchRequest(
            execution_environment_name="my_execution_environment",
            data_connector_name="my_data_connector",
            data_asset_name="my_data_asset_name",
        ),
        batch_definition=BatchDefinition(
            execution_environment_name="my_execution_environment",
            data_connector_name="my_data_connector",
            data_asset_name="my_data_asset_name",
            partition_definition=PartitionDefinition({}),
        ),
        batch_spec=BatchSpec(
            path="/some/path/some.file"
        ),
        batch_markers=BatchMarkers(
            ge_load_time="FAKE_LOAD_TIME"
        ),
    )
    print(batch.__str__())

    assert batch.__str__() == """{
  "data": "None",
  "batch_request": {
    "execution_environment_name": "my_execution_environment",
    "data_connector_name": "my_data_connector",
    "data_asset_name": "my_data_asset_name",
    "partition_request": null
  },
  "batch_definition": {
    "execution_environment_name": "my_execution_environment",
    "data_connector_name": "my_data_connector",
    "data_asset_name": "my_data_asset_name",
    "partition_definition": {}
  },
  "batch_spec": "{'path': '/some/path/some.file'}",
  "batch_markers": "{'ge_load_time': 'FAKE_LOAD_TIME'}"
}"""


def test_batch_request_instantiation():
    BatchRequest(
        execution_environment_name="A",
        data_connector_name="a",
        data_asset_name="aaa",
        partition_request={
            "id": "A"
        }
    )

    BatchRequest(
        "A",
        "a",
        "aaa",
        {
            "id": "A"
        }
    )

    # with pytest.raises(TypeError):
    #     BatchRequest(
    #         "A",
    #         "a",
    #         "aaa",
    #         PartitionDefinition({
    #             "id": "A"
    #         })
    #     )

    BatchRequest(
        data_connector_name="a",
        data_asset_name="aaa",
        partition_request={
            "id": "A"
        }
    )

    BatchRequest(
        data_asset_name="aaa",
        partition_request={
            "id": "A"
        }
    )

    BatchRequest(
        partition_request={
            "id": "A"
        }
    )

    BatchRequest(
        execution_environment_name="A",
        data_connector_name="a",
        data_asset_name="aaa",
    )
