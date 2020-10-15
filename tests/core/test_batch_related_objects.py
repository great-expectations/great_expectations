import datetime

from great_expectations.core.batch import (
    Batch,
    BatchRequest,
    BatchDefinition,
    BatchSpec,
    BatchMarkers,
)

def test_batch_definition_id():
    A = BatchDefinition(
        "A",
        "a",
        "aaa",
        {
            "id": "A"
        }
    )
    print(A.id)

    B = BatchDefinition(
        "B",
        "b",
        "bbb",
        {
            "id": "B"
        }
    )
    print(B.id)

    assert A.id != B.id

def test_batch_definition_equality():
    A = BatchDefinition(
        "A",
        "a",
        "aaa",
        {
            "id": "A"
        }
    )

    B = BatchDefinition(
        "B",
        "b",
        "bbb",
        {
            "id": "B"
        }
    )

    assert A != B

    A2 = BatchDefinition(
        "A",
        "a",
        "aaa",
        {
            "id": "A"
        }
    )

    assert A == A2

def test_batch__str__method():
    batch = Batch(
        data=None,
        batch_request=BatchRequest(
            execution_environment="my_execution_environment",
            data_connector="my_data_connector",
            data_asset_name="my_data_asset_name",
        ),
        batch_definition=BatchDefinition(
            execution_environment_name="my_execution_environment",
            data_connector_name="my_data_connector",
            data_asset_name="my_data_asset_name",
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
    "partition_definition": null
  },
  "batch_spec": "{'path': '/some/path/some.file'}",
  "batch_markers": "{'ge_load_time': 'FAKE_LOAD_TIME'}"
}"""



