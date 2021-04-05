import pandas as pd
import pytest

from great_expectations.core.batch import (
    Batch,
    BatchDefinition,
    BatchMarkers,
    BatchRequest,
    BatchSpec,
    IDDict,
)
from great_expectations.core.batch_spec import RuntimeDataBatchSpec
from great_expectations.exceptions import InvalidBatchSpecError


def test_batch_definition_id():
    # noinspection PyUnusedLocal,PyPep8Naming
    A = BatchDefinition("A", "a", "aaa", batch_identifiers=IDDict({"id": "A"}))
    print(A.id)

    # noinspection PyUnusedLocal,PyPep8Naming
    B = BatchDefinition("B", "b", "bbb", batch_identifiers=IDDict({"id": "B"}))
    print(B.id)

    assert A.id != B.id


def test_batch_definition_instantiation():
    with pytest.raises(TypeError):
        # noinspection PyTypeChecker,PyUnusedLocal,PyPep8Naming
        A = BatchDefinition("A", "a", "aaa", {"id": "A"})

    A = BatchDefinition("A", "a", "aaa", batch_identifiers=IDDict({"id": "A"}))

    print(A.id)


def test_batch_definition_equality():
    # noinspection PyUnusedLocal,PyPep8Naming
    A = BatchDefinition("A", "a", "aaa", batch_identifiers=IDDict({"id": "A"}))

    # noinspection PyUnusedLocal,PyPep8Naming
    B = BatchDefinition("B", "b", "bbb", batch_identifiers=IDDict({"id": "B"}))

    assert A != B

    # noinspection PyUnusedLocal,PyPep8Naming
    A2 = BatchDefinition("A", "a", "aaa", batch_identifiers=IDDict({"id": "A"}))

    assert A == A2


def test_batch__str__method():
    batch = Batch(
        data=None,
        batch_request=BatchRequest(
            datasource_name="my_datasource",
            data_connector_name="my_data_connector",
            data_asset_name="my_data_asset_name",
        ),
        batch_definition=BatchDefinition(
            datasource_name="my_datasource",
            data_connector_name="my_data_connector",
            data_asset_name="my_data_asset_name",
            batch_identifiers=IDDict({}),
        ),
        batch_spec=BatchSpec(path="/some/path/some.file"),
        batch_markers=BatchMarkers(ge_load_time="FAKE_LOAD_TIME"),
    )
    print(batch.__str__())

    assert (
        batch.__str__()
        == """{
  "data": "None",
  "batch_request": {
    "datasource_name": "my_datasource",
    "data_connector_name": "my_data_connector",
    "data_asset_name": "my_data_asset_name",
    "data_connector_query": null
  },
  "batch_definition": {
    "datasource_name": "my_datasource",
    "data_connector_name": "my_data_connector",
    "data_asset_name": "my_data_asset_name",
    "batch_identifiers": {}
  },
  "batch_spec": "{'path': '/some/path/some.file'}",
  "batch_markers": "{'ge_load_time': 'FAKE_LOAD_TIME'}"
}"""
    )


def test_batch_request_instantiation():
    BatchRequest(
        datasource_name="A",
        data_connector_name="a",
        data_asset_name="aaa",
        data_connector_query={"id": "A"},
    )

    BatchRequest("A", "a", "aaa", {"id": "A"})

    # with pytest.raises(TypeError):
    #     BatchRequest(
    #         "A",
    #         "a",
    #         "aaa",
    #         IDDict({
    #             "id": "A"
    #         })
    #     )

    # No data_source_name specified
    with pytest.raises(TypeError):
        BatchRequest(
            data_connector_name="a",
            data_asset_name="aaa",
            data_connector_query={"id": "A"},
        )

    # No data_source_name and data_connector_name specified
    with pytest.raises(TypeError):
        BatchRequest(data_asset_name="aaa", data_connector_query={"id": "A"})

    # No data_source_name and data_connector_name and data_asset_name specified
    with pytest.raises(TypeError):
        BatchRequest(data_connector_query={"id": "A"})

    BatchRequest(datasource_name="A", data_connector_name="a", data_asset_name="aaa")


def test_RuntimeDataBatchSpec():
    with pytest.raises(InvalidBatchSpecError):
        RuntimeDataBatchSpec()

    RuntimeDataBatchSpec({"batch_data": pd.DataFrame({"x": range(10)})})

    RuntimeDataBatchSpec(
        batch_data="we don't check types yet",
    )

    RuntimeDataBatchSpec(
        {
            "batch_data": "we don't check types yet",
        }
    )
