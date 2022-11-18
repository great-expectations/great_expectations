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
from great_expectations.core.id_dict import deep_convert_properties_iterable_to_id_dict
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.exceptions import InvalidBatchSpecError


@pytest.mark.unit
def test_id_dict_structure():
    data: dict = {
        "a0": 1,
        "a1": {
            "b0": "2",
            "b1": [
                "c0",
                (
                    "d0",
                    {
                        "e0": 3,
                        "e1": 4,
                    },
                    {"f0", "f1", "f2"},
                ),
            ],
            "b2": 5,
        },
    }
    nested_id_dictionary: IDDict = deep_convert_properties_iterable_to_id_dict(
        source=data
    )
    assert isinstance(nested_id_dictionary, IDDict)
    assert isinstance(nested_id_dictionary["a0"], int)
    assert isinstance(nested_id_dictionary["a1"], IDDict)
    assert isinstance(nested_id_dictionary["a1"]["b0"], str)
    assert isinstance(nested_id_dictionary["a1"]["b1"], list)
    assert isinstance(nested_id_dictionary["a1"]["b1"][0], str)
    assert isinstance(nested_id_dictionary["a1"]["b1"][1], tuple)
    assert isinstance(list(nested_id_dictionary["a1"]["b1"][1])[0], str)
    assert isinstance(list(nested_id_dictionary["a1"]["b1"][1])[1], IDDict)
    assert isinstance(list(nested_id_dictionary["a1"]["b1"][1])[2], set)
    assert isinstance(nested_id_dictionary["a1"]["b2"], int)


@pytest.mark.unit
def test_iddict_is_hashable():
    data_0: dict = {
        "a0": 1,
        "a1": {
            "b0": "2",
            "b1": [
                "c0",
                (
                    "d0",
                    {
                        "e0": 3,
                        "e1": 4,
                    },
                    convert_to_json_serializable(data={"f0", "f1", "f2"}),
                ),
            ],
            "b2": 5,
        },
    }
    data_1: dict = {
        "a0": 0,
        "a1": 1,
    }
    data_2: dict = {
        "b0": 2,
        "b1": 3,
    }
    data_3: dict = {
        "c0": "4",
        "c1": "5",
    }
    # noinspection PyBroadException,PyUnusedLocal
    try:
        # noinspection PyUnusedLocal
        dictionaries_as_set: set = {
            deep_convert_properties_iterable_to_id_dict(source=data_0),
            deep_convert_properties_iterable_to_id_dict(source=data_1),
            deep_convert_properties_iterable_to_id_dict(source=data_2),
            deep_convert_properties_iterable_to_id_dict(source=data_3),
        }
    except Exception as e:
        assert False, "IDDict.__hash__() failed."


@pytest.mark.unit
def test_batch_definition_id():
    # noinspection PyUnusedLocal,PyPep8Naming
    A = BatchDefinition("A", "a", "aaa", batch_identifiers=IDDict({"id": "A"}))
    print(A.id)

    # noinspection PyUnusedLocal,PyPep8Naming
    B = BatchDefinition("B", "b", "bbb", batch_identifiers=IDDict({"id": "B"}))
    print(B.id)

    assert A.id != B.id


@pytest.mark.unit
def test_batch_definition_instantiation():
    with pytest.raises(TypeError):
        # noinspection PyTypeChecker,PyUnusedLocal,PyPep8Naming
        A = BatchDefinition("A", "a", "aaa", {"id": "A"})

    # noinspection PyPep8Naming
    A = BatchDefinition("A", "a", "aaa", batch_identifiers=IDDict({"id": "A"}))

    print(A.id)


@pytest.mark.unit
def test_batch_definition_equality():
    # noinspection PyUnusedLocal,PyPep8Naming
    A = BatchDefinition("A", "a", "aaa", batch_identifiers=IDDict({"id": "A"}))

    # noinspection PyUnusedLocal,PyPep8Naming
    B = BatchDefinition("B", "b", "bbb", batch_identifiers=IDDict({"id": "B"}))

    assert A != B

    # noinspection PyUnusedLocal,PyPep8Naming
    A2 = BatchDefinition("A", "a", "aaa", batch_identifiers=IDDict({"id": "A"}))

    assert A == A2


@pytest.mark.unit
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
    "data_asset_name": "my_data_asset_name"
  },
  "batch_definition": {
    "datasource_name": "my_datasource",
    "data_connector_name": "my_data_connector",
    "data_asset_name": "my_data_asset_name",
    "batch_identifiers": {}
  },
  "batch_spec": {
    "path": "/some/path/some.file"
  },
  "batch_markers": {
    "ge_load_time": "FAKE_LOAD_TIME"
  }
}"""
    )


@pytest.mark.unit
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
        # noinspection PyArgumentList
        BatchRequest(
            data_connector_name="a",
            data_asset_name="aaa",
            data_connector_query={"id": "A"},
        )

    # No data_source_name and data_connector_name specified
    with pytest.raises(TypeError):
        # noinspection PyArgumentList
        BatchRequest(data_asset_name="aaa", data_connector_query={"id": "A"})

    # No data_source_name and data_connector_name and data_asset_name specified
    with pytest.raises(TypeError):
        # noinspection PyArgumentList
        BatchRequest(data_connector_query={"id": "A"})

    BatchRequest(datasource_name="A", data_connector_name="a", data_asset_name="aaa")


# noinspection PyPep8Naming
@pytest.mark.unit
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
