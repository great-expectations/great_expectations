from typing import Any, Dict

import pytest

from great_expectations.core.batch import (
    BatchRequest,
    RuntimeBatchRequest,
    get_batch_request_from_acceptable_arguments,
)
from great_expectations.datasource.fluent import BatchRequest as FluentBatchRequest


@pytest.fixture()
def base_fluent() -> Dict[str, str]:
    return {"datasource_name": "ds", "data_asset_name": "da"}


@pytest.fixture()
def base_block(base_fluent: Dict[str, str]) -> Dict[str, str]:
    """Basic block-style batch request args"""
    result = base_fluent.copy()
    result["data_connector_name"] = "dc"
    return result


@pytest.fixture()
def runtime_base_block(base_block: Dict[str, Any]) -> Dict[str, str]:
    """Basic block-style batch request args and a runtime parameter"""
    result = base_block.copy()
    result["path"] = "p"
    return result


@pytest.mark.unit
def test_get_batch_request_from_acceptable_arguments_datasource_name_must_be_str():
    with pytest.raises(TypeError):
        get_batch_request_from_acceptable_arguments(5)


@pytest.mark.unit
def test_get_batch_request_from_acceptable_arguments_batch_request_passthrough():
    batch_request = FluentBatchRequest(datasource_name="ds", data_asset_name="da")

    actual = get_batch_request_from_acceptable_arguments(batch_request=batch_request)

    assert actual == batch_request


@pytest.mark.unit
def test_get_batch_request_from_acceptable_arguments_runtime_parameter_conflicts_raise(
    base_block: Dict[str, Any],
):
    with pytest.raises(ValueError) as ve:
        runtime = base_block.copy()
        runtime["batch_data"] = 1
        get_batch_request_from_acceptable_arguments(
            "ds", "dc", "da", runtime_parameters=runtime, batch_data=1
        )

    assert "runtime_parameters" in str(ve.value)

    with pytest.raises(ValueError) as ve:
        runtime = base_block.copy()
        runtime["query"] = "1"
        get_batch_request_from_acceptable_arguments(
            "ds", "dc", "da", runtime_parameters=runtime, query="1"
        )

    assert "runtime_parameters" in str(ve.value)

    with pytest.raises(ValueError) as ve:
        runtime = base_block.copy()
        runtime["path"] = "."
        get_batch_request_from_acceptable_arguments(
            "ds", "dc", "da", runtime_parameters=runtime, path="."
        )

    assert "runtime_parameters" in str(ve.value)


@pytest.mark.unit
@pytest.mark.parametrize("param,value", [("batch_data", "b"), ("query", "q"), ("path", "p")])
def test_get_batch_request_from_acceptable_arguments_runtime_parameter_path(
    base_block: Dict[str, Any], param, value
):
    """Setting any of the parameters should result in a runtime batch request"""
    base_block["batch_identifiers"] = {"a": "1"}
    base_block[param] = value
    actual = get_batch_request_from_acceptable_arguments(**base_block)
    assert isinstance(actual, RuntimeBatchRequest)
    assert actual.runtime_parameters is not None
    assert actual.runtime_parameters[param] == value

    # if runtime parameters are present with the same value, we should get an error
    with pytest.raises(ValueError):
        runtime_parameters = {param: value}
        base_block["runtime_parameters"] = runtime_parameters
        get_batch_request_from_acceptable_arguments(**base_block)


@pytest.mark.unit
def test_get_batch_request_from_acceptable_arguments_runtime_batch_identifiers_kwargs(
    runtime_base_block: Dict[str, Any],
):
    """Batch identifiers can be provided by batch identifiers or kwargs"""
    bids = {"a": 1, "b": 2}

    # testing batch identifiers as kwargs
    actual = get_batch_request_from_acceptable_arguments(
        datasource_name=runtime_base_block["datasource_name"],
        data_connector_name=runtime_base_block["data_connector_name"],
        data_asset_name=runtime_base_block["data_asset_name"],
        path=runtime_base_block["path"],
        a=1,
        b=2,
    )
    assert isinstance(actual, RuntimeBatchRequest)
    assert actual.batch_identifiers == bids


@pytest.mark.unit
def test_get_batch_request_from_acceptable_arguments_runtime_parameters_no_batch_identifiers(
    runtime_base_block: Dict[str, Any],
):
    """Batch identifiers can be provided by batch identifiers or kwargs"""
    # testing no batch identifiers
    with pytest.raises(ValueError):
        get_batch_request_from_acceptable_arguments(**runtime_base_block)


@pytest.mark.unit
def test_get_batch_request_from_acceptable_arguments_runtime_batch_identifiers(
    runtime_base_block: Dict[str, Any],
):
    """Batch identifiers can be provided by batch identifiers or kwargs"""
    bids = {"a": 1, "b": 2}

    # testing batch identifiers as batch_identifiers
    runtime_base_block["batch_identifiers"] = bids
    actual = get_batch_request_from_acceptable_arguments(**runtime_base_block)
    assert isinstance(actual, RuntimeBatchRequest)
    assert actual.batch_identifiers == bids


@pytest.mark.unit
def test_get_batch_request_from_acceptable_arguments_fluent(
    base_fluent: Dict[str, str],
):
    actual = get_batch_request_from_acceptable_arguments(
        datasource_name=base_fluent["datasource_name"],
        data_asset_name=base_fluent["data_asset_name"],
    )
    assert isinstance(actual, FluentBatchRequest)


@pytest.mark.unit
def test_get_batch_request_from_acceptable_arguments_fluent_with_options(
    base_fluent: Dict[str, str],
):
    actual = get_batch_request_from_acceptable_arguments(
        datasource_name=base_fluent["datasource_name"],
        data_asset_name=base_fluent["data_asset_name"],
    )
    assert isinstance(actual, FluentBatchRequest)


@pytest.mark.unit
def test_get_batch_request_from_acceptable_arguments_fluent_and_block_args_raises(
    base_fluent: Dict[str, str],
):
    with pytest.raises(ValueError) as ve:
        get_batch_request_from_acceptable_arguments(
            datasource_name=base_fluent["datasource_name"],
            data_asset_name=base_fluent["data_asset_name"],
            # Deliberately the wrong type: passing a block-style arg alongside
            # fluent args is the condition this test asserts raises.
            data_connector_query="q",  # type: ignore[arg-type]
        )

    assert "Fluent Batch Requests" in str(ve.value)


@pytest.mark.unit
def test_get_batch_request_from_acceptable_arguments_block(base_block: Dict[str, Any]):
    actual = get_batch_request_from_acceptable_arguments(**base_block)
    assert isinstance(actual, BatchRequest)


@pytest.mark.unit
def test_get_batch_request_from_acceptable_arguments_block_batch_filter_parameters(
    base_block: Dict[str, Any],
):
    filter_params = {"a": "1"}

    # filter params as kwargs
    actual = get_batch_request_from_acceptable_arguments(
        datasource_name=base_block["datasource_name"],
        data_connector_name=base_block["data_connector_name"],
        data_asset_name=base_block["data_asset_name"],
        a="1",
    )
    assert isinstance(actual, BatchRequest)
    assert actual.data_connector_query is not None
    assert actual.data_connector_query["batch_filter_parameters"] == filter_params

    # filter params as an argument yields the same result
    base_block["batch_filter_parameters"] = filter_params
    actual = get_batch_request_from_acceptable_arguments(**base_block)
    assert isinstance(actual, BatchRequest)
    assert actual.data_connector_query is not None
    assert actual.data_connector_query["batch_filter_parameters"] == filter_params

    # filter params and batch identifiers raise
    base_block["batch_identifiers"] = filter_params
    with pytest.raises(ValueError):
        actual = get_batch_request_from_acceptable_arguments(**base_block)


@pytest.mark.unit
def test_get_batch_request_from_acceptable_arguments_block_data_connector_query(
    base_block: Dict[str, Any],
):
    query = {"a": "1"}  # any old dict can be passed

    # filter params as kwargs
    base_block["data_connector_query"] = query
    actual = get_batch_request_from_acceptable_arguments(**base_block)
    assert isinstance(actual, BatchRequest)
    assert actual.data_connector_query == query


@pytest.mark.unit
def test_get_batch_request_from_acceptable_arguments_block_partitioner_sampler_batch_spec_passthrough(  # noqa: E501 # FIXME CoP
    base_block: Dict[str, Any],
):
    # partitioner and sampling as batch_spec_passthrough
    base_block["sampling_method"] = "sample"
    base_block["sampling_kwargs"] = {"a": "1"}
    base_block["partitioner_method"] = "partition"
    base_block["partitioner_kwargs"] = {"b": "2"}
    actual = get_batch_request_from_acceptable_arguments(**base_block)

    assert isinstance(actual, BatchRequest)
    assert actual.batch_spec_passthrough is not None
    assert actual.batch_spec_passthrough["sampling_method"] == "sample"
    assert actual.batch_spec_passthrough["sampling_kwargs"] == {"a": "1"}
    assert actual.batch_spec_passthrough["partitioner_method"] == "partition"
    assert actual.batch_spec_passthrough["partitioner_kwargs"] == {"b": "2"}

    # existing batch_spec_passthrough should be preserved, no partitioner or sampling args exist
    base_block["batch_spec_passthrough"] = {"c": "3"}
    actual = get_batch_request_from_acceptable_arguments(**base_block)

    assert isinstance(actual, BatchRequest)
    assert actual.batch_spec_passthrough is not None
    assert actual.batch_spec_passthrough["c"] == "3"
    assert "sampling_method" not in actual.batch_spec_passthrough
    assert "sampling_kwargs" not in actual.batch_spec_passthrough
    assert "partitioner_method" not in actual.batch_spec_passthrough
    assert "partitioner_kwargs" not in actual.batch_spec_passthrough
    assert isinstance(actual, BatchRequest)


@pytest.mark.unit
def test_batch_request_hash_consistency_with_equality():
    batch1 = BatchRequest(
        datasource_name="test_datasource",
        data_connector_name="test_connector",
        data_asset_name="test_asset",
    )
    batch2 = BatchRequest(
        datasource_name="test_datasource",
        data_connector_name="test_connector",
        data_asset_name="test_asset",
    )

    assert batch1 == batch2
    assert hash(batch1) == hash(batch2)


@pytest.mark.unit
def test_batch_request_hash_different_for_different_attributes():
    batch1 = BatchRequest(
        datasource_name="test_datasource_1",
        data_connector_name="test_connector",
        data_asset_name="test_asset",
    )
    batch2 = BatchRequest(
        datasource_name="test_datasource_2",
        data_connector_name="test_connector",
        data_asset_name="test_asset",
    )

    assert batch1 != batch2
    assert hash(batch1) != hash(batch2)


@pytest.mark.unit
def test_batch_request_hash_stable_across_runs():
    batch = BatchRequest(
        datasource_name="test_datasource",
        data_connector_name="test_connector",
        data_asset_name="test_asset",
    )

    hash1 = hash(batch)
    hash2 = hash(batch)
    hash3 = hash(batch)

    assert hash1 == hash2 == hash3


@pytest.mark.unit
def test_runtime_batch_request_hash_consistency_with_equality():
    batch1 = RuntimeBatchRequest(
        datasource_name="test_datasource",
        data_connector_name="test_connector",
        data_asset_name="test_asset",
        runtime_parameters={"path": "test_path"},
        batch_identifiers={"test": "identifier"},
    )
    batch2 = RuntimeBatchRequest(
        datasource_name="test_datasource",
        data_connector_name="test_connector",
        data_asset_name="test_asset",
        runtime_parameters={"path": "test_path"},
        batch_identifiers={"test": "identifier"},
    )

    assert batch1 == batch2
    assert hash(batch1) == hash(batch2)


@pytest.mark.unit
def test_runtime_batch_request_hash_different_for_different_runtime_parameters():
    batch1 = RuntimeBatchRequest(
        datasource_name="test_datasource",
        data_connector_name="test_connector",
        data_asset_name="test_asset",
        runtime_parameters={"path": "test_path_1"},
        batch_identifiers={"test": "identifier"},
    )
    batch2 = RuntimeBatchRequest(
        datasource_name="test_datasource",
        data_connector_name="test_connector",
        data_asset_name="test_asset",
        runtime_parameters={"path": "test_path_2"},
        batch_identifiers={"test": "identifier"},
    )

    assert batch1 != batch2
    assert hash(batch1) != hash(batch2)


@pytest.mark.unit
def test_runtime_batch_request_hash_stable_across_runs():
    batch = RuntimeBatchRequest(
        datasource_name="test_datasource",
        data_connector_name="test_connector",
        data_asset_name="test_asset",
        runtime_parameters={"path": "test_path"},
        batch_identifiers={"test": "identifier"},
    )

    hash1 = hash(batch)
    hash2 = hash(batch)
    hash3 = hash(batch)

    assert hash1 == hash2 == hash3
