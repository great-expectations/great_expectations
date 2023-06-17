from typing import Dict

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
def runtime_base_block(base_block: Dict[str, str]) -> Dict[str, str]:
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
    base_block: Dict[str, str]
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
@pytest.mark.parametrize(
    "param,value", [("batch_data", "b"), ("query", "q"), ("path", "p")]
)
def test_get_batch_request_from_acceptable_arguments_runtime_parameter_path(
    base_block: Dict[str, str], param, value
):
    """Setting any of the parameters should result in a runtime batch request"""
    base_block["batch_identifiers"] = {"a": "1"}
    base_block[param] = value
    actual = get_batch_request_from_acceptable_arguments(**base_block)
    assert actual.runtime_parameters[param] == value
    assert isinstance(actual, RuntimeBatchRequest)

    # if runtime parameters are present with the same value, we should get an error
    with pytest.raises(ValueError):
        runtime_parameters = {param, value}
        base_block["runtime_parameters"] = runtime_parameters
        get_batch_request_from_acceptable_arguments(**base_block)


@pytest.mark.unit
def test_get_batch_request_from_acceptable_arguments_runtime_batch_identifiers_kwargs(
    runtime_base_block: Dict[str, str]
):
    """Batch identifiers can be provided by batch identifiers or kwargs"""
    bids = {"a": 1, "b": 2}

    # testing batch identifiers as kwargs
    actual = get_batch_request_from_acceptable_arguments(**runtime_base_block, **bids)
    assert actual.batch_identifiers == bids
    assert isinstance(actual, RuntimeBatchRequest)


@pytest.mark.unit
def test_get_batch_request_from_acceptable_arguments_runtime_parameters_no_batch_identifiers(
    runtime_base_block: Dict[str, str]
):
    """Batch identifiers can be provided by batch identifiers or kwargs"""
    # testing no batch identifiers
    with pytest.raises(ValueError):
        get_batch_request_from_acceptable_arguments(**runtime_base_block)


@pytest.mark.unit
def test_get_batch_request_from_acceptable_arguments_runtime_batch_identifiers(
    runtime_base_block: Dict[str, str]
):
    """Batch identifiers can be provided by batch identifiers or kwargs"""
    bids = {"a": 1, "b": 2}

    # testing batch identifiers as batch_identifiers
    runtime_base_block["batch_identifiers"] = bids
    actual = get_batch_request_from_acceptable_arguments(**runtime_base_block)
    assert actual.batch_identifiers == bids
    assert isinstance(actual, RuntimeBatchRequest)


@pytest.mark.unit
def test_get_batch_request_from_acceptable_arguments_fluent(
    base_fluent: Dict[str, str]
):
    actual = get_batch_request_from_acceptable_arguments(**base_fluent)
    assert isinstance(actual, FluentBatchRequest)


@pytest.mark.unit
def test_get_batch_request_from_acceptable_arguments_fluent_with_options(
    base_fluent: Dict[str, str]
):
    actual = get_batch_request_from_acceptable_arguments(**base_fluent)
    assert isinstance(actual, FluentBatchRequest)


@pytest.mark.unit
def test_get_batch_request_from_acceptable_arguments_fluent_and_block_args_raises(
    base_fluent: Dict[str, str]
):
    base_fluent["data_connector_query"] = "q"

    with pytest.raises(ValueError) as ve:
        get_batch_request_from_acceptable_arguments(**base_fluent)

    assert "Fluent Batch Requests" in str(ve.value)


@pytest.mark.unit
def test_get_batch_request_from_acceptable_arguments_block(base_block: Dict[str, str]):
    actual = get_batch_request_from_acceptable_arguments(**base_block)
    assert isinstance(actual, BatchRequest)


@pytest.mark.unit
def test_get_batch_request_from_acceptable_arguments_block_batch_filter_parameters(
    base_block: Dict[str, str],
):
    filter_params = {"a": "1"}

    # filter params as kwargs
    actual = get_batch_request_from_acceptable_arguments(**base_block, **filter_params)
    assert actual.data_connector_query["batch_filter_parameters"] == filter_params
    assert isinstance(actual, BatchRequest)

    # filter params as an argument yields the same result
    base_block["batch_filter_parameters"] = filter_params
    actual = get_batch_request_from_acceptable_arguments(**base_block)
    assert actual.data_connector_query["batch_filter_parameters"] == filter_params
    assert isinstance(actual, BatchRequest)

    # filter params and batch identifiers raise
    base_block["batch_identifiers"] = filter_params
    with pytest.raises(ValueError):
        actual = get_batch_request_from_acceptable_arguments(**base_block)


@pytest.mark.unit
def test_get_batch_request_from_acceptable_arguments_block_data_connector_query(
    base_block: Dict[str, str],
):
    query = {"a": "1"}  # any old dict can be passed

    # filter params as kwargs
    base_block["data_connector_query"] = query
    actual = get_batch_request_from_acceptable_arguments(**base_block)
    assert actual.data_connector_query == query
    assert isinstance(actual, BatchRequest)


@pytest.mark.unit
def test_get_batch_request_from_acceptable_arguments_block_splitter_sampler_batch_spec_passthrough(
    base_block: Dict[str, str],
):
    # splitter and sampling as batch_spec_passthrough
    base_block["sampling_method"] = "sample"
    base_block["sampling_kwargs"] = {"a": "1"}
    base_block["splitter_method"] = "split"
    base_block["splitter_kwargs"] = {"b": "2"}
    actual = get_batch_request_from_acceptable_arguments(**base_block)

    assert actual.batch_spec_passthrough["sampling_method"] == "sample"
    assert actual.batch_spec_passthrough["sampling_kwargs"] == {"a": "1"}
    assert actual.batch_spec_passthrough["splitter_method"] == "split"
    assert actual.batch_spec_passthrough["splitter_kwargs"] == {"b": "2"}
    assert isinstance(actual, BatchRequest)

    # existing batch_spec_passthrough should be preserved, no splitter or sampling args exist
    base_block["batch_spec_passthrough"] = {"c": "3"}
    actual = get_batch_request_from_acceptable_arguments(**base_block)

    assert actual.batch_spec_passthrough["c"] == "3"
    assert "sampling_method" not in actual.batch_spec_passthrough
    assert "sampling_kwargs" not in actual.batch_spec_passthrough
    assert "splitter_method" not in actual.batch_spec_passthrough
    assert "splitter_kwargs" not in actual.batch_spec_passthrough
    assert isinstance(actual, BatchRequest)
