from typing import Dict
import pytest
from great_expectations.core.batch import (
    RuntimeBatchRequest,
    get_batch_request_from_acceptable_arguments,
)
from great_expectations.datasource.fluent import BatchRequest as FluentBatchRequest


@pytest.fixture()
def base_fluent():
    return {"datasource_name": "ds", "data_asset_name": "da"}


@pytest.fixture()
def base_block(base_fluent):
    """Basic block-style batch request args"""
    base_fluent["data_connector_name"] = "dc"
    return base_fluent


@pytest.fixture()
def runtime_base_block(base_block):
    """Basic block-style batch request args and a runtime parameter"""
    base_block["path"] = "p"
    return base_block


@pytest.mark.unit()
def test_get_batch_request_from_acceptable_arguments_datasource_name_must_be_str():
    with pytest.raises(TypeError):
        get_batch_request_from_acceptable_arguments(5)


@pytest.mark.unit()
def test_get_batch_request_from_acceptable_arguments_batch_request_passthrough():
    batch_request = FluentBatchRequest(datasource_name="ds", data_asset_name="da")

    actual = get_batch_request_from_acceptable_arguments(batch_request=batch_request)

    assert actual == batch_request


@pytest.mark.unit()
def test_get_batch_request_from_acceptable_arguments_runtime_parameter_conflicts_raise(
    base_block: dict[str, str]
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


@pytest.mark.unit()
@pytest.mark.parametrize(
    "param,value", [("batch_data", "b"), ("query", "q"), ("path", "p")]
)
def test_get_batch_request_from_acceptable_arguments_runtime_parameter_path(
    base_block: Dict[str, str], param, value
):
    """Setting any of the parameters should result in a runtime batch request"""
    base_block[param] = value
    actual = get_batch_request_from_acceptable_arguments(**base_block)
    actual.runtime_parameters[param] == value
    assert type(actual) == RuntimeBatchRequest

    # if runtime parameters are present with the same value, we should get an error
    with pytest.raises(ValueError):
        runtime_parmaeters = {param, value}
        base_block["runtime_parameters"] = runtime_parmaeters
        get_batch_request_from_acceptable_arguments(**base_block)


@pytest.mark.unit()
def test_get_batch_request_from_acceptable_arguments_runtime_batch_identifiers_kwargs(
    runtime_base_block: Dict[str, str]
):
    """Batch identifiers can be provided by batch identifiers or kwargs"""
    bids = {"a": 1, "b": 2}

    # testing batch identifiers as kwargs
    actual = get_batch_request_from_acceptable_arguments(**runtime_base_block, **bids)
    assert actual.batch_identifiers == bids


@pytest.mark.unit()
def test_get_batch_request_from_acceptable_arguments_runtime_batch_identifiers(
    runtime_base_block: Dict[str, str]
):
    """Batch identifiers can be provided by batch identifiers or kwargs"""
    bids = {"a": 1, "b": 2}

    # testing batch identifiers as batch_identifiers
    runtime_base_block["batch_identifiers"] = bids
    actual = get_batch_request_from_acceptable_arguments(**runtime_base_block)
    assert actual.batch_identifiers == bids


def test_get_batch_request_from_acceptable_arguments_fluent(
    base_fluent: Dict[str, str]
):
    actual = get_batch_request_from_acceptable_arguments(**base_fluent)
    assert isinstance(actual, FluentBatchRequest)


def test_get_batch_request_from_acceptable_arguments_fluent_and_block_raises(
    base_fluent: Dict[str, str]
):
    base_fluent["data_connector_query"] = "q"

    with pytest.raises(ValueError) as ve:
        get_batch_request_from_acceptable_arguments(**base_fluent)

    assert "Fluent Batch Requests" in str(ve.value)
