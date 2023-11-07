import json
import uuid
from unittest import mock

import pact
import pytest
import requests

import great_expectations as gx
from great_expectations.data_context import CloudDataContext
from great_expectations.datasource.fluent import PandasDatasource
from tests.integration.cloud.rest_contracts.conftest import JsonData, PactBody
from tests.integration.cloud.rest_contracts.test_data_context_configuration import (
    GET_DATA_CONTEXT_CONFIGURATION_MIN_RESPONSE_BODY,
)
from tests.integration.cloud.rest_contracts.test_datasource import (
    GET_DATASOURCE_MIN_RESPONSE_BODY,
    POST_DATASOURCE_MIN_RESPONSE_BODY,
)


def _convert_matcher_to_value(matcher: pact.matchers.Matcher) -> JsonData:
    return matcher.generate()["contents"]


def _reify_pact_body(
    body: PactBody,
) -> JsonData:
    if isinstance(body, list):
        for index, item in enumerate(body):
            if isinstance(item, pact.matchers.Matcher):
                body[index] = _convert_matcher_to_value(matcher=item)
            body[index] = _reify_pact_body(body=body[index])
        return body
    elif isinstance(body, pact.matchers.Matcher):
        return _reify_pact_body(body=_convert_matcher_to_value(matcher=body))
    elif isinstance(body, dict):
        # calling generate on a parent matcher causes all child matchers
        # to take the form of a dict with the following keys
        if all(key in body for key in ("json_class", "data")):
            body = body["data"]["generate"]
        else:
            for key, value in body.items():
                if isinstance(value, pact.matchers.Matcher):
                    body[key] = _convert_matcher_to_value(matcher=value)
                body[key] = _reify_pact_body(body=body[key])
        return body
    else:
        return body


def _get_mock_response_from_pact_response_body(
    status_code: int,
    pact_response_body: PactBody,
) -> requests.Response:
    response_body: JsonData = _reify_pact_body(
        body=pact_response_body,
    )
    mock_response = requests.Response()
    mock_response.status_code = status_code
    mock_response._content = json.dumps(response_body).encode("utf-8")
    return mock_response


@pytest.fixture
def mock_cloud_data_context() -> CloudDataContext:
    mock_response: requests.Response = _get_mock_response_from_pact_response_body(
        status_code=200,
        pact_response_body=GET_DATA_CONTEXT_CONFIGURATION_MIN_RESPONSE_BODY,
    )

    with mock.patch(
        target="requests.Session.get",
        return_value=mock_response,
    ):
        mock_cloud_data_context: CloudDataContext = gx.get_context(
            mode="cloud",
            cloud_base_url="https://fake-host.io",
            cloud_organization_id=str(uuid.uuid4()),
            cloud_access_token="not a real token",
        )

    assert isinstance(mock_cloud_data_context, CloudDataContext)
    assert mock_cloud_data_context.variables.include_rendered_content.globally is True
    assert (
        mock_cloud_data_context.variables.include_rendered_content.expectation_suite
        is True
    )
    assert (
        mock_cloud_data_context.variables.include_rendered_content.expectation_validation_result
        is True
    )
    return mock_cloud_data_context


@pytest.fixture
def mock_cloud_pandas_datasource(
    mock_cloud_data_context: CloudDataContext,
) -> PandasDatasource:
    datasource_name = "mock_cloud_pandas_datasource"

    mock_post_response: requests.Response = _get_mock_response_from_pact_response_body(
        status_code=200,
        pact_response_body=POST_DATASOURCE_MIN_RESPONSE_BODY,
    )

    mock_get_response: requests.Response = _get_mock_response_from_pact_response_body(
        status_code=200,
        pact_response_body=GET_DATASOURCE_MIN_RESPONSE_BODY,
    )

    with mock.patch(
        target="great_expectations.core.datasource_dict.DatasourceDict.data",
        return_value={},
    ):
        with mock.patch(
            target="requests.Session.post",
            return_value=mock_post_response,
        ):
            with mock.patch(
                target="requests.Session.get",
                return_value=mock_get_response,
            ):
                mock_cloud_pandas_datasource: PandasDatasource = (
                    mock_cloud_data_context.sources.add_pandas(name=datasource_name)
                )

    assert isinstance(mock_cloud_pandas_datasource, PandasDatasource)
    assert mock_cloud_pandas_datasource.name == datasource_name

    return mock_cloud_pandas_datasource
