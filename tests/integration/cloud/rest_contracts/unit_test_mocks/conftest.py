import re
import uuid
from typing import Final, Generator
from unittest import mock

import pact
import pytest
import responses

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

DUMMY_BASE_URL: Final[str] = "https://fake-host.io"
DUMMY_ORG_ID: Final[str] = str(uuid.uuid4())
DUMMY_ORG_URL_REGEX: Final[re.Pattern] = re.compile(
    f"{DUMMY_BASE_URL}/organizations/{DUMMY_ORG_ID}"
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


@pytest.fixture
def requests_mocker() -> Generator[responses.RequestsMock, None, None]:
    with responses.RequestsMock(assert_all_requests_are_fired=True) as rsps:
        yield rsps


@pytest.fixture
def mock_cloud_data_context(
    requests_mocker: responses.RequestsMock,
) -> CloudDataContext:
    requests_mocker.get(
        DUMMY_ORG_URL_REGEX,
        json=_reify_pact_body(GET_DATA_CONTEXT_CONFIGURATION_MIN_RESPONSE_BODY),
    )

    mock_cloud_data_context: CloudDataContext = gx.get_context(
        mode="cloud",
        cloud_base_url=DUMMY_BASE_URL,
        cloud_organization_id=DUMMY_ORG_ID,
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
    requests_mocker: responses.RequestsMock,
) -> PandasDatasource:
    datasource_name = "mock_cloud_pandas_datasource"

    requests_mocker.get(
        DUMMY_ORG_URL_REGEX, json=_reify_pact_body(GET_DATASOURCE_MIN_RESPONSE_BODY)
    )
    requests_mocker.post(
        DUMMY_ORG_URL_REGEX, json=_reify_pact_body(POST_DATASOURCE_MIN_RESPONSE_BODY)
    )

    with mock.patch(
        target="great_expectations.core.datasource_dict.DatasourceDict.data",
        return_value={},
    ):
        mock_cloud_pandas_datasource: PandasDatasource = (
            mock_cloud_data_context.sources.add_pandas(name=datasource_name)
        )

    assert isinstance(mock_cloud_pandas_datasource, PandasDatasource)
    assert mock_cloud_pandas_datasource.name == datasource_name

    return mock_cloud_pandas_datasource
