from unittest import mock

import pytest
from pact.matchers import Matcher

import great_expectations as gx
from great_expectations.core.datasource_dict import DatasourceDict
from great_expectations.data_context import CloudDataContext
from great_expectations.datasource.fluent import PandasDatasource
from tests.integration.cloud.rest_contracts.conftest import JsonData, PactBody
from tests.integration.cloud.rest_contracts.test_data_context_configuration import (
    DATA_CONTEXT_CONFIGURATION_MIN_RESPONSE_BODY,
)
from tests.integration.cloud.rest_contracts.test_datasource import (
    DATASOURCE_MIN_RESPONSE_BODY,
)


def _convert_matcher_to_value(matcher: Matcher) -> JsonData:
    return matcher.generate()["contents"]


def _reify_pact_body(
    body: PactBody,
) -> JsonData:
    if isinstance(body, list):
        for index, item in enumerate(body):
            if isinstance(item, Matcher):
                body[index] = _convert_matcher_to_value(matcher=item)
            body[index] = _reify_pact_body(body=body[index])
        return body
    elif isinstance(body, Matcher):
        return _reify_pact_body(body=_convert_matcher_to_value(matcher=body))
    elif isinstance(body, dict):
        for key, value in body.items():
            if isinstance(value, Matcher):
                body[key] = _convert_matcher_to_value(matcher=value)
            body[key] = _reify_pact_body(body=body[key])
        return body
    else:
        return body


@mock.patch(target="requests.Session.get")
@pytest.fixture
def mock_cloud_data_context() -> CloudDataContext:
    response_body: JsonData = _reify_pact_body(
        body=DATA_CONTEXT_CONFIGURATION_MIN_RESPONSE_BODY
    )
    with mock.patch(
        target="requests.Response.json",
        return_value=response_body,
    ):
        mock_cloud_data_context = gx.get_context(mode="cloud")
        mock_cloud_data_context.datasources.return_value = DatasourceDict(
            context=mock_cloud_data_context,
            datasource_store=mock_cloud_data_context._datasource_store,
        )
        return mock_cloud_data_context


@pytest.fixture
def mock_cloud_datasource(
    mock_cloud_data_context: CloudDataContext,
) -> PandasDatasource:
    _reify_pact_body(body=DATASOURCE_MIN_RESPONSE_BODY)
    return mock_cloud_data_context.sources.add_pandas(name="mock_cloud_datasource")
