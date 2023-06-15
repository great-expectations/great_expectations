import random
import string
from unittest import mock

import pandas as pd
import pytest
import responses

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationSuite,
    ExpectationValidationResult,
)
from great_expectations.data_context import CloudDataContext
from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.types.refs import GXCloudResourceRef
from great_expectations.render import RenderedAtomicContent
from great_expectations.validator.validator import Validator


@pytest.mark.cloud
@responses.activate
def test_cloud_backed_data_context_add_or_update_expectation_suite_include_rendered_content(
    empty_cloud_data_context: CloudDataContext,
) -> None:
    """
    Cloud-backed contexts should save an ExpectationSuite with rendered_content by default.
    """
    context = empty_cloud_data_context

    ge_cloud_id = "d581305a-cdce-483b-84ba-5c673d2ce009"
    cloud_ref = GXCloudResourceRef(
        resource_type=GXCloudRESTResource.EXPECTATION_SUITE,
        id=ge_cloud_id,
        url="foo/bar/baz",
        # response_json will not be empty but is not needed for this test.
        response_json={},
    )

    empty_expectation_suite = ExpectationSuite(expectation_suite_name="test_suite")
    with mock.patch(
        "great_expectations.data_context.store.gx_cloud_store_backend.GXCloudStoreBackend._get"
    ), mock.patch(
        "great_expectations.data_context.store.gx_cloud_store_backend.GXCloudStoreBackend._set",
        return_value=cloud_ref,
    ), mock.patch(
        "great_expectations.data_context.data_context.CloudDataContext.get_expectation_suite",
        return_value=empty_expectation_suite,
    ):
        expectation_suite: ExpectationSuite = context.add_or_update_expectation_suite(
            "test_suite"
        )
    expectation_suite.expectations.append(
        ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_equal", kwargs={"value": 10}
        )
    )
    assert expectation_suite.expectations[0].rendered_content is None

    with mock.patch(
        "great_expectations.data_context.store.gx_cloud_store_backend.GXCloudStoreBackend.list_keys"
    ), mock.patch(
        "great_expectations.data_context.store.gx_cloud_store_backend.GXCloudStoreBackend._set"
    ) as mock_update:
        context.save_expectation_suite(expectation_suite=expectation_suite)

        # remove dynamic great_expectations version
        mock_update.call_args[0][1].pop("meta")

        assert mock_update.call_args[0][1] == {
            "expectation_suite_name": "test_suite",
            "ge_cloud_id": None,
            "data_asset_type": None,
            "expectations": [
                {
                    "rendered_content": [
                        {
                            "value": {
                                "template": "Must have exactly $value rows.",
                                "params": {
                                    "value": {"schema": {"type": "number"}, "value": 10}
                                },
                                "schema": {
                                    "type": "com.superconductive.rendered.string"
                                },
                            },
                            "value_type": "StringValueType",
                            "name": "atomic.prescriptive.summary",
                        }
                    ],
                    "expectation_type": "expect_table_row_count_to_equal",
                    "meta": {},
                    "kwargs": {"value": 10},
                }
            ],
        }


@pytest.mark.cloud
@pytest.mark.integration
def test_cloud_backed_data_context_expectation_validation_result_include_rendered_content(
    empty_cloud_data_context: CloudDataContext,
) -> None:
    """
    All CloudDataContexts should save an ExpectationValidationResult with rendered_content by default.
    """
    context = empty_cloud_data_context

    df = pd.DataFrame([1, 2, 3, 4, 5])
    suite_name = f"test_suite_{''.join(random.choice(string.ascii_letters + string.digits) for _ in range(8))}"
    mock_datasource_get_response = {
        "data": {
            "id": "123456",
            "attributes": {
                "datasource_config": {},
            },
        },
    }

    with mock.patch(
        "great_expectations.data_context.store.gx_cloud_store_backend.GXCloudStoreBackend.has_key",
        return_value=False,
    ), mock.patch(
        "great_expectations.data_context.store.gx_cloud_store_backend.GXCloudStoreBackend.set"
    ), mock.patch(
        "great_expectations.data_context.store.gx_cloud_store_backend.GXCloudStoreBackend.get",
        return_value=mock_datasource_get_response,
    ):
        data_asset = context.sources.pandas_default.add_dataframe_asset(
            name="my_dataframe_asset",
            dataframe=df,
        )
        validator: Validator = context.get_validator(
            batch_request=data_asset.build_batch_request(),
            create_expectation_suite_with_name=suite_name,
        )

        expectation_validation_result: ExpectationValidationResult = (
            validator.expect_table_row_count_to_equal(value=10)
        )

    for rendered_content in expectation_validation_result.rendered_content:
        assert isinstance(rendered_content, RenderedAtomicContent)

    for (
        rendered_content
    ) in expectation_validation_result.expectation_config.rendered_content:
        assert isinstance(rendered_content, RenderedAtomicContent)
