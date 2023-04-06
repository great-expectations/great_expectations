from unittest import mock

import pandas as pd
import pytest

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
@pytest.mark.integration
def test_cloud_backed_data_context_save_expectation_suite_include_rendered_content(
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
    )

    with mock.patch(
        "great_expectations.data_context.store.gx_cloud_store_backend.GXCloudStoreBackend.has_key"
    ), mock.patch(
        "great_expectations.data_context.store.gx_cloud_store_backend.GXCloudStoreBackend._set",
        return_value=cloud_ref,
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
        "great_expectations.data_context.store.gx_cloud_store_backend.GXCloudStoreBackend._update"
    ) as mock_update:
        context.save_expectation_suite(
            expectation_suite=expectation_suite,
        )

        # remove dynamic great_expectations version
        mock_update.call_args[1]["value"].pop("meta")

        mock_update.assert_called_with(
            id=ge_cloud_id,
            value={
                "expectations": [
                    {
                        "meta": {},
                        "kwargs": {"value": 10},
                        "expectation_type": "expect_table_row_count_to_equal",
                        "rendered_content": [
                            {
                                "value": {
                                    "schema": {
                                        "type": "com.superconductive.rendered.string"
                                    },
                                    "params": {
                                        "value": {
                                            "schema": {"type": "number"},
                                            "value": 10,
                                        }
                                    },
                                    "template": "Must have exactly $value rows.",
                                },
                                "name": "atomic.prescriptive.summary",
                                "value_type": "StringValueType",
                            }
                        ],
                    }
                ],
                "ge_cloud_id": ge_cloud_id,
                "data_asset_type": None,
                "expectation_suite_name": "test_suite",
            },
        )


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

    data_asset = context.sources.pandas_default.add_dataframe_asset(
        name="my_dataframe_asset",
        dataframe=df,
    )

    with mock.patch(
        "great_expectations.data_context.store.gx_cloud_store_backend.GXCloudStoreBackend.has_key",
        return_value=False,
    ), mock.patch(
        "great_expectations.data_context.store.gx_cloud_store_backend.GXCloudStoreBackend._set"
    ):
        validator: Validator = context.get_validator(
            batch_request=data_asset.build_batch_request(),
            create_expectation_suite_with_name="test_suite",
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
