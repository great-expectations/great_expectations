from unittest import mock

import pandas as pd
import pytest

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationSuite,
    ExpectationValidationResult,
)
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.types.refs import GXCloudResourceRef
from great_expectations.render import RenderedAtomicContent
from great_expectations.validator.validator import Validator


@pytest.mark.cloud
@pytest.mark.integration
@pytest.mark.parametrize(
    "data_context_fixture_name",
    [
        # In order to leverage existing fixtures in parametrization, we provide
        # their string names and dynamically retrieve them using pytest's built-in
        # `request` fixture.
        # Source: https://stackoverflow.com/a/64348247
        pytest.param(
            "empty_base_data_context_in_cloud_mode",
            id="BaseDataContext",
        ),
        pytest.param("empty_data_context_in_cloud_mode", id="DataContext"),
        pytest.param("empty_cloud_data_context", id="CloudDataContext"),
    ],
)
def test_cloud_backed_data_context_save_expectation_suite_include_rendered_content(
    data_context_fixture_name: str,
    request,
) -> None:
    """
    All Cloud-backed contexts (DataContext, BaseDataContext, and CloudDataContext) should save an ExpectationSuite
    with rendered_content by default.
    """
    context = request.getfixturevalue(data_context_fixture_name)

    ge_cloud_id = "d581305a-cdce-483b-84ba-5c673d2ce009"
    cloud_ref = GXCloudResourceRef(
        resource_type=GXCloudRESTResource.EXPECTATION_SUITE,
        cloud_id=ge_cloud_id,
        url="foo/bar/baz",
    )

    with mock.patch(
        "great_expectations.data_context.store.gx_cloud_store_backend.GXCloudStoreBackend.list_keys"
    ), mock.patch(
        "great_expectations.data_context.store.gx_cloud_store_backend.GXCloudStoreBackend._set",
        return_value=cloud_ref,
    ):
        expectation_suite: ExpectationSuite = context.create_expectation_suite(
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
            expectation_suite,
        )

        # remove dynamic great_expectations version
        mock_update.call_args[1]["value"].pop("meta")

        mock_update.assert_called_with(
            ge_cloud_id=ge_cloud_id,
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
                                    "header": None,
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


# TODO: ACB - Enable this test after merging fixes in PRs 5778 and 5763
@pytest.mark.cloud
@pytest.mark.integration
@pytest.mark.xfail(strict=True, reason="Remove xfail on merge of PRs 5778 and 5763")
@pytest.mark.parametrize(
    "data_context_fixture_name",
    [
        # In order to leverage existing fixtures in parametrization, we provide
        # their string names and dynamically retrieve them using pytest's built-in
        # `request` fixture.
        # Source: https://stackoverflow.com/a/64348247
        pytest.param(
            "cloud_base_data_context_in_cloud_mode_with_datasource_pandas_engine",
            id="BaseDataContext",
        ),
        pytest.param(
            "cloud_data_context_in_cloud_mode_with_datasource_pandas_engine",
            id="DataContext",
        ),
        pytest.param(
            "cloud_data_context_with_datasource_pandas_engine",
            id="CloudDataContext",
        ),
    ],
)
def test_cloud_backed_data_context_expectation_validation_result_include_rendered_content(
    data_context_fixture_name: str,
    request,
) -> None:
    """
    All Cloud-backed contexts (DataContext, BaseDataContext, and CloudDataContext) should save an ExpectationValidationResult
    with rendered_content by default.
    """
    context = request.getfixturevalue(data_context_fixture_name)

    df = pd.DataFrame([1, 2, 3, 4, 5])

    batch_request = RuntimeBatchRequest(
        datasource_name="my_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="my_data_asset",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier_name": "my_id"},
    )

    with mock.patch(
        "great_expectations.data_context.store.gx_cloud_store_backend.GXCloudStoreBackend.list_keys"
    ), mock.patch(
        "great_expectations.data_context.store.gx_cloud_store_backend.GXCloudStoreBackend._set"
    ):
        validator: Validator = context.get_validator(
            batch_request=batch_request,
            create_expectation_suite_with_name="test_suite",
        )

        expectation_validation_result: ExpectationValidationResult = (
            validator.expect_table_row_count_to_equal(value=10)
        )

    for result in expectation_validation_result.results:
        for rendered_content in result.rendered_content:
            assert isinstance(rendered_content, RenderedAtomicContent)

    for expectation_configuration in expectation_validation_result.expectation_config:
        for rendered_content in expectation_configuration.rendered_content:
            assert isinstance(rendered_content, RenderedAtomicContent)
