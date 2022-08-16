from typing import Type
from unittest import mock

import pandas as pd
import pytest

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationSuite,
    ExpectationValidationResult,
)
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import (
    AbstractDataContext,
    BaseDataContext,
    CloudDataContext,
    DataContext,
)
from great_expectations.render.types import RenderedAtomicContent
from great_expectations.validator.validator import Validator


@pytest.mark.cloud
@pytest.mark.integration
@pytest.mark.parametrize(
    "data_context_fixture_name,data_context_type",
    [
        # In order to leverage existing fixtures in parametrization, we provide
        # their string names and dynamically retrieve them using pytest's built-in
        # `request` fixture.
        # Source: https://stackoverflow.com/a/64348247
        pytest.param(
            "empty_base_data_context_in_cloud_mode",
            BaseDataContext,
            id="BaseDataContext",
        ),
        pytest.param("empty_data_context_in_cloud_mode", DataContext, id="DataContext"),
        pytest.param(
            "empty_cloud_data_context", CloudDataContext, id="CloudDataContext"
        ),
    ],
)
def test_cloud_backed_data_context_save_expectation_suite_include_rendered_content(
    data_context_fixture_name: str,
    data_context_type: Type[AbstractDataContext],
    request,
) -> None:
    """
    All Cloud-backed contexts (DataContext, BaseDataContext, and CloudDataContext) should save an ExpectationSuite
    with rendered_content by default.
    """
    context = request.getfixturevalue(data_context_fixture_name)

    with mock.patch(
        "great_expectations.data_context.store.ge_cloud_store_backend.GeCloudStoreBackend.list_keys"
    ), mock.patch(
        "great_expectations.data_context.store.ge_cloud_store_backend.GeCloudStoreBackend._set"
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
        "great_expectations.data_context.store.ge_cloud_store_backend.GeCloudStoreBackend.list_keys"
    ), mock.patch(
        "great_expectations.data_context.store.ge_cloud_store_backend.GeCloudStoreBackend._update"
    ) as mock_update:
        context.save_expectation_suite(
            expectation_suite,
        )

        # remove dynamic great_expectations version
        mock_update.call_args[1]["value"].pop("meta")

        mock_update.assert_called_with(
            ge_cloud_id="None",
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
                "ge_cloud_id": None,
                "data_asset_type": None,
                "expectation_suite_name": "test_suite",
            },
        )


@pytest.mark.cloud
@pytest.mark.integration
@pytest.mark.parametrize(
    "data_context_fixture_name,data_context_type",
    [
        # In order to leverage existing fixtures in parametrization, we provide
        # their string names and dynamically retrieve them using pytest's built-in
        # `request` fixture.
        # Source: https://stackoverflow.com/a/64348247
        pytest.param(
            "cloud_base_data_context_in_cloud_mode_with_datasource_pandas_engine",
            BaseDataContext,
            id="BaseDataContext",
        ),
        pytest.param(
            "cloud_data_context_in_cloud_mode_with_datasource_pandas_engine",
            DataContext,
            id="DataContext",
        ),
        pytest.param(
            "cloud_data_context_with_datasource_pandas_engine",
            CloudDataContext,
            id="CloudDataContext",
        ),
    ],
)
def test_cloud_backed_data_context_expectation_validation_result_include_rendered_content(
    data_context_fixture_name: str,
    data_context_type: Type[AbstractDataContext],
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
        "great_expectations.data_context.store.ge_cloud_store_backend.GeCloudStoreBackend.list_keys"
    ), mock.patch(
        "great_expectations.data_context.store.ge_cloud_store_backend.GeCloudStoreBackend._set"
    ):
        validator: Validator = context.get_validator(
            batch_request=batch_request,
            create_expectation_suite_with_name="test_suite",
        )

        expectation_validation_result: ExpectationValidationResult = (
            validator.expect_table_row_count_to_equal(value=10)
        )

    assert isinstance(
        expectation_validation_result.rendered_content[0], RenderedAtomicContent
    )
    assert isinstance(
        expectation_validation_result.expectation_config.rendered_content[0],
        RenderedAtomicContent,
    )
