from typing import Type
from unittest import mock

import pytest

from great_expectations.core import ExpectationConfiguration, ExpectationSuite
from great_expectations.data_context import (
    AbstractDataContext,
    BaseDataContext,
    CloudDataContext,
    DataContext,
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
