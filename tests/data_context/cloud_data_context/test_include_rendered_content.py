import random
import string

import pandas as pd
import pytest

from great_expectations.core import (
    ExpectationValidationResult,
)
from great_expectations.data_context import CloudDataContext
from great_expectations.render import RenderedAtomicContent
from great_expectations.validator.validator import Validator


@pytest.mark.xfail(
    reason="add_or_update not responsible for rendered content - rewrite test for new suites factory"  # noqa: E501
)
@pytest.mark.cloud
def test_cloud_backed_data_context_expectation_validation_result_include_rendered_content(
    empty_cloud_context_fluent: CloudDataContext,
) -> None:
    """
    All CloudDataContexts should save an ExpectationValidationResult with rendered_content by default.
    """  # noqa: E501
    context = empty_cloud_context_fluent

    df = pd.DataFrame([1, 2, 3, 4, 5])
    suite_name = f"test_suite_{''.join(random.choice(string.ascii_letters + string.digits) for _ in range(8))}"  # noqa: E501

    data_asset = context.data_sources.pandas_default.add_dataframe_asset(
        name="my_dataframe_asset",
    )
    validator: Validator = context.get_validator(
        batch_request=data_asset.build_batch_request(options={"dataframe": df}),
        create_expectation_suite_with_name=suite_name,
    )

    expectation_validation_result: ExpectationValidationResult = (
        validator.expect_table_row_count_to_equal(value=10)
    )

    for rendered_content in expectation_validation_result.rendered_content:
        assert isinstance(rendered_content, RenderedAtomicContent)

    for rendered_content in expectation_validation_result.expectation_config.rendered_content:
        assert isinstance(rendered_content, RenderedAtomicContent)
