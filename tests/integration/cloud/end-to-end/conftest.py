import os

import pytest

import great_expectations as gx
from great_expectations.data_context import CloudDataContext


@pytest.fixture
def context() -> CloudDataContext:
    context = gx.get_context(
        mode="cloud",
        cloud_base_url=os.environ.get("MERCURY_BASE_URL"),
        cloud_organization_id=os.environ.get("MERCURY_ORGANIZATION_ID"),
        cloud_access_token=os.environ.get("MERCURY_ACCESS_TOKEN"),
    )
    assert isinstance(context, CloudDataContext)
    return context
