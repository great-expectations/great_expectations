from enum import Enum

from typing_extensions import Final

CLOUD_DEFAULT_BASE_URL: Final[str] = "https://api.greatexpectations.io/"


class GECloudEnvironmentVariable(str, Enum):
    BASE_URL = "GE_CLOUD_BASE_URL"
    ORGANIZATION_ID = "GE_CLOUD_ORGANIZATION_ID"
    ACCESS_TOKEN = "GE_CLOUD_ACCESS_TOKEN"


class GeCloudRESTResource(str, Enum):
    BATCH = "batch"
    CHECKPOINT = "checkpoint"
    # Chetan - 20220811 - CONTRACT is deprecated by GX Cloud and is to be removed upon migration of E2E tests
    CONTRACT = "contract"
    DATASOURCE = "datasource"
    DATA_ASSET = "data_asset"
    DATA_CONTEXT = "data_context"
    DATA_CONTEXT_VARIABLES = "data_context_variables"
    EXPECTATION = "expectation"
    EXPECTATION_SUITE = "expectation_suite"
    EXPECTATION_VALIDATION_RESULT = "expectation_validation_result"
    PROFILER = "profiler"
    RENDERED_DATA_DOC = "rendered_data_doc"
    # Chetan - 20220812 - SUITE_VALIDATION_RESULT is deprecated by GX Cloud and is to be removed upon migration of E2E tests
    SUITE_VALIDATION_RESULT = "suite_validation_result"
    VALIDATION_RESULT = "validation_result"
