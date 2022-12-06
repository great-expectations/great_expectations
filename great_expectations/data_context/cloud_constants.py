from enum import Enum

from typing_extensions import Final

SUPPORT_EMAIL = "support@greatexpectations.io"
CLOUD_DEFAULT_BASE_URL: Final[str] = "https://api.greatexpectations.io/"


class GXCloudEnvironmentVariable(str, Enum):
    BASE_URL = "GX_CLOUD_BASE_URL"
    ORGANIZATION_ID = "GX_CLOUD_ORGANIZATION_ID"
    ACCESS_TOKEN = "GX_CLOUD_ACCESS_TOKEN"
    # Deprecated as of 0.15.37
    _BASE_URL = "GE_CLOUD_BASE_URL"
    _ORGANIZATION_ID = "GE_CLOUD_ORGANIZATION_ID"
    _ACCESS_TOKEN = "GE_CLOUD_ACCESS_TOKEN"


class GXCloudRESTResource(str, Enum):
    BATCH = "batch"
    CHECKPOINT = "checkpoint"
    DATASOURCE = "datasource"
    DATA_ASSET = "data_asset"
    DATA_CONTEXT = "data_context"
    DATA_CONTEXT_VARIABLES = "data_context_variables"
    EXPECTATION = "expectation"
    EXPECTATION_SUITE = "expectation_suite"
    EXPECTATION_VALIDATION_RESULT = "expectation_validation_result"
    PROFILER = "profiler"
    RENDERED_DATA_DOC = "rendered_data_doc"
    VALIDATION_RESULT = "validation_result"
