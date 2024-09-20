from __future__ import annotations

from enum import Enum
from typing import Final

SUPPORT_EMAIL = "support@greatexpectations.io"
CLOUD_DEFAULT_BASE_URL: Final[str] = "https://api.greatexpectations.io/"
CLOUD_APP_DEFAULT_BASE_URL: Final[str] = "https://app.greatexpectations.io/"


class GXCloudEnvironmentVariable(str, Enum):
    BASE_URL = "GX_CLOUD_BASE_URL"
    ORGANIZATION_ID = "GX_CLOUD_ORGANIZATION_ID"
    ACCESS_TOKEN = "GX_CLOUD_ACCESS_TOKEN"


class GXCloudRESTResource(str, Enum):
    CHECKPOINT = "checkpoint"
    DATASOURCE = "datasource"
    DATA_ASSET = "data_asset"
    DATA_CONTEXT = "data_context_configuration"
    DATA_CONTEXT_VARIABLES = "data_context_variables"
    EXPECTATION_SUITE = "expectation_suite"
    RENDERED_DATA_DOC = "rendered_data_doc"
    VALIDATION_DEFINITION = "validation_definition"
    VALIDATION_RESULT = "validation_result"
