import pydantic
from pydantic import AnyUrl

from great_expectations.data_context.cloud_constants import CLOUD_DEFAULT_BASE_URL


class GxAgentEnvVars(pydantic.BaseSettings):
    gx_cloud_base_url: AnyUrl = CLOUD_DEFAULT_BASE_URL  # type: ignore[assignment]
    gx_cloud_organization_id: str
    gx_cloud_access_token: str
