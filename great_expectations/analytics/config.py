from __future__ import annotations

from typing import Optional
from uuid import UUID

from great_expectations.compatibility.pydantic import (
    BaseSettings,
    GenericModel,
    HttpUrl,
)

DUMMY_UUID = UUID("00000000-0000-0000-0000-000000000000")


class _EnvConfig(BaseSettings):
    gx_analytics_enabled: bool = False  # disabled by default, for now

    gx_posthog_debug: bool = False
    gx_posthog_host: HttpUrl = "https://posthog.greatexpectations.io"  # type: ignore[assignment] # default will be coerced
    gx_posthog_project_api_key: str = ""

    _gx_cloud_dev_posthog_project_api_key: str = "phc_dq7deLClUIj5Sm9M40eAMthzkNtBOhF22ZDqPVxU14e"

    @property
    def posthog_enabled(self) -> bool:
        return self.gx_analytics_enabled

    @property
    def posthog_debug(self) -> bool:
        return self.gx_posthog_debug

    @property
    def posthog_host(self) -> str:
        return self.gx_posthog_host

    @property
    def posthog_project_api_key(self):
        if self.gx_posthog_project_api_key:
            return self.gx_posthog_project_api_key

        return self._gx_cloud_dev_posthog_project_api_key


class Config(GenericModel):
    organization_id: Optional[UUID] = None
    user_id: Optional[UUID] = None
    data_context_id: UUID = DUMMY_UUID
    oss_id: UUID = DUMMY_UUID
    cloud_mode: bool = False


ENV_CONFIG = _EnvConfig()
_CONFIG = Config()


def get_config() -> Config:
    return _CONFIG


def update_config(config: Config):
    global _CONFIG  # noqa: PLW0603
    _CONFIG = config
