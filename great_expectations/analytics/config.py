from __future__ import annotations

from typing import Optional
from uuid import UUID

from great_expectations.compatibility.pydantic import (
    BaseSettings,
    GenericModel,
    HttpUrl,
)


class _EnvConfig(BaseSettings):
    gx_analytics_enabled: Optional[bool] = None

    gx_posthog_debug: bool = False
    gx_posthog_host: HttpUrl = "https://posthog.greatexpectations.io"  # type: ignore[assignment] # default will be coerced
    gx_posthog_project_api_key: str = "phc_ph6ugZ1zq94dli0r1xgFg19fk2bb1EdDoLn9NZnCvRs"

    @property
    def posthog_enabled(self) -> Optional[bool]:
        return self.gx_analytics_enabled

    @property
    def posthog_debug(self) -> bool:
        return self.gx_posthog_debug

    @property
    def posthog_host(self) -> str:
        return self.gx_posthog_host

    @property
    def posthog_project_api_key(self):
        return self.gx_posthog_project_api_key


class Config(GenericModel):
    organization_id: Optional[UUID] = None
    user_id: Optional[UUID] = None
    data_context_id: Optional[UUID] = None
    oss_id: Optional[UUID] = None
    cloud_mode: bool = False


ENV_CONFIG = _EnvConfig()
_CONFIG = Config()


def get_config() -> Config:
    return _CONFIG


def update_config(config: Config):
    global _CONFIG  # noqa: PLW0603
    _CONFIG = config
