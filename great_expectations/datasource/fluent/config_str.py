from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pydantic import SecretStr

from great_expectations.core.config_substitutor import TEMPLATE_STR_REGEX

if TYPE_CHECKING:
    from great_expectations.core.config_provider import _ConfigurationProvider

LOGGER = logging.getLogger(__name__)


class ConfigStr(SecretStr):
    def __init__(
        self,
        template_str: str,
    ) -> None:
        # TODO: do the config sub immediately and save it to `_secret_value`?
        # or do at at access time in `get_secret_value`
        self.template_str: str = template_str
        self.config_provider: _ConfigurationProvider | None = None
        # TODO: does this still make sense?
        if self.config_provider:
            self._secret_value = self.get_config_value()
        else:
            self._secret_value: str = ""

    def get_config_value(
        self, config_provider: _ConfigurationProvider | None = None
    ) -> str:
        config_provider = config_provider or self.config_provider
        if not config_provider:
            raise ValueError(
                f"No `config_provider` present, cannot resolve '{self.template_str}'"
            )
        return config_provider.substitute_config(self.template_str)

    def _display(self) -> str:
        return str(self)

    def __str__(self) -> str:
        return self.template_str

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._display()!r})"

    @classmethod
    def _validate_template_str_format(cls, v):
        if TEMPLATE_STR_REGEX.match(v):
            return v
        raise ValueError(
            cls.__name__
            + r" - contains no config template strings in the format '${MY_CONFIG_VAR}'"
        )

    @classmethod
    def __get_validators__(cls):
        # one or more validators may be yielded which will be called in the
        # order to validate the input, each validator will receive as an input
        # the value returned from the previous validator
        yield cls._validate_template_str_format
        yield cls.validate
