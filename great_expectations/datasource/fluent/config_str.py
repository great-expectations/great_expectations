from __future__ import annotations

import logging
import warnings
from typing import TYPE_CHECKING, Mapping

from pydantic import SecretStr

from great_expectations.core.config_substitutor import TEMPLATE_STR_REGEX

if TYPE_CHECKING:
    from great_expectations.core.config_provider import _ConfigurationProvider
    from great_expectations.datasource.fluent import Datasource

LOGGER = logging.getLogger(__name__)


class ConfigStr(SecretStr):
    """
    Special type that enables great_expectation config variable substitution.

    To enable config substitution for Fluent Datasources or DataAsset fields must be of
    the `ConfigStr` type, or a union containing this type.

    Note: this type is meant to used as part of pydantic model.
    To use this outside of a model see the pydantic docs below.
    https://docs.pydantic.dev/usage/models/#parsing-data-into-a-specified-type
    """

    def __init__(
        self,
        template_str: str,
    ) -> None:
        self.template_str: str = template_str
        self._secret_value = template_str  # for compatibility with SecretStr

    def get_config_value(self, config_provider: _ConfigurationProvider) -> str:
        """
        Resolve the config template string to its string value according to the passed
        _ConfigurationProvider.
        """
        LOGGER.info(f"Substituting '{self}'")
        return config_provider.substitute_config(self.template_str)

    def _display(self) -> str:
        return str(self)

    def __str__(self) -> str:
        return self.template_str

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._display()!r})"

    @classmethod
    def _validate_template_str_format(cls, v):
        if TEMPLATE_STR_REGEX.search(v):
            return v
        raise ValueError(
            cls.__name__
            + " - contains no config template strings in the format"
            + r" '${MY_CONFIG_VAR}' or '$MY_CONFIG_VAR'"
        )

    @classmethod
    def __get_validators__(cls):
        # one or more validators may be yielded which will be called in the
        # order to validate the input, each validator will receive as an input
        # the value returned from the previous validator
        yield cls._validate_template_str_format
        yield cls.validate


def _check_config_substitutions_needed(
    datasource: Datasource,
    options: Mapping,
    raise_warning_if_provider_not_present: bool,
) -> set[str]:
    """
    Given a Datasource and a dict-like mapping type return the keys whose value is a `ConfigStr` type.
    Optionally raise a warning if config substitution is needed but impossible due to a missing `_config_provider`.
    """
    need_config_subs: set[str] = {
        k for (k, v) in options.items() if isinstance(v, ConfigStr)
    }
    if (
        need_config_subs
        and raise_warning_if_provider_not_present
        and not datasource._config_provider
    ):
        warnings.warn(
            f"config variables '{','.join(need_config_subs)}' need substitution but no `_ConfigurationProvider` is present"
        )
    return need_config_subs
