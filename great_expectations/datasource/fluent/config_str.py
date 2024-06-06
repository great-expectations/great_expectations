from __future__ import annotations

import logging
import warnings
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Literal,
    Mapping,
    Optional,
    TypedDict,
)

from great_expectations.compatibility.pydantic import AnyUrl, SecretStr, parse_obj_as
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.config_substitutor import TEMPLATE_STR_REGEX

if TYPE_CHECKING:
    from typing_extensions import Self, TypeAlias

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

    @override
    def __str__(self) -> str:
        return self.template_str

    @override
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._display()!r})"

    @classmethod
    def str_contains_config_template(cls, v: str) -> bool:
        """
        Returns True if the input string contains a config template string.
        """
        return TEMPLATE_STR_REGEX.search(v) is not None

    @classmethod
    def _validate_template_str_format(cls, v: str) -> str | None:
        if cls.str_contains_config_template(v):
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


UriParts: TypeAlias = (
    Literal[  # https://docs.pydantic.dev/1.10/usage/types/#url-properties
        "scheme", "host", "user", "password", "port", "path", "query", "fragment", "tld"
    ]
)


class UriPartsDict(TypedDict, total=False):
    scheme: str
    user: str | None
    password: str | None
    ipv4: str | None
    ipv6: str | None
    domain: str | None
    port: str | None
    path: str | None
    query: str | None
    fragment: str | None


class ConfigUri(AnyUrl, ConfigStr):  # type: ignore[misc] # Mixin "validate" signature mismatch
    """
    Special type that enables great_expectation config variable substitution for the
    `user` and `password` section of a URI.

    Example:
    ```
    "snowflake://${MY_USER}:${MY_PASSWORD}@account/database/schema/table"
    ```

    Note: this type is meant to used as part of pydantic model.
    To use this outside of a model see the pydantic docs below.
    https://docs.pydantic.dev/usage/models/#parsing-data-into-a-specified-type
    """

    ALLOWED_SUBSTITUTIONS: ClassVar[set[UriParts]] = {"user", "password"}

    min_length: int = 1
    max_length: int = 2**16

    def __init__(  # noqa: PLR0913 # for compatibility with AnyUrl
        self,
        template_str: str,
        *,
        scheme: str,
        user: Optional[str] = None,
        password: Optional[str] = None,
        host: Optional[str] = None,
        tld: Optional[str] = None,
        host_type: str = "domain",
        port: Optional[str] = None,
        path: Optional[str] = None,
        query: Optional[str] = None,
        fragment: Optional[str] = None,
    ) -> None:
        if template_str:  # may have already been set in __new__
            self.template_str: str = template_str
        self._secret_value = template_str  # for compatibility with SecretStr
        super().__init__(
            template_str,
            scheme=scheme,
            user=user,
            password=password,
            host=host,
            tld=tld,
            host_type=host_type,
            port=port,
            path=path,
            query=query,
            fragment=fragment,
        )

    def __new__(cls: type[Self], template_str: Optional[str], **kwargs) -> Self:
        """custom __new__ for compatibility with pydantic.parse_obj_as()"""
        built_url = cls.build(**kwargs) if template_str is None else template_str
        instance = str.__new__(cls, built_url)
        instance.template_str = str(instance)
        return instance

    @classmethod
    @override
    def validate_parts(
        cls, parts: UriPartsDict, validate_port: bool = True
    ) -> UriPartsDict:
        """
        Ensure that only the `user` and `password` parts have config template strings.
        Also validate that all parts of the URI are valid.
        """
        allowed_substitutions = sorted(cls.ALLOWED_SUBSTITUTIONS)

        for name, part in parts.items():
            if not part:
                continue
            if (
                cls.str_contains_config_template(part)  # type: ignore[arg-type] # is str
                and name not in cls.ALLOWED_SUBSTITUTIONS
            ):
                raise ValueError(
                    f"Only {', '.join(allowed_substitutions)} may use config substitution; '{name}' substitution not allowed"
                )
        return AnyUrl.validate_parts(parts, validate_port)

    @override
    def get_config_value(self, config_provider: _ConfigurationProvider) -> AnyUrl:
        """
        Resolve the config template string to its string value according to the passed
        _ConfigurationProvider.
        Parse the resolved URI string into an `AnyUrl` object.
        """
        LOGGER.info(f"Substituting '{self}'")
        raw_value = config_provider.substitute_config(self.template_str)
        return parse_obj_as(AnyUrl, raw_value)

    @classmethod
    def __get_validators__(cls):
        # one or more validators may be yielded which will be called in the
        # order to validate the input, each validator will receive as an input
        # the value returned from the previous validator
        yield ConfigStr._validate_template_str_format
        yield cls.validate  # equivalent to AnyUrl.validate


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
