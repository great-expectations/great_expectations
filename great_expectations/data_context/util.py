from __future__ import annotations

import copy
import inspect
import logging
import pathlib
import re
import warnings
from typing import Any, Optional
from urllib.parse import urlparse

import pyparsing as pp

from great_expectations.alias_types import PathStr  # noqa: TCH001
from great_expectations.exceptions import StoreConfigurationError
from great_expectations.types import safe_deep_copy
from great_expectations.util import load_class, verify_dynamic_loading_support

try:
    import sqlalchemy as sa  # noqa: TID251
except ImportError:
    sa = None

logger = logging.getLogger(__name__)


# TODO: Rename config to constructor_kwargs and config_defaults -> constructor_kwarg_default
# TODO: Improve error messages in this method. Since so much of our workflow is config-driven, this will be a *super* important part of DX.
def instantiate_class_from_config(  # noqa: PLR0912
    config, runtime_environment, config_defaults=None
):
    """Build a GX class from configuration dictionaries."""

    if config_defaults is None:
        config_defaults = {}

    config = copy.deepcopy(config)

    module_name = config.pop("module_name", None)
    if module_name is None:
        try:
            module_name = config_defaults.pop("module_name")
        except KeyError:
            raise KeyError(
                "Neither config : {} nor config_defaults : {} contains a module_name key.".format(
                    config,
                    config_defaults,
                )
            )
    else:
        # Pop the value without using it, to avoid sending an unwanted value to the config_class
        config_defaults.pop("module_name", None)

    logger.debug(f"(instantiate_class_from_config) module_name -> {module_name}")
    verify_dynamic_loading_support(module_name=module_name)

    class_name = config.pop("class_name", None)
    if class_name is None:
        logger.warning(
            "Instantiating class from config without an explicit class_name is dangerous. Consider adding "
            f"an explicit class_name for {config.get('name')}"
        )
        try:
            class_name = config_defaults.pop("class_name")
        except KeyError:
            raise KeyError(
                "Neither config : {} nor config_defaults : {} contains a class_name key.".format(
                    config,
                    config_defaults,
                )
            )
    else:
        # Pop the value without using it, to avoid sending an unwanted value to the config_class
        config_defaults.pop("class_name", None)

    class_ = load_class(class_name=class_name, module_name=module_name)

    config_with_defaults = copy.deepcopy(config_defaults)
    config_with_defaults.update(config)
    if runtime_environment is not None:
        # If there are additional kwargs available in the runtime_environment requested by a
        # class to be instantiated, provide them
        argspec = inspect.getfullargspec(class_.__init__)[0][1:]

        missing_args = set(argspec) - set(config_with_defaults.keys())
        config_with_defaults.update(
            {
                missing_arg: runtime_environment[missing_arg]
                for missing_arg in missing_args
                if missing_arg in runtime_environment
            }
        )
        # Add the entire runtime_environment as well if it's requested
        if "runtime_environment" in missing_args:
            config_with_defaults.update({"runtime_environment": runtime_environment})

    try:
        class_instance = class_(**config_with_defaults)
    except TypeError as e:
        raise TypeError(
            "Couldn't instantiate class: {} with config: \n\t{}\n \n".format(
                class_name, format_dict_for_error_message(config_with_defaults)
            )
            + str(e)
        )

    return class_instance


def format_dict_for_error_message(dict_):
    # TODO : Tidy this up a bit. Indentation isn't fully consistent.

    return "\n\t".join("\t\t".join((str(key), str(dict_[key]))) for key in dict_)


def file_relative_path(
    source_path: PathStr,
    relative_path: PathStr,
    strict: bool = True,
) -> str:
    """
    This function is useful when one needs to load a file that is
    relative to the position of the current file. (Such as when
    you encode a configuration file path in source file and want
    in runnable in any current working directory)

    It is meant to be used like the following:
    file_relative_path(__file__, 'path/relative/to/file')

    This has been modified from Dagster's utils:
    H/T https://github.com/dagster-io/dagster/blob/8a250e9619a49e8bff8e9aa7435df89c2d2ea039/python_modules/dagster/dagster/utils/__init__.py#L34
    """
    dir_path = pathlib.Path(source_path).parent
    abs_path = dir_path.joinpath(relative_path).resolve(strict=strict)
    return str(abs_path)


def parse_substitution_variable(substitution_variable: str) -> Optional[str]:
    """
    Parse and check whether the string contains a substitution variable of the case insensitive form ${SOME_VAR} or $SOME_VAR
    Args:
        substitution_variable: string to be parsed

    Returns:
        string of variable name e.g. SOME_VAR or None if not parsable. If there are multiple substitution variables this currently returns the first e.g. $SOME_$TRING -> $SOME_
    """
    substitution_variable_name = pp.Word(pp.alphanums + "_").setResultsName(
        "substitution_variable_name"
    )
    curly_brace_parser = "${" + substitution_variable_name + "}"
    non_curly_brace_parser = "$" + substitution_variable_name
    both_parser = curly_brace_parser | non_curly_brace_parser
    try:
        parsed_substitution_variable = both_parser.parseString(substitution_variable)
        return parsed_substitution_variable.substitution_variable_name
    except pp.ParseException:
        return None


class PasswordMasker:
    """
    Used to mask passwords in Datasources. Does not mask sqlite urls.

    Example usage
    masked_db_url = PasswordMasker.mask_db_url(url)
    where url = "postgresql+psycopg2://username:password@host:65432/database"
    and masked_url = "postgresql+psycopg2://username:***@host:65432/database"

    """

    MASKED_PASSWORD_STRING = "***"

    # values with the following keys will be processed with cls.mask_db_url:
    URL_KEYS = {"connection_string", "url"}

    # values with these keys will be directly replaced with cls.MASKED_PASSWORD_STRING:
    PASSWORD_KEYS = {"access_token", "password"}

    @classmethod
    def mask_db_url(cls, url: str, use_urlparse: bool = False, **kwargs) -> str:
        """
        Mask password in database url.
        Uses sqlalchemy engine parsing if sqlalchemy is installed, otherwise defaults to using urlparse from the stdlib which does not handle kwargs.
        Args:
            url: Database url e.g. "postgresql+psycopg2://username:password@host:65432/database"
            use_urlparse: Skip trying to parse url with sqlalchemy and use urlparse
            **kwargs: passed to create_engine()

        Returns:
            url with password masked e.g. "postgresql+psycopg2://username:***@host:65432/database"
        """
        if url.startswith("DefaultEndpointsProtocol"):
            return cls._obfuscate_azure_blobstore_connection_string(url)
        elif sa is not None and use_urlparse is False:
            try:
                engine = sa.create_engine(url, **kwargs)
                return engine.url.__repr__()
            # Account for the edge case where we have SQLAlchemy in our env but haven't installed the appropriate dialect to match the input URL
            except Exception as e:
                logger.warning(
                    f"Something went wrong when trying to use SQLAlchemy to obfuscate URL: {e}"
                )
        else:
            warnings.warn(
                "SQLAlchemy is not installed, using urlparse to mask database url password which ignores **kwargs."
            )
        return cls._mask_db_url_no_sa(url=url)

    @classmethod
    def _obfuscate_azure_blobstore_connection_string(cls, url: str) -> str:
        # Parse Azure Connection Strings
        azure_conn_str_re = re.compile(
            "(DefaultEndpointsProtocol=(http|https));(AccountName=([a-zA-Z0-9]+));(AccountKey=)(.+);(EndpointSuffix=([a-zA-Z\\.]+))"
        )
        try:
            matched: re.Match[str] | None = azure_conn_str_re.match(url)
            if not matched:
                raise StoreConfigurationError(
                    f"The URL for the Azure connection-string, was not configured properly. Please check and try again: {url} "
                )
            res = f"DefaultEndpointsProtocol={matched.group(2)};AccountName={matched.group(4)};AccountKey=***;EndpointSuffix={matched.group(8)}"
            return res
        except Exception as e:
            raise StoreConfigurationError(
                f"Something went wrong when trying to obfuscate URL for Azure connection-string. Please check your configuration: {e}"
            )

    @classmethod
    def _mask_db_url_no_sa(cls, url: str) -> str:
        # oracle+cx_oracle does not parse well using urlparse, parse as oracle then swap back
        replace_prefix = None
        if url.startswith("oracle+cx_oracle"):
            replace_prefix = {"original": "oracle+cx_oracle", "temporary": "oracle"}
            url = url.replace(replace_prefix["original"], replace_prefix["temporary"])

        parsed_url = urlparse(url)

        # Do not parse sqlite
        if parsed_url.scheme == "sqlite":
            return url

        colon = ":" if parsed_url.port is not None else ""
        masked_url = (
            f"{parsed_url.scheme}://{parsed_url.username}:{cls.MASKED_PASSWORD_STRING}"
            f"@{parsed_url.hostname}{colon}{parsed_url.port or ''}{parsed_url.path or ''}"
        )

        if replace_prefix is not None:
            masked_url = masked_url.replace(
                replace_prefix["temporary"], replace_prefix["original"]
            )

        return masked_url

    @classmethod
    def sanitize_config(cls, config: dict) -> dict:
        """
        Mask sensitive fields in a Dict.
        """

        # be defensive, since it would be logical to expect this method works with DataContextConfig
        if not isinstance(config, dict):
            raise TypeError(
                "PasswordMasker.sanitize_config expects param `config` "
                + f"to be of type Dict, not of type {type(config)}"
            )

        config_copy = safe_deep_copy(config)  # be immutable

        def recursive_cleaner_method(config: Any) -> None:
            if isinstance(config, dict):
                for key, val in config.items():
                    if not isinstance(val, str):
                        recursive_cleaner_method(val)
                    elif key in cls.URL_KEYS:
                        config[key] = cls.mask_db_url(val)
                    elif key in cls.PASSWORD_KEYS:
                        config[key] = cls.MASKED_PASSWORD_STRING
                    else:
                        pass  # this string is not sensitive
            elif isinstance(config, list):
                for val in config:
                    recursive_cleaner_method(val)

        recursive_cleaner_method(config_copy)  # Perform anonymization in place

        return config_copy
