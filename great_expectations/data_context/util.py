from __future__ import annotations

import copy
import inspect
import logging
import pathlib
import re
import uuid
import warnings
from typing import Any, Optional
from urllib.parse import ParseResult, urlparse, urlunparse

from great_expectations.alias_types import PathStr  # noqa: TC001 # FIXME CoP
from great_expectations.compatibility.postgresql import resolve_postgresql_driver
from great_expectations.compatibility.pyparsing import (
    ParseException,
    Word,
    alphanums,
    parse_string,
    set_results_name,
)
from great_expectations.exceptions import StoreConfigurationError
from great_expectations.types import safe_deep_copy
from great_expectations.util import load_class, verify_dynamic_loading_support

try:
    import sqlalchemy as sa  # noqa: TID251 # FIXME CoP
except ImportError:
    sa = None  # type: ignore[assignment] # FIXME CoP

logger = logging.getLogger(__name__)


# TODO: Rename config to constructor_kwargs and config_defaults -> constructor_kwarg_default
# TODO: Improve error messages in this method. Since so much of our workflow is config-driven, this will be a *super* important part of DX.  # noqa: E501 # FIXME CoP
def instantiate_class_from_config(  # noqa: C901 # FIXME CoP
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
            raise KeyError(  # noqa: TRY003 # FIXME CoP
                f"Neither config : {config} nor config_defaults : {config_defaults} contains a module_name key."  # noqa: E501 # FIXME CoP
            )
    else:
        # Pop the value without using it, to avoid sending an unwanted value to the config_class
        config_defaults.pop("module_name", None)

    logger.debug(f"(instantiate_class_from_config) module_name -> {module_name}")
    verify_dynamic_loading_support(module_name=module_name)

    class_name = config.pop("class_name", None)
    if class_name is None:
        logger.warning(
            "Instantiating class from config without an explicit class_name is dangerous. Consider adding "  # noqa: E501 # FIXME CoP
            f"an explicit class_name for {config.get('name')}"
        )
        try:
            class_name = config_defaults.pop("class_name")
        except KeyError:
            raise KeyError(  # noqa: TRY003 # FIXME CoP
                f"Neither config : {config} nor config_defaults : {config_defaults} contains a class_name key."  # noqa: E501 # FIXME CoP
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
            f"Couldn't instantiate class: {class_name} with config: \n\t{format_dict_for_error_message(config_with_defaults)}\n \n"  # noqa: E501 # FIXME CoP
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
    """  # noqa: E501 # FIXME CoP
    substitution_variable_name = set_results_name(
        Word(alphanums + "_"), "substitution_variable_name"
    )
    curly_brace_parser = "${" + substitution_variable_name + "}"
    non_curly_brace_parser = "$" + substitution_variable_name
    both_parser = curly_brace_parser | non_curly_brace_parser
    try:
        parsed_substitution_variable = parse_string(both_parser, substitution_variable)
        return parsed_substitution_variable.substitution_variable_name
    except ParseException:
        return None


# RFC 3986: scheme = ALPHA *( ALPHA / DIGIT / "+" / "-" / "." ). urlparse() rejects anything
# else, so a scheme carrying "_" - "oracle+cx_oracle" - is parsed as if the URL had no scheme.
_RFC_3986_SCHEME_RE = re.compile(r"[a-zA-Z][a-zA-Z0-9+.\-]*")

# A database URL scheme as SQLAlchemy accepts one: RFC 3986 plus "_" ("oracle+cx_oracle").
_DB_URL_SCHEME_RE = re.compile(r"[a-zA-Z][a-zA-Z0-9+.\-_]*")


# The userinfo SQLAlchemy recognises at the start of the part after "://": a username, then an
# optional password running up to the "@" (so it may contain "/", "?" and "#").
_USERINFO_RE = re.compile(r"(?P<username>[^:/]*)(?::(?P<password>[^@]*))?@")


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
    URL_KEYS = {"conn_str", "connection_string", "url"}

    # values with these keys will be directly replaced with cls.MASKED_PASSWORD_STRING:
    PASSWORD_KEYS = {"access_token", "password"}

    # Azure blobstore connection-string fields that are known not to carry a credential, and so
    # may be shown in cleartext. Every other field is masked rather than dropped: the masked
    # string stays a faithful description of what was configured, while a field we do not
    # recognize - including one Azure adds later - is never revealed by default.
    AZURE_SAFE_TO_DISPLAY_FIELDS = {"DefaultEndpointsProtocol", "AccountName", "EndpointSuffix"}

    # Query parameters whose values are known not to carry a credential, and so may be shown in
    # cleartext. Matched case-insensitively, because drivers disagree on capitalisation
    # (`TrustServerCertificate` vs `trustservercertificate`). Every other parameter keeps its
    # name but has its value replaced with cls.MASKED_PASSWORD_STRING, following
    # AZURE_SAFE_TO_DISPLAY_FIELDS: a credential can be passed as a query parameter - a bare
    # `password=`, or a whole `odbc_connect=` connection string - so keeping the query must not
    # reveal it, while the masked string still describes what was actually configured.
    URL_SAFE_TO_DISPLAY_QUERY_PARAMS = {
        "application_name",
        "charset",
        "connect_timeout",
        "database",
        "driver",
        "encoding",
        "encrypt",
        "role",
        "schema",
        "sslmode",
        "trustservercertificate",
        "unix_socket",
        "warehouse",
    }

    @classmethod
    def mask_db_url(cls, url: str, use_urlparse: bool = False, **kwargs) -> str:
        """
        Mask password in database url unless it is a substitution string, e.g. ConfigStr.
        Uses sqlalchemy engine parsing if sqlalchemy is installed, otherwise defaults to using urlparse from the stdlib which does not handle kwargs.
        Args:
            url: Database url e.g. "postgresql+psycopg2://username:password@host:65432/database"
            use_urlparse: Skip trying to parse url with sqlalchemy and use urlparse
            **kwargs: passed to create_engine()

        Returns:
            url with password masked e.g. "postgresql+psycopg2://username:***@host:65432/database"
        """  # noqa: E501 # FIXME CoP

        from great_expectations.datasource.fluent.config_str import ConfigStr

        is_config_str = ConfigStr.str_contains_config_template(url)

        if url.startswith("DefaultEndpointsProtocol"):
            return cls._obfuscate_azure_blobstore_connection_string(url)
        elif is_config_str:
            return url
        elif sa is not None and use_urlparse is False:
            try:
                resolved_url = resolve_postgresql_driver(url)
                engine = sa.create_engine(resolved_url, **kwargs)
                masked_url = engine.url
                if resolved_url is not url:
                    # Describe the URL as configured, not the driver it fell back to.
                    masked_url = masked_url.set(drivername="postgresql")
                # Mask the rendered URL rather than returning it as-is: SQLAlchemy masks the
                # password but copies the query string through verbatim, and a credential can
                # be passed as a query parameter. Routing both paths through the same masking
                # is also what keeps the two renderings equivalent.
                return cls._mask_url(cls._render_masked_url(masked_url))
            # Account for the edge case where we have SQLAlchemy in our env but haven't installed the appropriate dialect to match the input URL  # noqa: E501 # FIXME CoP
            except Exception as e:
                logger.warning(
                    f"Something went wrong when trying to use SQLAlchemy to obfuscate URL: {e}"
                )
        else:
            warnings.warn(
                "SQLAlchemy is not installed, using urlparse to mask database url password which ignores **kwargs."  # noqa: E501 # FIXME CoP
            )
        return cls._mask_url(url=url)

    @staticmethod
    def _render_masked_url(url: Any) -> str:
        """Render a SQLAlchemy URL with its password masked and its database verbatim.

        SQLAlchemy 2.1 percent-encodes the database when rendering a URL, which turns a
        Windows SQLite path such as C:\\path\\to\\foo.db into C%3A%5Cpath%5Cto%5Cfoo.db.
        Earlier versions render the database as given. Render with an alphanumeric
        placeholder, which no version encodes, and put the database back in its place.
        """
        if url.database is None:
            return repr(url)
        placeholder = uuid.uuid4().hex
        return repr(url.set(database=placeholder)).replace(placeholder, url.database, 1)

    @classmethod
    def _obfuscate_azure_blobstore_connection_string(cls, url: str) -> str:
        # Azure connection strings are an unordered set of `key=value` fields separated by `;`.
        # Parse them as such (rather than matching a single fixed field order) so that masking
        # succeeds regardless of field order and with the optional EndpointSuffix omitted.
        # Field order is preserved in the output. No exception raised here may echo the raw url
        # or the AccountKey value, since that would defeat the purpose of masking it.
        fields = [
            (key, value)
            for key, sep, value in (segment.partition("=") for segment in url.split(";"))
            if sep
        ]
        field_values = dict(fields)

        if (
            field_values.get("DefaultEndpointsProtocol") not in ("http", "https")
            or not field_values.get("AccountKey")
            or not re.fullmatch(r"[a-zA-Z0-9]+", field_values.get("AccountName", ""))
            or (
                "EndpointSuffix" in field_values
                and not re.fullmatch(r"[a-zA-Z\.]+", field_values["EndpointSuffix"])
            )
        ):
            raise StoreConfigurationError(  # noqa: TRY003 # FIXME CoP
                "The URL for the Azure connection-string was not configured properly. Please "
                "check that DefaultEndpointsProtocol, AccountName, and AccountKey are present "
                "and valid."
            )

        # Emit every field the connection string carried, masking the value of any field not
        # known to be safe to display. Dropping unrecognized fields instead would make the
        # masked string describe a connection that was never configured - `BlobEndpoint`, for
        # one, overrides the endpoint built from protocol/account/suffix, so omitting it points
        # the output at a different endpoint than the real config uses.
        return ";".join(
            f"{key}={value}"
            if key in cls.AZURE_SAFE_TO_DISPLAY_FIELDS
            else f"{key}={cls.MASKED_PASSWORD_STRING}"
            for key, value in fields
        )

    @classmethod
    def _mask_url(cls, url: str) -> str:
        """Mask the password in the userinfo and every unsafe query value, changing nothing else.

        Both branches of mask_db_url() go through here, so a URL that carries a credential in
        its query string is masked the same way whether or not SQLAlchemy could parse it.
        """
        scheme, _, after_scheme = url.partition(":")

        # Anything that is not "<scheme>://..." is not a database URL - a raw ODBC string such as
        # "Driver={...};Server=tcp:h,1433;Uid=u;Pwd=secret", say - and nothing here can tell its
        # secret apart from the rest. Fail closed and mask it whole.
        if not (_DB_URL_SCHEME_RE.fullmatch(scheme) and after_scheme.startswith("//")):
            return cls.MASKED_PASSWORD_STRING

        parsed_url = cls._parse_db_url(url, scheme, after_scheme)
        if parsed_url is None:
            # Nothing past the scheme can be told apart from a password, so fail closed: masking
            # is this function's one job, and raising instead would fail every config it
            # sanitizes. The scheme was checked above, so it is safe to show.
            return f"{scheme}://{cls.MASKED_PASSWORD_STRING}"

        # Do not parse sqlite, however its driver is spelled: "sqlite+pysqlite://" is the same
        # database as "sqlite://".
        if parsed_url.scheme.partition("+")[0] == "sqlite":
            return url

        # An unencoded "/", "?" or "#" in a password ends the netloc early, so the "@" after it
        # never reaches the netloc and the password would pass through unmasked. That holds even
        # when the netloc does contain an "@" - a username such as "user@server" puts one there.
        # SQLAlchemy takes everything up to the next "@" as the password (`[^@]*`); mask it the
        # same way, then parse again. A rewritten URL rewrites to itself, so this recurses once.
        rewritten = cls._mask_password_ending_the_netloc(url, scheme)
        if rewritten != url:
            return cls._mask_url(rewritten)

        # Replace only the password inside the netloc and re-emit every other component the
        # URL actually carried. Rebuilding the netloc from username/hostname/port drops the
        # query and fragment, renders an IPv6 host without its brackets, and turns an absent
        # username or password into the string "None", so the result describes a connection
        # that was never configured.
        userinfo, has_userinfo, host = parsed_url.netloc.rpartition("@")
        if not has_userinfo:
            masked_netloc = parsed_url.netloc
        elif ":" in userinfo:
            masked_netloc = f"{userinfo.partition(':')[0]}:{cls.MASKED_PASSWORD_STRING}@{host}"
        else:
            masked_netloc = f"{userinfo}@{host}"

        masked_url = urlunparse(
            parsed_url._replace(
                netloc=masked_netloc,
                query=cls._mask_query_string(parsed_url.query),
            )
        )

        # urlunparse() drops an empty authority, so "mssql+pyodbc:///?odbc_connect=..." comes
        # back as "mssql+pyodbc:/?odbc_connect=..." - the "//" the input carried, missing from
        # the output, leaving something that no longer parses as the URL that was configured.
        if not parsed_url.netloc and url.startswith(f"{parsed_url.scheme}://"):
            masked_url = masked_url.replace(f"{parsed_url.scheme}:", f"{parsed_url.scheme}://", 1)

        return masked_url

    @staticmethod
    def _parse_db_url(url: str, scheme: str, after_scheme: str) -> Optional[ParseResult]:
        """Parse a database URL, or return None when urlparse() cannot read its netloc.

        urlparse() rejects "_" in a scheme, so "oracle+cx_oracle://..." parses as if it carried
        no scheme at all, dropping the whole URL - query string included - into `path`, where
        nothing masks it. Parse those as plain "oracle" and put the real scheme back on the
        parsed object. Swapping the string back afterwards instead also rewrites an "oracle"
        that happens to appear in the host or the path.

        urlparse() raises on a netloc it cannot read, such as an unbalanced IPv6 "[".
        """
        try:
            if not _RFC_3986_SCHEME_RE.fullmatch(scheme):
                return urlparse(f"oracle:{after_scheme}")._replace(scheme=scheme)
            return urlparse(url)
        except ValueError:
            return None

    @classmethod
    def _mask_password_ending_the_netloc(cls, url: str, scheme: str) -> str:
        """Mask a password holding an unencoded "/", "?" or "#", which urlparse() splits off."""
        _, authority_separator, after_scheme = url.partition("://")
        credentials = _USERINFO_RE.match(after_scheme) if authority_separator else None
        if not credentials or credentials.group("password") is None:
            return url
        return (
            f"{scheme}://{credentials.group('username')}:{cls.MASKED_PASSWORD_STRING}@"
            f"{after_scheme[credentials.end() :]}"
        )

    @classmethod
    def _mask_query_string(cls, query: str) -> str:
        """Replace the value of every query parameter not known to be safe to display.

        Every name is kept: dropping a parameter would describe a connection that was never
        configured. A parameter written without a value is masked too, so that an unrecognized
        one fails closed.
        """
        return "&".join(
            f"{name}={value}"
            if name.lower() in cls.URL_SAFE_TO_DISPLAY_QUERY_PARAMS
            else f"{name}={cls.MASKED_PASSWORD_STRING}"
            for name, _, value in (param.partition("=") for param in query.split("&"))
            if name
        )

    @classmethod
    def sanitize_config(cls, config: dict) -> dict:  # noqa: C901 #  too complex
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
