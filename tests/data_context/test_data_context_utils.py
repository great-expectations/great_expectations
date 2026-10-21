import logging
import os
import sys
from urllib.parse import parse_qsl, urlsplit

import pytest

import great_expectations.exceptions as gx_exceptions
from great_expectations.data_context.util import (
    PasswordMasker,
    parse_substitution_variable,
)
from great_expectations.exceptions.exceptions import StoreConfigurationError
from great_expectations.types import safe_deep_copy
from great_expectations.util import load_class


@pytest.mark.unit
def test_load_class_raises_error_when_module_not_found():
    with pytest.raises(gx_exceptions.PluginModuleNotFoundError):
        load_class("foo", "bar")


@pytest.mark.unit
def test_load_class_raises_error_when_class_not_found():
    with pytest.raises(gx_exceptions.PluginClassNotFoundError):
        load_class("TotallyNotARealClass", "great_expectations.datasource")


@pytest.mark.unit
def test_load_class_raises_error_when_class_name_is_None():
    with pytest.raises(TypeError):
        load_class(None, "great_expectations.datasource")


@pytest.mark.unit
def test_load_class_raises_error_when_class_name_is_not_string():
    for bad_input in [1, 1.3, ["a"], {"foo": "bar"}]:
        with pytest.raises(TypeError):
            load_class(bad_input, "great_expectations.datasource")


@pytest.mark.unit
def test_load_class_raises_error_when_module_name_is_None():
    with pytest.raises(TypeError):
        load_class("foo", None)


@pytest.mark.unit
def test_load_class_raises_error_when_module_name_is_not_string():
    for bad_input in [1, 1.3, ["a"], {"foo": "bar"}]:
        with pytest.raises(TypeError):
            load_class(bad_input, "great_expectations.datasource")


@pytest.mark.unit
def test_mask_db_url__does_not_mask_config_strings():
    config_str = "${MY_DB_URL}"
    output = PasswordMasker.mask_db_url(config_str)
    assert output == config_str


@pytest.mark.postgresql
def test_mask_db_url__driverless_postgresql_url_with_only_psycopg2_installed(monkeypatch, caplog):
    """A driverless postgresql:// URL masks identically when only psycopg2 is installed.

    SQLAlchemy 2.1 defaults such a URL to psycopg (3). With that driver unimportable the
    psycopg2 fallback is used, and the result must still describe the URL as configured,
    with nothing logged.
    """
    pytest.importorskip("sqlalchemy")
    pytest.importorskip("psycopg2")
    # A None entry in sys.modules makes `import psycopg` raise ImportError.
    monkeypatch.setitem(sys.modules, "psycopg", None)

    with caplog.at_level(logging.DEBUG):
        masked = PasswordMasker.mask_db_url("postgresql://scott:tiger@h:5432/db?sslmode=require")

    assert masked == "postgresql://scott:***@h:5432/db?sslmode=require"
    assert caplog.records == []


@pytest.mark.filesystem
@pytest.mark.filterwarnings(
    "ignore:SQLAlchemy is not installed*:UserWarning:great_expectations.data_context.util"
)
def test_password_masker_mask_db_url(  # noqa: PLR0915, C901 # 11 complexity
    monkeypatch, tmp_path
):
    """
    What does this test and why?
    The PasswordMasker.mask_db_url() should mask passwords consistently inruff  database urls. The output of mask_db_url should be the same whether user_urlparse is set to True or False.
    This test uses database url examples from
    https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls
    """  # noqa: E501 # FIXME CoP
    # PostgreSQL (if installed in test environment)
    # default
    db_hostname = os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost")
    try:
        assert (
            PasswordMasker.mask_db_url(f"postgresql://scott:tiger@{db_hostname}:65432/mydatabase")
            == f"postgresql://scott:{PasswordMasker.MASKED_PASSWORD_STRING}@{db_hostname}:65432/mydatabase"
        )
    except ModuleNotFoundError:
        pass
    assert (
        PasswordMasker.mask_db_url(
            f"postgresql://scott:tiger@{db_hostname}:65432/mydatabase",
            use_urlparse=True,
        )
        == f"postgresql://scott:{PasswordMasker.MASKED_PASSWORD_STRING}@{db_hostname}:65432/mydatabase"
    )
    # missing port number, using urlparse
    assert (
        PasswordMasker.mask_db_url(
            f"postgresql://scott:tiger@{db_hostname}/mydatabase", use_urlparse=True
        )
        == f"postgresql://scott:{PasswordMasker.MASKED_PASSWORD_STRING}@{db_hostname}/mydatabase"
    )

    # psycopg2 (if installed in test environment)
    try:
        assert (
            PasswordMasker.mask_db_url(
                f"postgresql+psycopg2://scott:tiger@{db_hostname}:65432/mydatabase"
            )
            == f"postgresql+psycopg2://scott:{PasswordMasker.MASKED_PASSWORD_STRING}@{db_hostname}:65432/mydatabase"
        )
    except ModuleNotFoundError:
        pass
    assert (
        PasswordMasker.mask_db_url(
            f"postgresql+psycopg2://scott:tiger@{db_hostname}:65432/mydatabase",
            use_urlparse=True,
        )
        == f"postgresql+psycopg2://scott:{PasswordMasker.MASKED_PASSWORD_STRING}@{db_hostname}:65432/mydatabase"
    )

    # pg8000 (if installed in test environment)
    try:
        assert (
            PasswordMasker.mask_db_url(
                f"postgresql+pg8000://scott:tiger@{db_hostname}:65432/mydatabase"
            )
            == f"postgresql+pg8000://scott:{PasswordMasker.MASKED_PASSWORD_STRING}@{db_hostname}:65432/mydatabase"
        )
    except ModuleNotFoundError:
        pass
    assert (
        PasswordMasker.mask_db_url(
            f"postgresql+pg8000://scott:tiger@{db_hostname}:65432/mydatabase",
            use_urlparse=True,
        )
        == f"postgresql+pg8000://scott:{PasswordMasker.MASKED_PASSWORD_STRING}@{db_hostname}:65432/mydatabase"
    )

    # MySQL
    # default (if installed in test environment)
    try:
        assert (
            PasswordMasker.mask_db_url(f"mysql://scott:tiger@{db_hostname}:65432/foo")
            == f"mysql://scott:{PasswordMasker.MASKED_PASSWORD_STRING}@{db_hostname}:65432/foo"
        )
    except ModuleNotFoundError:
        pass

    assert (
        PasswordMasker.mask_db_url(
            f"mysql://scott:tiger@{db_hostname}:65432/foo", use_urlparse=True
        )
        == f"mysql://scott:{PasswordMasker.MASKED_PASSWORD_STRING}@{db_hostname}:65432/foo"
    )

    # mysqlclient (a maintained fork of MySQL-Python) (if installed in test environment)
    try:
        assert (
            PasswordMasker.mask_db_url(f"mysql+mysqldb://scott:tiger@{db_hostname}:65432/foo")
            == f"mysql+mysqldb://scott:{PasswordMasker.MASKED_PASSWORD_STRING}@{db_hostname}:65432/foo"
        )
    except ModuleNotFoundError:
        pass
    assert (
        PasswordMasker.mask_db_url(
            f"mysql+mysqldb://scott:tiger@{db_hostname}:65432/foo", use_urlparse=True
        )
        == f"mysql+mysqldb://scott:{PasswordMasker.MASKED_PASSWORD_STRING}@{db_hostname}:65432/foo"
    )

    # PyMySQL (if installed in test environment)
    try:
        assert (
            PasswordMasker.mask_db_url(f"mysql+pymysql://scott:tiger@{db_hostname}:65432/foo")
            == f"mysql+pymysql://scott:{PasswordMasker.MASKED_PASSWORD_STRING}@{db_hostname}:65432/foo"
        )
    except ModuleNotFoundError:
        pass
    assert (
        PasswordMasker.mask_db_url(
            f"mysql+pymysql://scott:tiger@{db_hostname}:65432/foo", use_urlparse=True
        )
        == f"mysql+pymysql://scott:{PasswordMasker.MASKED_PASSWORD_STRING}@{db_hostname}:65432/foo"
    )

    # Oracle (if installed in test environment)
    url_host = os.getenv("GE_TEST_LOCALHOST_URL", "127.0.0.1")
    try:
        assert (
            PasswordMasker.mask_db_url(f"oracle://scott:tiger@{url_host}:1521/sidname")
            == f"oracle://scott:***@{url_host}:1521/sidname"
        )
    except ModuleNotFoundError:
        pass

    assert (
        PasswordMasker.mask_db_url(
            f"oracle://scott:tiger@{url_host}:1521/sidname", use_urlparse=True
        )
        == f"oracle://scott:{PasswordMasker.MASKED_PASSWORD_STRING}@{url_host}:1521/sidname"
    )

    try:
        assert (
            PasswordMasker.mask_db_url("oracle+cx_oracle://scott:tiger@tnsname")
            == f"oracle+cx_oracle://scott:{PasswordMasker.MASKED_PASSWORD_STRING}@tnsname"
        )
    except ModuleNotFoundError:
        pass
    assert (
        PasswordMasker.mask_db_url("oracle+cx_oracle://scott:tiger@tnsname", use_urlparse=True)
        == f"oracle+cx_oracle://scott:{PasswordMasker.MASKED_PASSWORD_STRING}@tnsname"
    )

    # Microsoft SQL Server
    # pyodbc (if installed in test environment)
    try:
        assert (
            PasswordMasker.mask_db_url("mssql+pyodbc://scott:tiger@mydsn")
            == "mssql+pyodbc://scott:***@mydsn"
        )
    except ModuleNotFoundError:
        pass
    assert (
        PasswordMasker.mask_db_url("mssql+pyodbc://scott:tiger@mydsn", use_urlparse=True)
        == f"mssql+pyodbc://scott:{PasswordMasker.MASKED_PASSWORD_STRING}@mydsn"
    )

    # pymssql driver (if installed in test environment)
    try:
        assert (
            PasswordMasker.mask_db_url(f"mssql+pymssql://scott:tiger@{db_hostname}:12345/dbname")
            == f"mssql+pymssql://scott:{PasswordMasker.MASKED_PASSWORD_STRING}@{db_hostname}:12345/dbname"
        )
    except ModuleNotFoundError:
        pass
    assert (
        PasswordMasker.mask_db_url(
            f"mssql+pymssql://scott:tiger@{db_hostname}:12345/dbname", use_urlparse=True
        )
        == f"mssql+pymssql://scott:{PasswordMasker.MASKED_PASSWORD_STRING}@{db_hostname}:12345/dbname"
    )

    # SQLite
    # relative path
    temp_dir = tmp_path / "sqllite_tests"
    temp_dir.mkdir()
    monkeypatch.chdir(temp_dir)
    assert PasswordMasker.mask_db_url("sqlite:///something/foo.db") == "sqlite:///something/foo.db"
    assert (
        PasswordMasker.mask_db_url("sqlite:///something/foo.db", use_urlparse=True)
        == "sqlite:///something/foo.db"
    )

    # absolute path
    # Unix/Mac - 4 initial slashes in total
    assert (
        PasswordMasker.mask_db_url("sqlite:////absolute/path/to/foo.db")
        == "sqlite:////absolute/path/to/foo.db"
    )
    assert (
        PasswordMasker.mask_db_url("sqlite:////absolute/path/to/foo.db", use_urlparse=True)
        == "sqlite:////absolute/path/to/foo.db"
    )

    # Windows
    assert (
        PasswordMasker.mask_db_url("sqlite:///C:\\path\\to\\foo.db")
        == "sqlite:///C:\\path\\to\\foo.db"
    )
    assert (
        PasswordMasker.mask_db_url("sqlite:///C:\\path\\to\\foo.db", use_urlparse=True)
        == "sqlite:///C:\\path\\to\\foo.db"
    )

    # Windows alternative using raw string
    assert (
        PasswordMasker.mask_db_url(r"sqlite:///C:\path\to\foo.db") == r"sqlite:///C:\path\to\foo.db"
    )
    assert (
        PasswordMasker.mask_db_url(r"sqlite:///C:\path\to\foo.db", use_urlparse=True)
        == r"sqlite:///C:\path\to\foo.db"
    )

    # in-memory
    assert PasswordMasker.mask_db_url("sqlite://") == "sqlite://"
    assert PasswordMasker.mask_db_url("sqlite://", use_urlparse=True) == "sqlite://"


@pytest.mark.unit
def test_sanitize_config_azure_blob_store():
    azure_url: str = "DefaultEndpointsProtocol=https;AccountName=iamname;AccountKey=i_am_account_key;EndpointSuffix=core.windows.net"  # noqa: E501 # FIXME CoP
    assert (
        PasswordMasker.mask_db_url(azure_url)
        == "DefaultEndpointsProtocol=https;AccountName=iamname;AccountKey=***;EndpointSuffix=core.windows.net"  # noqa: E501 # FIXME CoP
    )

    azure_wrong_url: str = "DefaultEndpointsProtocol=i_dont_work;AccountName=iamname;AccountKey=i_am_account_key;EndpointSuffix=core.windows.net"  # noqa: E501 # FIXME CoP
    with pytest.raises(StoreConfigurationError):
        PasswordMasker.mask_db_url(azure_wrong_url)

    azure_missing_fields: str = (
        "DefaultEndpointsProtocol=i_dont_work;AccountName=iamname;EndpointSuffix=core.windows.net"
    )
    with pytest.raises(StoreConfigurationError):
        PasswordMasker.mask_db_url(azure_missing_fields)


@pytest.mark.unit
@pytest.mark.parametrize(
    "connection_string,expected",
    [
        pytest.param(
            "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=iamname;AccountKey=i_am_account_key",
            "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=iamname;AccountKey=***",
            id="fields_reordered",
        ),
        pytest.param(
            "DefaultEndpointsProtocol=https;AccountName=iamname;AccountKey=i_am_account_key",
            "DefaultEndpointsProtocol=https;AccountName=iamname;AccountKey=***",
            id="endpoint_suffix_omitted",
        ),
        pytest.param(
            "DefaultEndpointsProtocol=https;AccountName=iamname;AccountKey=i_am_account_key;EndpointSuffix=core.windows.net;",
            "DefaultEndpointsProtocol=https;AccountName=iamname;AccountKey=***;EndpointSuffix=core.windows.net",
            id="trailing_semicolon",
        ),
        pytest.param(
            "DefaultEndpointsProtocol=https;AccountName=iamname;AccountKey=i_am_account_key;SharedAccessSignature=i_am_a_sas_token",
            "DefaultEndpointsProtocol=https;AccountName=iamname;AccountKey=***;SharedAccessSignature=***",
            id="sas_token_masked_not_dropped",
        ),
        pytest.param(
            "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=i_am_account_key;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1",
            "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=***;BlobEndpoint=***;QueueEndpoint=***",
            id="emulator_endpoint_fields_masked_not_dropped",
        ),
        pytest.param(
            "DefaultEndpointsProtocol=https;AccountName=iamname;SomeFieldAzureAddedLater=i_am_a_new_secret;AccountKey=i_am_account_key",
            "DefaultEndpointsProtocol=https;AccountName=iamname;SomeFieldAzureAddedLater=***;AccountKey=***",
            id="unknown_field_masked_by_default",
        ),
    ],
)
def test_azure_connection_string_masked_regardless_of_field_order(connection_string, expected):
    assert PasswordMasker.mask_db_url(connection_string) == expected


@pytest.mark.unit
def test_azure_safe_to_display_fields_is_limited_to_known_non_credentials():
    """Widening this allowlist reveals a field in cleartext, so it is pinned deliberately.

    Notably absent: AccountKey and SharedAccessSignature are credentials, and the *Endpoint
    fields can carry a SAS token in their query string.
    """
    expected_safe_fields = {"DefaultEndpointsProtocol", "AccountName", "EndpointSuffix"}
    assert expected_safe_fields == PasswordMasker.AZURE_SAFE_TO_DISPLAY_FIELDS


@pytest.mark.unit
def test_azure_connection_string_masking_drops_no_field():
    """A masked string that silently omits a field describes a connection nobody configured.

    `BlobEndpoint` overrides the endpoint built from protocol/account/suffix, so dropping it
    would render output pointing at a different endpoint than the real config uses.
    """
    connection_string = (
        "DefaultEndpointsProtocol=https;AccountName=iamname;AccountKey=i_am_account_key;"
        "EndpointSuffix=core.windows.net;BlobEndpoint=https://private.example.net/iamname;"
        "SharedAccessSignature=i_am_a_sas_token"
    )
    masked = PasswordMasker.mask_db_url(connection_string)

    def keys(value: str) -> list[str]:
        return [segment.partition("=")[0] for segment in value.split(";")]

    assert keys(masked) == keys(connection_string)


@pytest.mark.unit
def test_azure_connection_string_masks_every_value_not_safe_to_display():
    """Fail closed: only the allowlisted fields survive in cleartext, every other value is ***."""
    unsafe_values = {
        "AccountKey": "i_am_account_key",
        "SharedAccessSignature": "i_am_a_sas_token",
        "BlobEndpoint": "https://private.example.net/iamname",
        "SomeFieldAzureAddedLater": "i_am_a_new_secret",
    }
    connection_string = ";".join(
        ["DefaultEndpointsProtocol=https", "AccountName=iamname", "EndpointSuffix=core.windows.net"]
        + [f"{key}={value}" for key, value in unsafe_values.items()]
    )
    masked = PasswordMasker.mask_db_url(connection_string)

    for key, value in unsafe_values.items():
        assert value not in masked, f"{key} value leaked into masked output"
        assert f"{key}={PasswordMasker.MASKED_PASSWORD_STRING}" in masked


@pytest.mark.unit
def test_azure_connection_string_account_key_never_appears_in_raised_error():
    """A masking failure must not put the secret it was masking into the traceback."""
    account_key = "i_am_account_key"
    malformed = f"DefaultEndpointsProtocol=nonsense;AccountName=iamname;AccountKey={account_key}"
    with pytest.raises(StoreConfigurationError) as exc_info:
        PasswordMasker.mask_db_url(malformed)
    assert account_key not in str(exc_info.value)


@pytest.mark.unit
def test_azure_connection_string_raises_for_invalid_account_name():
    """AccountName must be alphanumeric; a hyphen should be rejected."""
    connection_string = (
        "DefaultEndpointsProtocol=https;AccountName=iam-name;AccountKey=i_am_account_key"
    )
    with pytest.raises(StoreConfigurationError):
        PasswordMasker.mask_db_url(connection_string)


@pytest.mark.unit
def test_azure_connection_string_raises_for_invalid_endpoint_suffix():
    """EndpointSuffix must match [a-zA-Z.]+; digits/symbols should be rejected."""
    connection_string = (
        "DefaultEndpointsProtocol=https;AccountName=iamname;AccountKey=i_am_account_key;"
        "EndpointSuffix=core.windows.net123"
    )
    with pytest.raises(StoreConfigurationError):
        PasswordMasker.mask_db_url(connection_string)


@pytest.mark.unit
def test_sanitize_config_raises_exception_with_bad_input(
    basic_data_context_config,
):
    # expect that an Exception is raised if something other than a dict is passed
    with pytest.raises(TypeError):
        PasswordMasker.sanitize_config(basic_data_context_config)


@pytest.mark.unit
def test_sanitize_config_doesnt_change_config_without_datasources(
    basic_data_context_config_dict,
):
    # expect no change without datasources
    config_without_creds = PasswordMasker.sanitize_config(basic_data_context_config_dict)
    assert config_without_creds == basic_data_context_config_dict


@pytest.mark.cloud
def test_sanitize_config_masks_cloud_store_backend_access_tokens(
    data_context_config_dict_with_cloud_backed_stores, ge_cloud_access_token
):
    # test that cloud store backend tokens have been properly masked
    config_with_creds_in_stores = PasswordMasker.sanitize_config(
        data_context_config_dict_with_cloud_backed_stores
    )
    for name, store_config in config_with_creds_in_stores["stores"].items():
        if (
            not store_config.get("store_backend")
            or not store_config["store_backend"].get("ge_cloud_credentials")
            or not store_config["store_backend"]["ge_cloud_credentials"].get("access_token")
        ):
            # a field in store_config["store_backend"]["ge_cloud_credentials"]["access_token"]
            # doesn't exist, so we expect this config to be unchanged
            assert store_config == data_context_config_dict_with_cloud_backed_stores["stores"][name]
        else:
            # check that the original token exists
            assert (
                data_context_config_dict_with_cloud_backed_stores["stores"][name]["store_backend"][
                    "ge_cloud_credentials"
                ]["access_token"]
                == ge_cloud_access_token
            )
            # expect that the GX Cloud token has been obscured
            assert (
                store_config["store_backend"]["ge_cloud_credentials"]["access_token"]
                != ge_cloud_access_token
            )


@pytest.mark.unit
def test_sanitize_config_with_arbitrarily_nested_sensitive_keys():
    # base case - this config should pass through unaffected
    config = {
        "some_field": "and a value",
        "some_other_field": {"password": "expect this to be found"},
    }
    config_copy = safe_deep_copy(config)
    res = PasswordMasker.sanitize_config(config_copy)
    assert res != config
    assert res["some_other_field"]["password"] == PasswordMasker.MASKED_PASSWORD_STRING


@pytest.mark.unit
def test_sanitize_config_with_password_field():
    # this case has a password field inside a credentials dict - expect it to be masked
    config = {"credentials": {"password": "my-super-duper-secure-passphrase-123"}}
    config_copy = safe_deep_copy(config)
    res = PasswordMasker.sanitize_config(config_copy)
    assert res != config
    assert res["credentials"]["password"] == PasswordMasker.MASKED_PASSWORD_STRING


@pytest.mark.unit
def test_sanitize_config_with_url_field(conn_string_with_embedded_password, conn_string_password):
    # this case has a url field inside a credentials dict - expect the password inside
    # of it to be masked
    config = {"credentials": {"url": conn_string_with_embedded_password}}
    config_copy = safe_deep_copy(config)
    res = PasswordMasker.sanitize_config(config_copy)
    assert res != config
    assert conn_string_password not in res["credentials"]["url"]
    assert PasswordMasker.MASKED_PASSWORD_STRING in res["credentials"]["url"]


@pytest.mark.parametrize("key", ["connection_string", "conn_str"])
@pytest.mark.unit
def test_sanitize_config_with_nested_url_field(
    conn_string_password,
    conn_string_with_embedded_password,
    key: str,
):
    # this case has a connection string in an execution_engine dict
    config = {"execution_engine": {key: conn_string_with_embedded_password}}
    config_copy = safe_deep_copy(config)
    res = PasswordMasker.sanitize_config(config_copy)
    assert res != config
    assert conn_string_password not in res["execution_engine"][key]
    assert PasswordMasker.MASKED_PASSWORD_STRING in res["execution_engine"][key]


@pytest.mark.unit
def test_sanitize_config_regardless_of_parent_key():
    # expect this config still be masked
    config = {
        "some_field": "and a value",
        "some_other_field": {"access_token": "but this won't be found"},
    }
    config_copy = safe_deep_copy(config)
    res = PasswordMasker.sanitize_config(config_copy)
    assert res != config
    assert res["some_other_field"]["access_token"] == PasswordMasker.MASKED_PASSWORD_STRING


@pytest.mark.cloud
def test_sanitize_config_masks_cloud_access_token(ge_cloud_access_token):
    # expect the access token to be found and masked
    config = {"store_backend": {"ge_cloud_credentials": {"access_token": ge_cloud_access_token}}}
    config_copy = safe_deep_copy(config)
    res = PasswordMasker.sanitize_config(config_copy)
    assert res != config
    assert (
        res["store_backend"]["ge_cloud_credentials"]["access_token"]
        == PasswordMasker.MASKED_PASSWORD_STRING
    )


@pytest.mark.unit
def test_sanitize_config_works_with_list():
    config = {"some_key": [{"access_token": "12345"}]}
    config_copy = safe_deep_copy(config)
    res = PasswordMasker.sanitize_config(config_copy)
    assert res != config
    assert res["some_key"][0]["access_token"] == PasswordMasker.MASKED_PASSWORD_STRING


@pytest.mark.unit
@pytest.mark.filterwarnings(
    "ignore:SQLAlchemy is not installed*:UserWarning:great_expectations.data_context.util"
)
@pytest.mark.parametrize(
    ("url", "expected"),
    [
        pytest.param(
            "mssql+pyodbc://scott:tiger@h:1433/db?driver=ODBC+Driver+17&TrustServerCertificate=yes",
            "mssql+pyodbc://scott:***@h:1433/db?driver=ODBC+Driver+17&TrustServerCertificate=yes",
            id="query_string_survives_masking",
        ),
        pytest.param(
            "postgresql://scott:tiger@h:5432/db?sslmode=require",
            f"postgresql://scott:{PasswordMasker.MASKED_PASSWORD_STRING}@h:5432/db?sslmode=require",
            id="single_query_parameter",
        ),
        pytest.param(
            "postgresql://scott:tiger@h:5432/db#section",
            "postgresql://scott:***@h:5432/db#section",
            id="fragment_survives_masking",
        ),
        pytest.param(
            "postgresql://scott:tiger@[::1]:5432/db?sslmode=require",
            "postgresql://scott:***@[::1]:5432/db?sslmode=require",
            id="ipv6_host_keeps_its_brackets",
        ),
        pytest.param(
            "bigquery://my-project/dataset",
            "bigquery://my-project/dataset",
            id="url_without_credentials_is_left_alone",
        ),
        pytest.param(
            "postgresql://scott@h:5432/db",
            "postgresql://scott@h:5432/db",
            id="username_without_password_is_not_given_a_masked_secret",
        ),
        pytest.param(
            "mysql+pymysql://scott:tiger@/db?unix_socket=/tmp/mysql.sock",
            "mysql+pymysql://scott:***@/db?unix_socket=/tmp/mysql.sock",
            id="empty_host_stays_empty",
        ),
        pytest.param(
            "oracle+cx_oracle://scott:tiger@tnsname?ssl_cert=/tmp/c.pem",
            "oracle+cx_oracle://scott:***@tnsname?ssl_cert=***",
            id="oracle_prefix_swap_masks_unknown_query_param",
        ),
        pytest.param(
            "oracle+cx_oracle://scott:tiger@oracle-db.example.com:1521/sid",
            "oracle+cx_oracle://scott:***@oracle-db.example.com:1521/sid",
            id="host_containing_oracle_is_not_rewritten",
        ),
        pytest.param(
            "mssql+pyodbc:///?odbc_connect=Server%3Dh%3BPWD%3Dhunter2",
            "mssql+pyodbc:///?odbc_connect=***",
            id="empty_authority_keeps_its_slashes",
        ),
    ],
)
def test_mask_db_url_with_urlparse_preserves_everything_but_the_password(url, expected):
    """The fallback must hide the secret and change nothing else about the URL.

    Rebuilding the netloc from username/hostname/port dropped the query and fragment, rendered
    an IPv6 host without brackets, and turned an absent username or password into "None".
    """
    assert PasswordMasker.mask_db_url(url, use_urlparse=True) == expected


@pytest.mark.unit
@pytest.mark.filterwarnings(
    "ignore:SQLAlchemy is not installed*:UserWarning:great_expectations.data_context.util"
)
@pytest.mark.parametrize(
    "url",
    [
        "mssql+pyodbc://scott:tiger@h:1433/db?driver=ODBC+Driver+17",
        "postgresql://scott:tiger@[::1]:5432/db?sslmode=require",
        "snowflake://user:pass@acct/db?role=PUBLIC",
        "oracle+cx_oracle://scott:tiger@tnsname",
    ],
)
def test_mask_db_url_with_urlparse_never_echoes_the_password(url):
    """Masking is the one job this function has; the secret may not survive it anywhere."""
    password = url.partition("://")[2].partition("@")[0].partition(":")[2]
    masked = PasswordMasker.mask_db_url(url, use_urlparse=True)
    assert password not in masked
    assert PasswordMasker.MASKED_PASSWORD_STRING in masked


@pytest.mark.unit
@pytest.mark.filterwarnings(
    "ignore:SQLAlchemy is not installed*:UserWarning:great_expectations.data_context.util"
)
@pytest.mark.parametrize(
    ("url", "expected"),
    [
        pytest.param(
            "postgresql://scott:pa/ss@h:5432/db",
            "postgresql://scott:***@h:5432/db",
            id="slash_in_password",
        ),
        pytest.param(
            "postgresql://scott:pa#ss@h/db",
            "postgresql://scott:***@h/db",
            id="hash_in_password",
        ),
        pytest.param(
            "postgresql://scott:pa?ss@h/db",
            "postgresql://scott:***@h/db",
            id="question_mark_in_password",
        ),
        # A "user@server" username (Azure SQL, Azure Postgres single server) or an email
        # username (Snowflake) puts an "@" inside the netloc urlparse() sees, ahead of the one
        # that ends the password.
        pytest.param(
            "mssql+pyodbc://user@myserver:pa/ss@myserver.database.windows.net:1433/db?driver=ODBC+Driver+18",
            "mssql+pyodbc://user@myserver:***@myserver.database.windows.net:1433/db?driver=ODBC+Driver+18",
            id="at_in_username_slash_in_password",
        ),
        pytest.param(
            "snowflake://me@corp.com:p#w@acct/db",
            "snowflake://me@corp.com:***@acct/db",
            id="at_in_username_hash_in_password",
        ),
        pytest.param(
            "postgresql://user@srv:Pa?ss@srv.postgres.database.azure.com/db",
            "postgresql://user@srv:***@srv.postgres.database.azure.com/db",
            id="at_in_username_question_mark_in_password",
        ),
        pytest.param(
            "postgresql://user@srv:plainpw@srv.postgres.database.azure.com/db",
            "postgresql://user@srv:***@srv.postgres.database.azure.com/db",
            id="at_in_username_plain_password",
        ),
    ],
)
def test_mask_db_url_with_urlparse_masks_a_password_holding_a_url_delimiter(url, expected):
    """An unencoded delimiter in the password ends the netloc early; it must still be masked."""
    assert PasswordMasker.mask_db_url(url, use_urlparse=True) == expected


@pytest.mark.unit
@pytest.mark.filterwarnings(
    "ignore:SQLAlchemy is not installed*:UserWarning:great_expectations.data_context.util"
)
@pytest.mark.parametrize(
    "url",
    [
        "sqlite:///foo.db?mode=ro&uri=true",
        "sqlite+pysqlite:///foo.db?mode=ro&uri=true",
    ],
)
@pytest.mark.parametrize("use_urlparse", [True, False], ids=["urlparse", "sqlalchemy"])
def test_mask_db_url_leaves_sqlite_urls_alone_whatever_the_driver(url, use_urlparse):
    assert PasswordMasker.mask_db_url(url, use_urlparse=use_urlparse) == url


@pytest.mark.unit
@pytest.mark.filterwarnings(
    "ignore:SQLAlchemy is not installed*:UserWarning:great_expectations.data_context.util"
)
@pytest.mark.parametrize(
    "url",
    [
        "postgresql://scott:tiger@h:5432/db?sslmode=require",
        "postgresql://scott:tiger@[::1]:5432/db?sslmode=require",
        "postgresql://scott@h:5432/db",
        "bigquery://my-project/dataset",
        "sqlite:///foo/bar.db",
        # More than one parameter: SQLAlchemy renders its own ordering, so this could not be
        # compared byte for byte even before query masking was added.
        "mssql+pyodbc://scott:tiger@h:1433/db?driver=ODBC+Driver+17&TrustServerCertificate=yes",
        "snowflake://user:pass@acct/db?role=PUBLIC&warehouse=WH",
        "postgresql://scott:tiger@h:5432/db?sslmode=require&password=hunter2",
        "postgresql://scott:tiger@h:5432/db#section",
        "mysql+pymysql://scott:tiger@/db?unix_socket=/tmp/mysql.sock",
        "oracle+cx_oracle://scott:tiger@tnsname?ssl_cert=/tmp/c.pem",
        "oracle+cx_oracle://scott:tiger@oracle-db.example.com:1521/sid",
        "mssql+pyodbc:///?odbc_connect=Server%3Dh%3BPWD%3Dhunter2",
    ],
)
def test_mask_db_url_with_urlparse_matches_sqlalchemy_rendering(url, caplog):
    """Both paths must agree on every component of a masked URL, not merely on its password.

    This is the invariant `test_password_masker_mask_db_url` states in its docstring. The two
    are compared component-wise rather than byte for byte, because SQLAlchemy re-orders and
    percent-encodes a query dict of its own making: the difference is presentation only, never
    a dropped, invented or differently-masked component.

    When the URL's dialect driver is not installed, `mask_db_url` falls back to the same
    function the urlparse path uses, so the comparison would be between a path and itself. That
    case is skipped rather than counted as a pass.
    """
    pytest.importorskip("sqlalchemy")

    via_urlparse = PasswordMasker.mask_db_url(url, use_urlparse=True)
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="great_expectations.data_context.util"):
        via_sqlalchemy = PasswordMasker.mask_db_url(url, use_urlparse=False)
    if any("Something went wrong when trying to use SQLAlchemy" in m for m in caplog.messages):
        pytest.skip(f"SQLAlchemy cannot build an engine for {url!r} here; both paths are the same")
    assert _components(via_urlparse) == _components(via_sqlalchemy)


def _components(url: str) -> tuple:
    """The parts of a URL that describe the connection, ignoring presentation-only differences."""
    parsed = urlsplit(url)
    return (
        parsed.scheme,
        parsed.username,
        parsed.password,
        parsed.hostname,
        parsed.port,
        parsed.path,
        parsed.fragment,
        frozenset(parse_qsl(parsed.query, keep_blank_values=True)),
    )


@pytest.mark.unit
@pytest.mark.filterwarnings(
    "ignore:SQLAlchemy is not installed*:UserWarning:great_expectations.data_context.util"
)
@pytest.mark.parametrize(
    ("url", "expected_query"),
    [
        pytest.param(
            "mssql+pyodbc://scott:tiger@h:1433/db?driver=ODBC+Driver+17&TrustServerCertificate=yes",
            "driver=ODBC+Driver+17&TrustServerCertificate=yes",
            id="allowlisted_parameters_stay_visible_case_insensitively",
        ),
        pytest.param(
            "postgresql://scott:tiger@h:5432/db?password=hunter2",
            "password=***",
            id="password_query_parameter_is_masked",
        ),
        pytest.param(
            "mssql+pyodbc://scott:tiger@h:1433/db?odbc_connect=Server%3Dh%3BPWD%3Dhunter2",
            "odbc_connect=***",
            id="whole_odbc_connect_value_is_masked",
        ),
        pytest.param(
            "postgresql://scott:tiger@h:5432/db?sslmode=require&secret=s3cr3t&driver=psqlodbc",
            "sslmode=require&secret=***&driver=psqlodbc",
            id="allowlisted_and_unrecognized_mixed",
        ),
    ],
)
@pytest.mark.parametrize("use_urlparse", [True, False], ids=["urlparse", "sqlalchemy"])
def test_mask_db_url_masks_credentials_in_the_query_string_on_both_paths(
    url, expected_query, use_urlparse
):
    """A credential can be passed as a query parameter, so keeping the query must not reveal it.

    Runs both paths: the fallback, and SQLAlchemy where available. Query masking is one
    implementation shared by both, so neither can be the one that leaks.
    """
    masked = PasswordMasker.mask_db_url(url, use_urlparse=use_urlparse)
    assert dict(parse_qsl(urlsplit(masked).query)) == dict(parse_qsl(expected_query))
    assert "hunter2" not in masked
    assert "s3cr3t" not in masked
    assert "PWD" not in masked


@pytest.mark.unit
def test_mask_db_url_masks_credential_in_query_when_sqlalchemy_cannot_render_the_url():
    """The query is masked even when the URL is one SQLAlchemy cannot build an engine for.

    This is the case the fallback exists for, and the one where a leaked parameter would be
    least likely to be noticed: no other path is taken, so nothing else masks the value.
    """
    url = "mssql+pyodbc://scott:tiger@h:1433/db?odbc_connect=Server%3Dh%3BPWD%3Dhunter2"
    assert PasswordMasker.mask_db_url(url) == "mssql+pyodbc://scott:***@h:1433/db?odbc_connect=***"


@pytest.mark.unit
@pytest.mark.filterwarnings(
    "ignore:SQLAlchemy is not installed*:UserWarning:great_expectations.data_context.util"
)
@pytest.mark.parametrize(
    "url",
    [
        # A non-numeric port is the unparseable case: SQLAlchemy raises on it, and the
        # stdlib's `.port` raises too, so the fallback must not reach for that property.
        "postgresql://scott:tiger@host:notaport/db",
        "postgresql://scott:tiger@host:notaport/db?password=hunter2",
    ],
)
def test_mask_db_url_never_returns_an_unparseable_url_unmasked(url):
    """Masking the secret is this function's one job, and it holds even when parsing fails.

    Returning the input unchanged, or naming it in an exception or log message, would put the
    password straight back into the output.
    """
    masked = PasswordMasker.mask_db_url(url, use_urlparse=True)
    assert "tiger" not in masked
    assert "hunter2" not in masked
    assert PasswordMasker.MASKED_PASSWORD_STRING in masked


@pytest.mark.unit
@pytest.mark.filterwarnings(
    "ignore:SQLAlchemy is not installed*:UserWarning:great_expectations.data_context.util"
)
@pytest.mark.parametrize(
    "url",
    [
        # Read the way SQLAlchemy reads a URL, the "@" in the query ends a password that starts
        # inside the IPv6 brackets, leaving an unbalanced "[" that urlparse() refuses.
        pytest.param("postgresql://user@[::1]:5432/db?x=a@b", id="ipv6_user_and_later_at"),
        pytest.param("postgresql://[::1]:5432/db?x=a@b", id="ipv6_no_userinfo_and_later_at"),
        pytest.param("postgresql://[%e9:SECRETZ@", id="unbalanced_ipv6_bracket"),
    ],
)
def test_mask_db_url_with_urlparse_fails_closed_when_the_netloc_cannot_be_parsed(url):
    """A netloc urlparse() cannot read is masked whole, never raised on or passed through."""
    assert PasswordMasker.mask_db_url(url, use_urlparse=True) == "postgresql://***"


@pytest.mark.unit
@pytest.mark.filterwarnings(
    "ignore:SQLAlchemy is not installed*:UserWarning:great_expectations.data_context.util"
)
@pytest.mark.parametrize("use_urlparse", [True, False], ids=["urlparse", "sqlalchemy"])
@pytest.mark.parametrize(
    "value",
    [
        pytest.param(
            "Driver={ODBC Driver 18 for SQL Server};Server=tcp:myserver.database.windows.net,1433;"
            "Database=db;Uid=user;Pwd=SECRET;Encrypt=yes",
            id="odbc_connection_string",
        ),
        pytest.param(
            "Uid=sa;Pwd=SECRET;Server=tcp://h", id="odbc_string_containing_scheme_slashes"
        ),
        pytest.param(
            "Uid=sa;Pwd=SECRET;Server=tcp://[fe80::1", id="odbc_string_with_unbalanced_ipv6"
        ),
        pytest.param("scott:SECRET@h/db", id="userinfo_without_scheme"),
        pytest.param("//scott:SECRET@h/db", id="authority_without_scheme"),
    ],
)
def test_mask_db_url_masks_a_value_that_is_not_a_url_whole(value, use_urlparse):
    """Text that is not "<scheme>://..." has no part masking could safely leave readable."""
    assert PasswordMasker.mask_db_url(value, use_urlparse=use_urlparse) == "***"


@pytest.mark.unit
@pytest.mark.filterwarnings(
    "ignore:SQLAlchemy is not installed*:UserWarning:great_expectations.data_context.util"
)
def test_sanitize_config_masks_a_raw_odbc_connection_string():
    """A raw ODBC string under a URL key must not reach the sanitized config with its Pwd=."""
    odbc = (
        "Driver={ODBC Driver 18 for SQL Server};Server=tcp:myserver.database.windows.net,1433;"
        "Database=db;Uid=user;Pwd=SECRET;Encrypt=yes"
    )
    assert PasswordMasker.sanitize_config({"connection_string": odbc}) == {
        "connection_string": "***"
    }


@pytest.mark.unit
@pytest.mark.filterwarnings(
    "ignore:SQLAlchemy is not installed*:UserWarning:great_expectations.data_context.util"
)
def test_sanitize_config_does_not_raise_on_an_unparseable_ipv6_url_when_no_driver_loads(
    monkeypatch,
):
    """With no driver SQLAlchemy can load, the fallback masks the URL rather than raising."""
    monkeypatch.setitem(sys.modules, "psycopg2", None)
    monkeypatch.setitem(sys.modules, "psycopg", None)
    config = {"url": "postgresql://user@[::1]:5432/db?x=a@b"}
    assert PasswordMasker.sanitize_config(config) == {"url": "postgresql://***"}


@pytest.mark.unit
def test_mask_db_url_does_not_echo_an_unparseable_url_in_its_warning(caplog):
    """A failed parse must not put the URL it was masking into the log."""
    url = "postgresql://scott:tiger@host:notaport/db"
    with caplog.at_level(logging.WARNING):
        PasswordMasker.mask_db_url(url)
    assert "tiger" not in caplog.text


@pytest.mark.unit
def test_parse_substitution_variable():
    """
    What does this test and why?
    Ensure parse_substitution_variable works as expected.
    Returns:

    """
    assert parse_substitution_variable("${SOME_VAR}") == "SOME_VAR"
    assert parse_substitution_variable("$SOME_VAR") == "SOME_VAR"
    assert parse_substitution_variable("SOME_STRING") is None
    assert parse_substitution_variable("SOME_$TRING") is None
    assert parse_substitution_variable("${some_var}") == "some_var"
    assert parse_substitution_variable("$some_var") == "some_var"
    assert parse_substitution_variable("some_string") is None
    assert parse_substitution_variable("some_$tring") is None
    assert parse_substitution_variable("${SOME_$TRING}") is None
    assert parse_substitution_variable("$SOME_$TRING") == "SOME_"
