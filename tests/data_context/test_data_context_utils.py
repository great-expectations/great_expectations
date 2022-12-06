import os

import pytest

import great_expectations.exceptions as gee
from great_expectations.data_context.util import (
    PasswordMasker,
    parse_substitution_variable,
)
from great_expectations.types import safe_deep_copy
from great_expectations.util import load_class


def test_load_class_raises_error_when_module_not_found():
    with pytest.raises(gee.PluginModuleNotFoundError):
        load_class("foo", "bar")


def test_load_class_raises_error_when_class_not_found():
    with pytest.raises(gee.PluginClassNotFoundError):
        load_class("TotallyNotARealClass", "great_expectations.datasource")


def test_load_class_raises_error_when_class_name_is_None():
    with pytest.raises(TypeError):
        load_class(None, "great_expectations.datasource")


def test_load_class_raises_error_when_class_name_is_not_string():
    for bad_input in [1, 1.3, ["a"], {"foo": "bar"}]:
        with pytest.raises(TypeError):
            load_class(bad_input, "great_expectations.datasource")


def test_load_class_raises_error_when_module_name_is_None():
    with pytest.raises(TypeError):
        load_class("foo", None)


def test_load_class_raises_error_when_module_name_is_not_string():
    for bad_input in [1, 1.3, ["a"], {"foo": "bar"}]:
        with pytest.raises(TypeError):
            load_class(bad_input, "great_expectations.datasource")


@pytest.mark.filterwarnings(
    "ignore:SQLAlchemy is not installed*:UserWarning:great_expectations.data_context.util"
)
def test_password_masker_mask_db_url(monkeypatch, tmp_path):
    """
    What does this test and why?
    The PasswordMasker.mask_db_url() should mask passwords consistently in database urls. The output of mask_db_url should be the same whether user_urlparse is set to True or False.
    This test uses database url examples from
    https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls
    """
    # PostgreSQL (if installed in test environment)
    # default
    db_hostname = os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost")
    try:
        assert (
            PasswordMasker.mask_db_url(
                f"postgresql://scott:tiger@{db_hostname}:65432/mydatabase"
            )
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
            PasswordMasker.mask_db_url(
                f"mysql+mysqldb://scott:tiger@{db_hostname}:65432/foo"
            )
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
            PasswordMasker.mask_db_url(
                f"mysql+pymysql://scott:tiger@{db_hostname}:65432/foo"
            )
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
        PasswordMasker.mask_db_url(
            "oracle+cx_oracle://scott:tiger@tnsname", use_urlparse=True
        )
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
        PasswordMasker.mask_db_url(
            "mssql+pyodbc://scott:tiger@mydsn", use_urlparse=True
        )
        == f"mssql+pyodbc://scott:{PasswordMasker.MASKED_PASSWORD_STRING}@mydsn"
    )

    # pymssql (if installed in test environment)
    try:
        assert (
            PasswordMasker.mask_db_url(
                f"mssql+pymssql://scott:tiger@{db_hostname}:12345/dbname"
            )
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
    assert (
        PasswordMasker.mask_db_url("sqlite:///something/foo.db")
        == "sqlite:///something/foo.db"
    )
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
        PasswordMasker.mask_db_url(
            "sqlite:////absolute/path/to/foo.db", use_urlparse=True
        )
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
        PasswordMasker.mask_db_url(r"sqlite:///C:\path\to\foo.db")
        == r"sqlite:///C:\path\to\foo.db"
    )
    assert (
        PasswordMasker.mask_db_url(r"sqlite:///C:\path\to\foo.db", use_urlparse=True)
        == r"sqlite:///C:\path\to\foo.db"
    )

    # in-memory
    assert PasswordMasker.mask_db_url("sqlite://") == "sqlite://"
    assert PasswordMasker.mask_db_url("sqlite://", use_urlparse=True) == "sqlite://"


def test_sanitize_config_raises_exception_with_bad_input(
    basic_data_context_config,
):

    # expect that an Exception is raised if something other than a dict is passed
    with pytest.raises(TypeError):
        PasswordMasker.sanitize_config(basic_data_context_config)


def test_sanitize_config_doesnt_change_config_without_datasources(
    basic_data_context_config_dict,
):

    # expect no change without datasources
    config_without_creds = PasswordMasker.sanitize_config(
        basic_data_context_config_dict
    )
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
            or not store_config["store_backend"]["ge_cloud_credentials"].get(
                "access_token"
            )
        ):
            # a field in store_config["store_backend"]["ge_cloud_credentials"]["access_token"]
            # doesn't exist, so we expect this config to be unchanged
            assert (
                store_config
                == data_context_config_dict_with_cloud_backed_stores["stores"][name]
            )
        else:
            # check that the original token exists
            assert (
                data_context_config_dict_with_cloud_backed_stores["stores"][name][
                    "store_backend"
                ]["ge_cloud_credentials"]["access_token"]
                == ge_cloud_access_token
            )
            # expect that the GX Cloud token has been obscured
            assert (
                store_config["store_backend"]["ge_cloud_credentials"]["access_token"]
                != ge_cloud_access_token
            )


def test_sanitize_config_masks_execution_engine_connection_strings(
    data_context_config_dict_with_datasources, conn_string_password
):

    # test that datasource credentials have been properly masked
    unaltered_datasources = data_context_config_dict_with_datasources["datasources"]
    config_with_creds_masked = PasswordMasker.sanitize_config(
        data_context_config_dict_with_datasources
    )
    masked_datasources = config_with_creds_masked["datasources"]

    # iterate through the processed datasources and check for correctness
    for name, processed_config in masked_datasources.items():

        # check if processed_config["execution_engine"]["connection_string"] exists
        if processed_config.get("execution_engine") and processed_config[
            "execution_engine"
        ].get("connection_string"):

            # check if the connection string contains a password
            if (
                conn_string_password
                in unaltered_datasources[name]["execution_engine"]["connection_string"]
            ):
                # it does contain a password, so make sure its masked
                assert (
                    conn_string_password
                    not in processed_config["execution_engine"]["connection_string"]
                )
            else:
                # it doesn't contain a password, so make sure it's unaltered
                assert processed_config == unaltered_datasources[name]

        # processed_config either doesn't have an `execution_engine` field,
        # or a `connection_string` field
        else:
            # expect this config to be unaltered
            assert processed_config == unaltered_datasources[name]


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


def test_sanitize_config_with_password_field():

    # this case has a password field inside a credentials dict - expect it to be masked
    config = {"credentials": {"password": "my-super-duper-secure-passphrase-123"}}
    config_copy = safe_deep_copy(config)
    res = PasswordMasker.sanitize_config(config_copy)
    assert res != config
    assert res["credentials"]["password"] == PasswordMasker.MASKED_PASSWORD_STRING


def test_sanitize_config_with_url_field(
    conn_string_with_embedded_password, conn_string_password
):

    # this case has a url field inside a credentials dict - expect the password inside
    # of it to be masked
    config = {"credentials": {"url": conn_string_with_embedded_password}}
    config_copy = safe_deep_copy(config)
    res = PasswordMasker.sanitize_config(config_copy)
    assert res != config
    assert conn_string_password not in res["credentials"]["url"]
    assert PasswordMasker.MASKED_PASSWORD_STRING in res["credentials"]["url"]


def test_sanitize_config_with_nested_url_field(
    conn_string_password, conn_string_with_embedded_password
):

    # this case has a connection string in an execution_engine dict
    config = {
        "execution_engine": {"connection_string": conn_string_with_embedded_password}
    }
    config_copy = safe_deep_copy(config)
    res = PasswordMasker.sanitize_config(config_copy)
    assert res != config
    assert conn_string_password not in res["execution_engine"]["connection_string"]
    assert (
        PasswordMasker.MASKED_PASSWORD_STRING
        in res["execution_engine"]["connection_string"]
    )


def test_sanitize_config_regardless_of_parent_key():

    # expect this config still be masked
    config = {
        "some_field": "and a value",
        "some_other_field": {"access_token": "but this won't be found"},
    }
    config_copy = safe_deep_copy(config)
    res = PasswordMasker.sanitize_config(config_copy)
    assert res != config
    assert (
        res["some_other_field"]["access_token"] == PasswordMasker.MASKED_PASSWORD_STRING
    )


@pytest.mark.cloud
def test_sanitize_config_masks_cloud_access_token(ge_cloud_access_token):

    # expect the access token to be found and masked
    config = {
        "store_backend": {
            "ge_cloud_credentials": {"access_token": ge_cloud_access_token}
        }
    }
    config_copy = safe_deep_copy(config)
    res = PasswordMasker.sanitize_config(config_copy)
    assert res != config
    assert (
        res["store_backend"]["ge_cloud_credentials"]["access_token"]
        == PasswordMasker.MASKED_PASSWORD_STRING
    )


def test_sanitize_config_works_with_list():
    config = {"some_key": [{"access_token": "12345"}]}
    config_copy = safe_deep_copy(config)
    res = PasswordMasker.sanitize_config(config_copy)
    assert res != config
    assert res["some_key"][0]["access_token"] == PasswordMasker.MASKED_PASSWORD_STRING


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
