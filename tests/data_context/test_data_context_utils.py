import os
from contextlib import contextmanager
from unittest import mock

import pytest

import great_expectations.exceptions as gee
from great_expectations.data_context.util import (
    PasswordMasker,
    parse_substitution_variable,
    secretmanager,
    substitute_value_from_aws_secrets_manager,
    substitute_value_from_azure_keyvault,
    substitute_value_from_gcp_secret_manager,
    substitute_value_from_secret_store,
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
            == f"postgresql://scott:***@{db_hostname}:65432/mydatabase"
        )
    except ModuleNotFoundError:
        pass
    assert (
        PasswordMasker.mask_db_url(
            f"postgresql://scott:tiger@{db_hostname}:65432/mydatabase",
            use_urlparse=True,
        )
        == f"postgresql://scott:***@{db_hostname}:65432/mydatabase"
    )
    # missing port number, using urlparse
    assert (
        PasswordMasker.mask_db_url(
            f"postgresql://scott:tiger@{db_hostname}/mydatabase", use_urlparse=True
        )
        == f"postgresql://scott:***@{db_hostname}/mydatabase"
    )

    # psycopg2 (if installed in test environment)
    try:
        assert (
            PasswordMasker.mask_db_url(
                f"postgresql+psycopg2://scott:tiger@{db_hostname}:65432/mydatabase"
            )
            == f"postgresql+psycopg2://scott:***@{db_hostname}:65432/mydatabase"
        )
    except ModuleNotFoundError:
        pass
    assert (
        PasswordMasker.mask_db_url(
            f"postgresql+psycopg2://scott:tiger@{db_hostname}:65432/mydatabase",
            use_urlparse=True,
        )
        == f"postgresql+psycopg2://scott:***@{db_hostname}:65432/mydatabase"
    )

    # pg8000 (if installed in test environment)
    try:
        assert (
            PasswordMasker.mask_db_url(
                f"postgresql+pg8000://scott:tiger@{db_hostname}:65432/mydatabase"
            )
            == f"postgresql+pg8000://scott:***@{db_hostname}:65432/mydatabase"
        )
    except ModuleNotFoundError:
        pass
    assert (
        PasswordMasker.mask_db_url(
            f"postgresql+pg8000://scott:tiger@{db_hostname}:65432/mydatabase",
            use_urlparse=True,
        )
        == f"postgresql+pg8000://scott:***@{db_hostname}:65432/mydatabase"
    )

    # MySQL
    # default (if installed in test environment)
    try:
        assert (
            PasswordMasker.mask_db_url(f"mysql://scott:tiger@{db_hostname}:65432/foo")
            == f"mysql://scott:***@{db_hostname}:65432/foo"
        )
    except ModuleNotFoundError:
        pass

    assert (
        PasswordMasker.mask_db_url(
            f"mysql://scott:tiger@{db_hostname}:65432/foo", use_urlparse=True
        )
        == f"mysql://scott:***@{db_hostname}:65432/foo"
    )

    # mysqlclient (a maintained fork of MySQL-Python) (if installed in test environment)
    try:
        assert (
            PasswordMasker.mask_db_url(
                f"mysql+mysqldb://scott:tiger@{db_hostname}:65432/foo"
            )
            == f"mysql+mysqldb://scott:***@{db_hostname}:65432/foo"
        )
    except ModuleNotFoundError:
        pass
    assert (
        PasswordMasker.mask_db_url(
            f"mysql+mysqldb://scott:tiger@{db_hostname}:65432/foo", use_urlparse=True
        )
        == f"mysql+mysqldb://scott:***@{db_hostname}:65432/foo"
    )

    # PyMySQL (if installed in test environment)
    try:
        assert (
            PasswordMasker.mask_db_url(
                f"mysql+pymysql://scott:tiger@{db_hostname}:65432/foo"
            )
            == f"mysql+pymysql://scott:***@{db_hostname}:65432/foo"
        )
    except ModuleNotFoundError:
        pass
    assert (
        PasswordMasker.mask_db_url(
            f"mysql+pymysql://scott:tiger@{db_hostname}:65432/foo", use_urlparse=True
        )
        == f"mysql+pymysql://scott:***@{db_hostname}:65432/foo"
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
        == f"oracle://scott:***@{url_host}:1521/sidname"
    )

    try:
        assert (
            PasswordMasker.mask_db_url("oracle+cx_oracle://scott:tiger@tnsname")
            == "oracle+cx_oracle://scott:***@tnsname"
        )
    except ModuleNotFoundError:
        pass
    assert (
        PasswordMasker.mask_db_url(
            "oracle+cx_oracle://scott:tiger@tnsname", use_urlparse=True
        )
        == "oracle+cx_oracle://scott:***@tnsname"
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
        == "mssql+pyodbc://scott:***@mydsn"
    )

    # pymssql (if installed in test environment)
    try:
        assert (
            PasswordMasker.mask_db_url(
                f"mssql+pymssql://scott:tiger@{db_hostname}:12345/dbname"
            )
            == f"mssql+pymssql://scott:***@{db_hostname}:12345/dbname"
        )
    except ModuleNotFoundError:
        pass
    assert (
        PasswordMasker.mask_db_url(
            f"mssql+pymssql://scott:tiger@{db_hostname}:12345/dbname", use_urlparse=True
        )
        == f"mssql+pymssql://scott:***@{db_hostname}:12345/dbname"
    )

    # SQLite
    # relative path
    temp_dir = tmp_path / "sqllite_tests"
    temp_dir.mkdir()
    monkeypatch.chdir(temp_dir)
    assert (
        PasswordMasker.mask_db_url(f"sqlite:///something/foo.db")
        == f"sqlite:///something/foo.db"
    )
    assert (
        PasswordMasker.mask_db_url(f"sqlite:///something/foo.db", use_urlparse=True)
        == f"sqlite:///something/foo.db"
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


def test_password_masker_sanitize_data_context_config(
    basic_data_context_config,
    basic_data_context_config_dict,
    data_context_config_dict_with_cloud_backed_stores,
    data_context_config_dict_with_datasources,
    conn_string_password,
    ge_cloud_access_token,
):
    """
    This unit test ensures that PasswordMasker.sanitize_data_context_config
    correctly removes passwords from DataContextConfig dicts.
    """

    # expect that an Exception is raised if something other than a dict is passed
    with pytest.raises(TypeError):
        PasswordMasker.sanitize_data_context_config(basic_data_context_config)

    # expect no change without datasources
    config_without_creds = PasswordMasker.sanitize_data_context_config(
        basic_data_context_config_dict
    )
    assert config_without_creds == basic_data_context_config_dict

    # test that cloud store backend tokens have been properly masked
    config_with_creds_in_stores = PasswordMasker.sanitize_data_context_config(
        data_context_config_dict_with_cloud_backed_stores
    )
    for name, store_config in config_with_creds_in_stores["stores"].items():
        try:
            # check that the original token exists
            assert (
                data_context_config_dict_with_cloud_backed_stores["stores"][name][
                    "store_backend"
                ]["ge_cloud_credentials"]["access_token"]
                == ge_cloud_access_token
            )
            # expect that the GE Cloud token has been obscured
            assert (
                store_config["store_backend"]["ge_cloud_credentials"]["access_token"]
                != ge_cloud_access_token
            )
        except KeyError:
            # expect config to not be changed
            assert (
                store_config
                == data_context_config_dict_with_cloud_backed_stores["stores"][name]
            )

    # test that datasource credentials have been properly masked
    unaltered_datasources = data_context_config_dict_with_datasources["datasources"]
    config_with_creds_masked = PasswordMasker.sanitize_data_context_config(
        data_context_config_dict_with_datasources
    )
    masked_datasources = config_with_creds_masked["datasources"]
    for name, processed_config in masked_datasources.items():
        if processed_config.get("execution_engine") and processed_config[
            "execution_engine"
        ].get("connection_string"):
            if (
                conn_string_password
                in unaltered_datasources[name]["execution_engine"]["connection_string"]
            ):
                # not every connection string uses a password
                assert (
                    conn_string_password
                    not in processed_config["execution_engine"]["connection_string"]
                )
            else:
                assert processed_config == unaltered_datasources[name]
        else:
            # expect these configs to be equal
            assert processed_config == unaltered_datasources[name]


def test_password_masker_sanitize_datasource_config():
    """
    This unit test verifies the behavior of PasswordMasker.sanitize_datasource_config.
    """
    password = "super-duper secure passphrase"
    conn_str = f"redshift+psycopg2://no_user:{password}@111.11.1.1:1111/foo"
    conn_str_no_password = "bigquery://foo/bar"

    # case 1
    # base case - this config should pass through unaffected
    config_A = {
        "some_field": "and a value",
        "some_other_field": {"password": "but this won't be found"},
    }
    config_A_copy = safe_deep_copy(config_A)
    assert PasswordMasker.sanitize_datasource_config(config_A_copy) == config_A

    # case 2
    # this case has a password field inside a credentials dict - expect it to be masked
    config_B = {"credentials": {"password": password}}
    config_B_copy = safe_deep_copy(config_B)
    res_B = PasswordMasker.sanitize_datasource_config(config_B_copy)
    assert res_B != config_B
    assert res_B["credentials"]["password"] == PasswordMasker.MASKED_PASSWORD_STRING

    # case 3
    # this case has a url field inside a credentials dict - expect the password inside
    # of it to be masked
    config_C = {"credentials": {"url": conn_str}}
    config_C_copy = safe_deep_copy(config_C)
    res_C = PasswordMasker.sanitize_datasource_config(config_C_copy)
    assert res_C != config_C
    assert password not in res_C["credentials"]["url"]
    assert PasswordMasker.MASKED_PASSWORD_STRING in res_C["credentials"]["url"]

    # case 4
    # this case has a BigQuery url field inside a credentials dict, which doesn't have a
    # password - expect it to be untouched
    config_D = {"credentials": {"url": conn_str_no_password}}
    config_D_copy = safe_deep_copy(config_D)
    res_D = PasswordMasker.sanitize_datasource_config(config_D_copy)
    assert res_D == config_D

    # case 5
    # this case has a connection string in an execution_engine dict
    config_E = {"execution_engine": {"connection_string": conn_str}}
    config_E_copy = safe_deep_copy(config_E)
    res_E = PasswordMasker.sanitize_datasource_config(config_E_copy)
    assert res_E != config_E
    assert password not in res_E["execution_engine"]["connection_string"]
    assert (
        PasswordMasker.MASKED_PASSWORD_STRING
        in res_E["execution_engine"]["connection_string"]
    )

    # case 6
    # this case has a BigQuery url inside the execution_engine dict, which doesn't have a
    # password - expect it to be untouched
    config_F = {"execution_engine": {"connection_string": conn_str_no_password}}
    config_F_copy = safe_deep_copy(config_F)
    res_F = PasswordMasker.sanitize_datasource_config(config_F_copy)
    assert res_F == config_F


def test_password_masker_sanitize_store_config(ge_cloud_access_token):
    """
    This unit test verifies the behavior of PasswordMasker.sanitize_store_config.
    """

    # case 1
    # base case - expect this config not to be changed
    config_A = {
        "some_field": "and a value",
        "some_other_field": {"access_token": "but this won't be found"},
    }
    config_A_copy = safe_deep_copy(config_A)
    assert PasswordMasker.sanitize_store_config(config_A_copy) == config_A

    # case 2
    # expect the access token to be found and masked
    config_B = {
        "store_backend": {
            "ge_cloud_credentials": {"access_token": ge_cloud_access_token}
        }
    }
    config_B_copy = safe_deep_copy(config_B)
    res_B = PasswordMasker.sanitize_store_config(config_B_copy)
    assert res_B != config_B
    assert (
        res_B["store_backend"]["ge_cloud_credentials"]["access_token"]
        == PasswordMasker.MASKED_UUID
    )


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


@contextmanager
def does_not_raise():
    yield


@pytest.mark.parametrize(
    "input_value,method_to_patch,return_value",
    [
        ("any_value", None, "any_value"),
        ("secret|any_value", None, "secret|any_value"),
        (
            "secret|arn:aws:secretsmanager:region-name-1:123456789012:secret:my-secret",
            "great_expectations.data_context.util.substitute_value_from_aws_secrets_manager",
            "success",
        ),
        (
            "secret|projects/project_id/secrets/my_secret",
            "great_expectations.data_context.util.substitute_value_from_gcp_secret_manager",
            "success",
        ),
        (
            "secret|https://my-vault-name.vault.azure.net/secrets/my_secret",
            "great_expectations.data_context.util.substitute_value_from_azure_keyvault",
            "success",
        ),
    ],
)
def test_substitute_value_from_secret_store(input_value, method_to_patch, return_value):
    if method_to_patch:
        with mock.patch(method_to_patch, return_value=return_value):
            assert substitute_value_from_secret_store(value=input_value) == return_value
    else:
        assert substitute_value_from_secret_store(value=input_value) == return_value


class MockedBoto3Client:
    def __init__(self, secret_response):
        self.secret_response = secret_response

    def get_secret_value(self, *args, **kwargs):
        return self.secret_response


class MockedBoto3Session:
    def __init__(self, secret_response):
        self.secret_response = secret_response

    def __call__(self):
        return self

    def client(self, *args, **kwargs):
        return MockedBoto3Client(self.secret_response)


@pytest.mark.parametrize(
    "input_value,secret_response,raises,expected",
    [
        (
            "secret|arn:aws:secretsmanager:region-name-1:123456789012:secret:my-secret",
            {"SecretString": "value"},
            does_not_raise(),
            "value",
        ),
        (
            "secret|arn:aws:secretsmanager:region-name-1:123456789012:secret:my-secret",
            {"SecretBinary": b"dmFsdWU="},
            does_not_raise(),
            "value",
        ),
        (
            "secret|arn:aws:secretsmanager:region-name-1:123456789012:secret:my-secret|key",
            {"SecretString": '{"key": "value"}'},
            does_not_raise(),
            "value",
        ),
        (
            "secret|arn:aws:secretsmanager:region-name-1:123456789012:secret:my-secret|key",
            {"SecretBinary": b"eyJrZXkiOiAidmFsdWUifQ=="},
            does_not_raise(),
            "value",
        ),
        (
            "secret|arn:aws:secretsmanager:region-name-1:123456789012:secret:my-se%&et|key",
            None,
            pytest.raises(ValueError),
            None,
        ),
        (
            "secret|arn:aws:secretsmanager:region-name-1:123456789012:secret:my-secret:000000000-0000-0000-0000-00000000000|key",
            None,
            pytest.raises(ValueError),
            None,
        ),
    ],
)
def test_substitute_value_from_aws_secrets_manager(
    input_value, secret_response, raises, expected
):
    with raises:
        with mock.patch(
            "great_expectations.data_context.util.boto3.session.Session",
            return_value=MockedBoto3Session(secret_response),
        ):
            assert substitute_value_from_aws_secrets_manager(input_value) == expected


class MockedSecretManagerServiceClient:
    def __init__(self, secret_response):
        self.secret_response = secret_response

    def __call__(self):
        return self

    def access_secret_version(self, *args, **kwargs):
        class Response:
            pass

        response = Response()
        response._pb = Response()
        response._pb.payload = Response()
        response._pb.payload.data = self.secret_response

        return response


@pytest.mark.skipif(
    secretmanager is None,
    reason="Could not import 'secretmanager' from google.cloud in data_context.util",
)
@pytest.mark.parametrize(
    "input_value,secret_response,raises,expected",
    [
        (
            "secret|projects/project_id/secrets/my_secret",
            b"value",
            does_not_raise(),
            "value",
        ),
        (
            "secret|projects/project_id/secrets/my_secret|key",
            b'{"key": "value"}',
            does_not_raise(),
            "value",
        ),
        (
            "secret|projects/project_id/secrets/my_se%&et|key",
            None,
            pytest.raises(ValueError),
            None,
        ),
        (
            "secret|projects/project_id/secrets/my_secret/version/A|key",
            None,
            pytest.raises(ValueError),
            None,
        ),
    ],
)
def test_substitute_value_from_gcp_secret_manager(
    input_value, secret_response, raises, expected
):
    with raises:
        with mock.patch(
            "great_expectations.data_context.util.secretmanager.SecretManagerServiceClient",
            return_value=MockedSecretManagerServiceClient(secret_response),
        ):
            assert substitute_value_from_gcp_secret_manager(input_value) == expected


class MockedSecretClient:
    def __init__(self, secret_response):
        self.secret_response = secret_response

    def __call__(self, *args, **kwargs):
        return self

    def get_secret(self, *args, **kwargs):
        class Response:
            pass

        response = Response()
        response.value = self.secret_response
        return response


@mock.patch("great_expectations.data_context.util.DefaultAzureCredential", new=object)
@pytest.mark.parametrize(
    "input_value,secret_response,raises,expected",
    [
        (
            "secret|https://my-vault-name.vault.azure.net/secrets/my-secret",
            "value",
            does_not_raise(),
            "value",
        ),
        (
            "secret|https://my-vault-name.vault.azure.net/secrets/my-secret|key",
            '{"key": "value"}',
            does_not_raise(),
            "value",
        ),
        (
            "secret|https://my-vault-name.vault.azure.net/secrets/my-se%&et|key",
            None,
            pytest.raises(ValueError),
            None,
        ),
        (
            "secret|https://my_vault_name.vault.azure.net/secrets/my-secret/A0000000000000000000000000000000|key",
            None,
            pytest.raises(ValueError),
            None,
        ),
    ],
)
def test_substitute_value_from_azure_keyvault(
    input_value, secret_response, raises, expected
):
    with raises:
        with mock.patch(
            "great_expectations.data_context.util.SecretClient",
            return_value=MockedSecretClient(secret_response),
        ):
            assert substitute_value_from_azure_keyvault(input_value) == expected
