import os
from contextlib import contextmanager
from unittest import mock

import pytest

import great_expectations.exceptions as gee
from great_expectations.data_context.util import (
    PasswordMasker,
    parse_substitution_variable,
    substitute_value_from_aws_secrets_manager,
    substitute_value_from_azure_keyvault,
    substitute_value_from_gcp_secret_manager,
    substitute_value_from_secret_store,
)
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
    # PostgreSQL
    # default
    db_hostname = os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost")
    assert (
        PasswordMasker.mask_db_url(
            f"postgresql://scott:tiger@{db_hostname}:65432/mydatabase"
        )
        == f"postgresql://scott:***@{db_hostname}:65432/mydatabase"
    )
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

    # psycopg2
    assert (
        PasswordMasker.mask_db_url(
            f"postgresql+psycopg2://scott:tiger@{db_hostname}:65432/mydatabase"
        )
        == f"postgresql+psycopg2://scott:***@{db_hostname}:65432/mydatabase"
    )
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

    # PyMySQL
    assert (
        PasswordMasker.mask_db_url(
            f"mysql+pymysql://scott:tiger@{db_hostname}:65432/foo"
        )
        == f"mysql+pymysql://scott:***@{db_hostname}:65432/foo"
    )
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
    # pyodbc
    assert (
        PasswordMasker.mask_db_url("mssql+pyodbc://scott:tiger@mydsn")
        == "mssql+pyodbc://scott:***@mydsn"
    )
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
