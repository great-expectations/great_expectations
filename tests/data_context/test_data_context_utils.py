import pytest

import great_expectations.exceptions as gee
from great_expectations.data_context.util import (
    PasswordMasker,
    parse_substitution_variable,
)
from great_expectations.util import load_class


def test_load_class_raises_error_when_module_not_found():
    with pytest.raises(gee.PluginModuleNotFoundError):
        load_class("foo", "bar")


def test_load_class_raises_error_when_class_not_found():
    with pytest.raises(gee.PluginClassNotFoundError):
        load_class("TotallyNotARealClass", "great_expectations.datasource")


def test_password_masker_mask_db_url():
    """
    What does this test and why?
    The PasswordMasker.mask_db_url() should mask passwords consistently in database urls. The output of mask_db_url should be the same whether user_urlparse is set to True or False.
    This test uses database url examples from
    https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls
    """
    # PostgreSQL
    # default
    assert (
        PasswordMasker.mask_db_url(
            "postgresql://scott:tiger@localhost:65432/mydatabase"
        )
        == "postgresql://scott:***@localhost:65432/mydatabase"
    )
    assert (
        PasswordMasker.mask_db_url(
            "postgresql://scott:tiger@localhost:65432/mydatabase", use_urlparse=True
        )
        == "postgresql://scott:***@localhost:65432/mydatabase"
    )
    # missing port number, using urlparse
    assert (
        PasswordMasker.mask_db_url(
            "postgresql://scott:tiger@localhost/mydatabase", use_urlparse=True
        )
        == "postgresql://scott:***@localhost/mydatabase"
    )

    # psycopg2
    assert (
        PasswordMasker.mask_db_url(
            "postgresql+psycopg2://scott:tiger@localhost:65432/mydatabase"
        )
        == "postgresql+psycopg2://scott:***@localhost:65432/mydatabase"
    )
    assert (
        PasswordMasker.mask_db_url(
            "postgresql+psycopg2://scott:tiger@localhost:65432/mydatabase",
            use_urlparse=True,
        )
        == "postgresql+psycopg2://scott:***@localhost:65432/mydatabase"
    )

    # pg8000 (if installed in test environment)
    try:
        assert (
            PasswordMasker.mask_db_url(
                "postgresql+pg8000://scott:tiger@localhost:65432/mydatabase"
            )
            == "postgresql+pg8000://scott:***@localhost:65432/mydatabase"
        )
    except ModuleNotFoundError:
        pass
    assert (
        PasswordMasker.mask_db_url(
            "postgresql+pg8000://scott:tiger@localhost:65432/mydatabase",
            use_urlparse=True,
        )
        == "postgresql+pg8000://scott:***@localhost:65432/mydatabase"
    )

    # MySQL
    # default (if installed in test environment)
    try:
        assert (
            PasswordMasker.mask_db_url("mysql://scott:tiger@localhost:65432/foo")
            == "mysql://scott:***@localhost:65432/foo"
        )
    except ModuleNotFoundError:
        pass

    assert (
        PasswordMasker.mask_db_url(
            "mysql://scott:tiger@localhost:65432/foo", use_urlparse=True
        )
        == "mysql://scott:***@localhost:65432/foo"
    )

    # mysqlclient (a maintained fork of MySQL-Python) (if installed in test environment)
    try:
        assert (
            PasswordMasker.mask_db_url(
                "mysql+mysqldb://scott:tiger@localhost:65432/foo"
            )
            == "mysql+mysqldb://scott:***@localhost:65432/foo"
        )
    except ModuleNotFoundError:
        pass
    assert (
        PasswordMasker.mask_db_url(
            "mysql+mysqldb://scott:tiger@localhost:65432/foo", use_urlparse=True
        )
        == "mysql+mysqldb://scott:***@localhost:65432/foo"
    )

    # PyMySQL
    assert (
        PasswordMasker.mask_db_url("mysql+pymysql://scott:tiger@localhost:65432/foo")
        == "mysql+pymysql://scott:***@localhost:65432/foo"
    )
    assert (
        PasswordMasker.mask_db_url(
            "mysql+pymysql://scott:tiger@localhost:65432/foo", use_urlparse=True
        )
        == "mysql+pymysql://scott:***@localhost:65432/foo"
    )

    # Oracle (if installed in test environment)
    try:
        assert (
            PasswordMasker.mask_db_url("oracle://scott:tiger@127.0.0.1:1521/sidname")
            == "oracle://scott:***@127.0.0.1:1521/sidname"
        )
    except ModuleNotFoundError:
        pass

    assert (
        PasswordMasker.mask_db_url(
            "oracle://scott:tiger@127.0.0.1:1521/sidname", use_urlparse=True
        )
        == "oracle://scott:***@127.0.0.1:1521/sidname"
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
                "mssql+pymssql://scott:tiger@hostname:12345/dbname"
            )
            == "mssql+pymssql://scott:***@hostname:12345/dbname"
        )
    except ModuleNotFoundError:
        pass
    assert (
        PasswordMasker.mask_db_url(
            "mssql+pymssql://scott:tiger@hostname:12345/dbname", use_urlparse=True
        )
        == "mssql+pymssql://scott:***@hostname:12345/dbname"
    )

    # SQLite
    # relative path
    assert PasswordMasker.mask_db_url("sqlite:///foo.db") == "sqlite:///foo.db"
    assert (
        PasswordMasker.mask_db_url("sqlite:///foo.db", use_urlparse=True)
        == "sqlite:///foo.db"
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
