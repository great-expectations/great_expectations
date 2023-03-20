import pytest
from unittest import mock

from packaging.version import Version

from great_expectations.compatibility.sqlalchemy import sa, sqlalchemy_shim


@pytest.mark.unit
@mock.patch("great_expectations.compatibility.sqlalchemy.sqlalchemy.__version__")
def test_get_sqlalchemy_version_in_shim(sqlalchemy_version):
    sqlalchemy_version.return_value = "1.4.0"
    assert sqlalchemy_shim.sqlalchemy_version == Version("1.4.0")
