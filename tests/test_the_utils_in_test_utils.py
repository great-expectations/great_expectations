import pytest

from great_expectations.util import (
    get_clickhouse_sqlalchemy_potential_type,
    is_library_loadable,
)
from tests.test_utils import get_awsathena_connection_url


@pytest.mark.athena
def test_get_awsathena_connection_url(monkeypatch):
    monkeypatch.setenv("ATHENA_STAGING_S3", "s3://test-staging/")
    monkeypatch.setenv("ATHENA_DB_NAME", "test_db_name")
    monkeypatch.setenv("ATHENA_TEN_TRIPS_DB_NAME", "test_ten_trips_db_name")

    assert (
        get_awsathena_connection_url()
        == "awsathena+rest://@athena.us-east-1.amazonaws.com/test_db_name?s3_staging_dir=s3://test-staging/"
    )

    assert (
        get_awsathena_connection_url(db_name_env_var="ATHENA_TEN_TRIPS_DB_NAME")
        == "awsathena+rest://@athena.us-east-1.amazonaws.com/test_ten_trips_db_name?s3_staging_dir=s3://test-staging/"
    )


@pytest.mark.clickhouse
@pytest.mark.skipif(
    not is_library_loadable(library_name="clickhouse_sqlalchemy"),
    reason="clickhouse_sqlalchemy is not installed",
)
def test_get_clickhouse_sqlalchemy_potential_type():
    import clickhouse_sqlalchemy
    from clickhouse_sqlalchemy import types

    input_output = (
        ("Nullable(String)", types.String),
        ("Int8", types.Int8),
        ("Map(String, String)", types.Map),
    )
    for pair in input_output:
        assert (
            get_clickhouse_sqlalchemy_potential_type(clickhouse_sqlalchemy.drivers.base, pair[0])
            == pair[1]
        )
