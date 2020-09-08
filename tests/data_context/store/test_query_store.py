import pytest

from great_expectations.data_context.store.query_store import SqlAlchemyQueryStore


@pytest.fixture()
def basic_sqlalchemy_query_store(titanic_sqlite_db):
    # For the purpose of this test, just steal the engine from a dataset
    credentials = {"engine": titanic_sqlite_db.engine}
    return SqlAlchemyQueryStore(
        credentials=credentials, queries={"q1": "SELECT count(*) FROM titanic;"}
    )


def test_basic_query(basic_sqlalchemy_query_store):
    assert basic_sqlalchemy_query_store.get("q1") == "SELECT count(*) FROM titanic;"
    basic_sqlalchemy_query_store.set("q2", "SELECT count(*) FROM ${table_name};")
    assert (
        basic_sqlalchemy_query_store.get("q2") == "SELECT count(*) FROM ${table_name};"
    )
    res = basic_sqlalchemy_query_store.get_query_result("q2", {"table_name": "titanic"})
    assert res[0] == 1313
