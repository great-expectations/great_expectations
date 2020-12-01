import pytest

from great_expectations.data_context.store.query_store import SqlAlchemyQueryStore


@pytest.fixture()
def basic_sqlalchemy_query_store(titanic_sqlite_db):
    # For the purpose of this test, just steal the engine from a dataset
    credentials = {"engine": titanic_sqlite_db.engine}
    return SqlAlchemyQueryStore(
        credentials=credentials, queries={"q1": "SELECT DISTINCT PClass FROM titanic;"}
    )


@pytest.fixture()
def sqlalchemy_query_store_specified_return_type(titanic_sqlite_db):
    # For the purpose of this test, just steal the engine from a dataset
    credentials = {"engine": titanic_sqlite_db.engine}
    return SqlAlchemyQueryStore(
        credentials=credentials,
        queries={
            "q1": "SELECT DISTINCT PClass FROM titanic;",
            "q2": {
                "query": "SELECT DISTINCT PClass  FROM titanic;",
                "return_type": "list",
            },
            "q3": {"query": "SELECT count(*) FROM titanic;", "return_type": "scalar"},
            "error_query": {
                "query": "SELECT count(*) FROM titanic;",
                "return_type": "not_list_or_scalar",
            },
        },
    )


def test_basic_query(basic_sqlalchemy_query_store):
    assert (
        basic_sqlalchemy_query_store.get("q1") == "SELECT DISTINCT PClass FROM titanic;"
    )
    basic_sqlalchemy_query_store.set("q2", "SELECT DISTINCT PClass FROM ${table_name};")
    assert (
        basic_sqlalchemy_query_store.get("q2")
        == "SELECT DISTINCT PClass FROM ${table_name};"
    )
    res = basic_sqlalchemy_query_store.get_query_result("q1", {"table_name": "titanic"})
    assert res == ["1st", "2nd", "*", "3rd"]


def test_queries_with_return_types(sqlalchemy_query_store_specified_return_type):
    default_result = sqlalchemy_query_store_specified_return_type.get_query_result("q1")
    list_result = sqlalchemy_query_store_specified_return_type.get_query_result("q2")
    scalar_result = sqlalchemy_query_store_specified_return_type.get_query_result("q3")

    assert default_result == list_result == ["1st", "2nd", "*", "3rd"]
    assert scalar_result == 1313

    with pytest.raises(ValueError):
        sqlalchemy_query_store_specified_return_type.get_query_result("error_query")
