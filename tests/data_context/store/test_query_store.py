import pytest

import tests.test_utils as test_utils
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

    """
    What does this test and why?
    A Store should be able to report it's store_backend_id
    which is set when the StoreBackend is instantiated.
    """
    # Check that store_backend_id exists can be read
    assert basic_sqlalchemy_query_store.store_backend_id is not None
    # Check that store_backend_id is a valid UUID
    assert test_utils.validate_uuid4(basic_sqlalchemy_query_store.store_backend_id)
