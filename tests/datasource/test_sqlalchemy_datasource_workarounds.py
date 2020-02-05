# This file is intended for tests whose functionality is a workaround for deficiencies in upstream libraries.
# In an ideal world, it is empty. As fixes come to be, we can replace its items.

import pybigquery
import sqlalchemy as sa

from great_expectations.dataset import SqlAlchemyDataset


def test_pybigquery_module_type_import(sqlitedb_engine):
    # We're really just testing that this particular hack works
    dataset = SqlAlchemyDataset(engine=sqlitedb_engine, custom_sql='SELECT "cat" as "pet_name"')
    dataset.engine.dialect = pybigquery.sqlalchemy_bigquery.BigQueryDialect()
    assert getattr(dataset._get_dialect_type_module(), "RECORD") == sa.types.JSON

    # assert getattr(pybigquery.sqlalchemy_bigquery._type_map, "RECORD") == sa.types.JSON
