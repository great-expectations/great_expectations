import logging

import great_expectations.exceptions as ge_exceptions
from great_expectations.data_context.store.store_backend import StoreBackend

try:
    import sqlalchemy
    from sqlalchemy import (
        create_engine,
        Column,
        String,
        MetaData,
        Table,
        select,
        and_,
        column,
        text,
    )
    from sqlalchemy.engine.url import URL
    from sqlalchemy.engine.reflection import Inspector
    from sqlalchemy.exc import SQLAlchemyError, NoSuchTableError
except ImportError:
    sqlalchemy = None
    create_engine = None


logger = logging.getLogger(__name__)


class DatabaseStoreBackend(StoreBackend):
    def __init__(self, credentials, table_name, key_columns, fixed_length_key=True):
        super().__init__(fixed_length_key=fixed_length_key)
        if not sqlalchemy:
            raise ge_exceptions.DataContextError(
                "ModuleNotFoundError: No module named 'sqlalchemy'"
            )

        if not self.fixed_length_key:
            raise ge_exceptions.InvalidConfigError(
                "DatabaseStoreBackend requires use of a fixed-length-key"
            )

        drivername = credentials.pop("drivername")
        options = URL(drivername, **credentials)
        self.engine = create_engine(options)

        meta = MetaData()
        self.key_columns = key_columns
        # Dynamically construct a SQLAlchemy table with the name and column names we'll use
        cols = []
        for column in key_columns:
            if column == "value":
                raise ge_exceptions.InvalidConfigError(
                    "'value' cannot be used as a key_element name"
                )
            cols.append(Column(column, String, primary_key=True))
        cols.append(Column("value", String))
        try:
            table = Table(table_name, meta, autoload=True, autoload_with=self.engine)
            # We do a "light" check: if the columns' names match, we will proceed, otherwise, create the table
            if set([str(col.name).lower() for col in table.columns]) != (
                set(key_columns) | {"value"}
            ):
                raise ge_exceptions.StoreBackendError(
                    f"Unable to use table {table_name}: it exists, but does not have the expected schema."
                )
        except NoSuchTableError:
            table = Table(table_name, meta, *cols)
            try:
                meta.create_all(self.engine)
            except SQLAlchemyError as e:
                raise ge_exceptions.StoreBackendError(
                    f"Unable to connect to table {table_name} because of an error. It is possible your table needs to be migrated to a new schema.  SqlAlchemyError: {str(e)}"
                )
        self._table = table

    def _get(self, key):
        sel = (
            select([column("value")])
            .select_from(self._table)
            .where(
                and_(
                    *[
                        getattr(self._table.columns, key_col) == val
                        for key_col, val in zip(self.key_columns, key)
                    ]
                )
            )
        )
        try:
            return self.engine.execute(sel).fetchone()[0]
        except (IndexError, SQLAlchemyError) as e:
            logger.debug("Error fetching value: " + str(e))
            raise ge_exceptions.StoreError("Unable to fetch value for key: " + str(key))

    def _set(self, key, value, **kwargs):
        cols = {k: v for (k, v) in zip(self.key_columns, key)}
        cols["value"] = value
        ins = self._table.insert().values(**cols)
        self.engine.execute(ins)

    def _move(self):
        raise NotImplementedError

    def _has_key(self, key):
        sel = (
            select([sqlalchemy.func.count(column("value"))])
            .select_from(self._table)
            .where(
                and_(
                    *[
                        getattr(self._table.columns, key_col) == val
                        for key_col, val in zip(self.key_columns, key)
                    ]
                )
            )
        )
        try:
            return self.engine.execute(sel).fetchone()[0] == 1
        except (IndexError, SQLAlchemyError) as e:
            logger.debug("Error checking for value: " + str(e))
            return False

    def list_keys(self, prefix=()):
        sel = (
            select([column(col) for col in self.key_columns])
            .select_from(self._table)
            .where(
                and_(
                    *[
                        getattr(self._table.columns, key_col) == val
                        for key_col, val in zip(self.key_columns[: len(prefix)], prefix)
                    ]
                )
            )
        )
        return [tuple(row) for row in self.engine.execute(sel).fetchall()]

    def remove_key(self, key):
        delete_statement = self._table.delete().where(
            and_(
                *[
                    getattr(self._table.columns, key_col) == val
                    for key_col, val in zip(self.key_columns, key)
                ]
            )
        )
        try:
            return self.engine.execute(delete_statement)
        except SQLAlchemyError as e:
            raise ge_exceptions.StoreBackendError(
                f"Unable to delete key: got sqlalchemy error {str(e)}"
            )

    _move = None
