import logging

import great_expectations.exceptions as ge_exceptions
from great_expectations.data_context.store.store_backend import StoreBackend

try:
    import sqlalchemy
    from sqlalchemy import (
        Column,
        MetaData,
        String,
        Table,
        and_,
        column,
        create_engine,
        select,
        text,
    )
    from sqlalchemy.engine.reflection import Inspector
    from sqlalchemy.engine.url import URL
    from sqlalchemy.exc import IntegrityError, NoSuchTableError, SQLAlchemyError
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
            if {str(col.name).lower() for col in table.columns} != (
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

    def _set(self, key, value, allow_update=True):
        cols = {k: v for (k, v) in zip(self.key_columns, key)}
        cols["value"] = value

        if allow_update:
            if self.has_key(key):
                ins = (
                    self._table.update()
                    .where(getattr(self._table.columns, self.key_columns[0]) == key[0])
                    .values(**cols)
                )
            else:
                ins = self._table.insert().values(**cols)
        else:
            ins = self._table.insert().values(**cols)

        try:
            self.engine.execute(ins)
        except IntegrityError as e:
            if self._get(key) == value:
                logger.info(f"Key {str(key)} already exists with the same value.")
            else:
                raise ge_exceptions.StoreBackendError(
                    f"Integrity error {str(e)} while trying to store key"
                )

    def _move(self):
        raise NotImplementedError

    def get_url_for_key(self, key):
        url = self._convert_engine_and_key_to_url(key)
        return url

    def _convert_engine_and_key_to_url(self, key):
        # SqlAlchemy engine URL is formatted in the following way
        # postgresql://postgres:password@localhost:5433/work
        # [engine]://[username]:[password]@[host]:[port]/[db_name]

        # which contains information like username and password that should not be public
        # This function changes the formatting to the following:
        # [engine]://[db_name]/[key]

        full_url = str(self.engine.url)
        engine_name = full_url.split("://")[0]
        db_name = full_url.split("/")[-1]
        return engine_name + "://" + db_name + "/" + str(key[0])

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
