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
    )
    from sqlalchemy.engine.url import URL
except ImportError:
    sqlalchemy = None
    create_engine = None


class DatabaseStoreBackend(StoreBackend):
    def __init__(self, credentials, table_name, key_columns, fixed_length_key=True):
        super().__init__(fixed_length_key=fixed_length_key)
        if not sqlalchemy:
            raise ge_exceptions.DataContextError(
                "ModuleNotFoundError: No module named 'sqlalchemy'"
            )

        if not self.fixed_length_key:
            raise ValueError("DatabaseStoreBackend requires use of a fixed-length-key")

        meta = MetaData()
        self.key_columns = key_columns
        # Dynamically construct a SQLAlchemy table with the name and column names we'll use
        cols = []
        for column in key_columns:
            if column == "value":
                raise ValueError("'value' cannot be used as a key_element name")
            cols.append(Column(column, String, primary_key=True))

        cols.append(Column("value", String))
        self._table = Table(table_name, meta, *cols)

        drivername = credentials.pop("drivername")
        options = URL(drivername, **credentials)
        self.engine = create_engine(options)
        meta.create_all(self.engine)

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
        res = self.engine.execute(sel).fetchone()
        if res:
            return self.engine.execute(sel).fetchone()[0]

    def _set(self, key, value, **kwargs):
        cols = {k: v for (k, v) in zip(self.key_columns, key)}
        cols["value"] = value
        ins = self._table.insert().values(**cols)
        self.engine.execute(ins)

    def _has_key(self, key):
        pass

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
        raise NotImplementedError
