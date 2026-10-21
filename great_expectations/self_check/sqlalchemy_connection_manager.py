from __future__ import annotations

import logging
import threading
from typing import Dict

from great_expectations.compatibility import sqlalchemy
from great_expectations.compatibility.postgresql import resolve_postgresql_driver
from great_expectations.compatibility.sqlalchemy import (
    sqlalchemy as sa,
)

logger = logging.getLogger(__name__)


SQLAlchemyError = sqlalchemy.SQLAlchemyError


class SqlAlchemyConnectionManager:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self._connections: Dict[str, sqlalchemy.Connection] = {}

    def get_connection(self, connection_string):
        if sa is not None:
            with self.lock:
                if connection_string not in self._connections:
                    try:
                        engine = sa.create_engine(resolve_postgresql_driver(connection_string))
                        conn = engine.connect()
                        self._connections[connection_string] = conn
                    except (ImportError, SQLAlchemyError) as e:
                        print(
                            f'Unable to establish connection with {connection_string} -- exception "{e}" occurred.'  # noqa: E501 # FIXME CoP
                        )
                        raise

                return self._connections[connection_string]

        return None


connection_manager = SqlAlchemyConnectionManager()


class LockingConnectionCheck:
    def __init__(self, sa, connection_string) -> None:
        self.lock = threading.Lock()
        self.sa = sa
        self.connection_string = connection_string
        self._is_valid = None

    def is_valid(self):
        with self.lock:
            if self._is_valid is None:
                try:
                    engine = self.sa.create_engine(
                        resolve_postgresql_driver(self.connection_string)
                    )
                    try:
                        conn = engine.connect()
                        conn.close()
                    finally:
                        # Close the pooled connection now rather than whenever the engine is
                        # garbage collected; a probe runs once per generated test, and
                        # connections that wait for collection exhaust the server's limit.
                        engine.dispose()
                    self._is_valid = True
                except (ImportError, self.sa.exc.SQLAlchemyError) as e:
                    print(f"{e!s}")
                    self._is_valid = False

            return self._is_valid
