"""Release the DB-API connections pooled by sqlite SQLAlchemy engines that GX creates.

An engine's pool keeps the DB-API connections it opens until the engine is disposed. GX
creates engines on behalf of datasources and execution engines, which have no close step of
their own, so nothing disposes them: when such an engine is garbage collected, its pooled
connections are finalized while still open. For sqlite3 that emits a ResourceWarning from
Python 3.13, attributed to whatever code happens to be running when the collector does.

Only sqlite engines are covered. Closing a connection from the garbage collector runs the
driver's close on whatever thread triggered the collection; for a network driver that is
remote I/O, with its own retries and sockets, at an arbitrary point in someone else's code.
"""

from __future__ import annotations

import contextlib
import weakref
from typing import TYPE_CHECKING, Any

from great_expectations.compatibility.sqlalchemy import Engine
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa

if TYPE_CHECKING:
    from great_expectations.compatibility import sqlalchemy

# The finalizer currently registered for each pool record. A record is reused for every
# connection its pool slot opens (on recycle, invalidation or a failed pre-ping), so a new
# connection replaces the record's finalizer rather than adding one beside it.
_finalizers: weakref.WeakKeyDictionary[Any, weakref.finalize] = weakref.WeakKeyDictionary()


def _close_quietly(dbapi_connection: Any) -> None:
    # Runs from the garbage collector, possibly on another thread than the one that opened the
    # connection (which sqlite3 refuses by default). A failure here leaves the connection as it
    # would have been without this hook, so there is nothing better to do with it.
    with contextlib.suppress(Exception):
        dbapi_connection.close()


def _close_with_its_pool(dbapi_connection: Any, connection_record: Any) -> None:
    # Keyed on the pool's record for the connection, not on the engine: the pool refers back to
    # the engine, so a finalizer that held the pool would keep the engine alive. The record dies
    # with its pool, and closing an already closed connection is a no-op.
    previous = _finalizers.pop(connection_record, None)
    if previous is not None:
        # The pool closed the record's previous connection before opening this one; the old
        # finalizer would only keep that closed connection alive.
        previous.detach()
    finalizer = weakref.finalize(connection_record, _close_quietly, dbapi_connection)
    # Only on collection: at interpreter exit the connection may still be in use by an exit
    # handler or a daemon thread, and it is closed by the interpreter's own teardown anyway.
    # `atexit` is a settable property at runtime; typeshed's empty `__slots__` hides it.
    finalizer.atexit = False  # type: ignore[misc]
    _finalizers[connection_record] = finalizer


def close_connections_when_collected(engine: sqlalchemy.Engine) -> sqlalchemy.Engine:
    """Close every DB-API connection a sqlite engine's pool opens once that pool is collected.

    Connections are only closed once no one can use them any more; an engine that is still
    reachable, and every connection checked out of it, is unaffected, including at interpreter
    exit. Covers pools the engine recreates on `dispose()`. Idempotent. An engine for any other
    dialect, or anything that is not an `Engine` (an execution engine can hold a `Connection`
    instead), is returned untouched.

    Returns:
        The same engine, so a call can wrap the expression that creates it.
    """
    # Engine is bound at import, so a test that patches `sqlalchemy.engine` doesn't swap it out.
    if not isinstance(engine, Engine) or engine.dialect.name != "sqlite":
        return engine
    if not sa.event.contains(engine, "connect", _close_with_its_pool):
        sa.event.listen(engine, "connect", _close_with_its_pool)
    return engine
