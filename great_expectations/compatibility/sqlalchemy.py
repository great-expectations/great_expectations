"""Compatibility shims for SQLAlchemy.

Currently supported v2.0.x, v1.4.x, v1.3.x.

Usage example for the `select` method:

from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.compatibility.sqlalchemy import SQLAlchemyShim

sqlalchemy_shim = SQLAlchemyShim()
select = sqlalchemy_shim.select
"""
from __future__ import annotations

from packaging.version import Version

from great_expectations.optional_imports import sqlalchemy_version_check
from great_expectations.util import NotImported

try:
    import sqlalchemy

    sqlalchemy_version_check(sqlalchemy.__version__)
except ImportError:
    sqlalchemy = NotImported("sqlalchemy not found, please install.")

import functools
import types


def copy_func(f):
    """Based on http://stackoverflow.com/a/6528148/190597 (Glenn Maynard)"""
    g = types.FunctionType(
        f.__code__,
        f.__globals__,
        name=f.__name__,
        argdefs=f.__defaults__,
        closure=f.__closure__,
    )
    g = functools.update_wrapper(g, f)
    g.__kwdefaults__ = f.__kwdefaults__
    return g


class SQLAlchemyShim:
    """This class is used in place of certain sqlalchemy methods that require
    a different implementation or translation between supported versions.

    For example, in SQLAlchemy 2.0.0, select no longer takes a list as the
    first argument, and takes positional arguments instead. Version 1.4.x takes
    either and 1.3.x takes only a list. This Shim will translate the input
    args and call the select method with the appropriate args.
    """

    def __init__(self):
        """Create an instance."""
        self._sqlalchemy_version: None | Version = None
        self._orig_library_select = copy_func(sqlalchemy.select)

    @property
    def sqlalchemy_version(self) -> None | Version:
        """Return the version of sqlalchemy that is installed, or None if not.

        Only checks the actual version the first time it is invoked, otherwise
        it is shortcut.
        """
        if self._sqlalchemy_version:
            # Short circuit if version already set
            return self._sqlalchemy_version
        else:
            if isinstance(sqlalchemy, NotImported):
                return None
            else:
                version = Version(sqlalchemy.__version__)
                self._sqlalchemy_version = version
                return self._sqlalchemy_version

    def is_version_1_3_x(self) -> bool:
        """Check whether sqlalchemy version starts with 1.3, and is above our lower bound."""
        return Version("1.3.18") <= self.sqlalchemy_version < Version("1.4.0")

    def is_version_1_4_x(self) -> bool:
        """Check whether sqlalchemy version starts with 1.4"""
        return Version("1.4.0") <= self.sqlalchemy_version < Version("1.5.0")

    def is_version_2_0_x(self) -> bool:
        """Check whether sqlalchemy version starts with 2.0"""
        return Version("2.0.0") <= self.sqlalchemy_version < Version("2.1.0")

    def select(self, *args, **kwargs):
        """Select method that works with all sqlalchemy versions supported in GX."""
        # TODO: wrap this in a make_select method and then grab signature from underlying select method
        #  and then return new select method to monkeypatch. Won't work for pycharm, but will work for jupyter.
        #  For IDEs, stub file like used in fluent datasources, use those here.
        #  Would it work? Might have to generate stub files
        #  on import. Might be other ways. Check with pod fka Mario for learnings.
        #  How to call `make_select` not at import time?
        #  Use property, then gets overriden when used to make this not happen during import time. If none, then call
        #  make_select, if not short circuit and return method
        if self.is_version_1_3_x():
            # Convert args to a list for compatibility with 1.3.x
            return self._orig_library_select(list(args), **kwargs)
        elif self.is_version_1_4_x() or self.is_version_2_0_x():
            # Convert list to args if list if using 1.3.x style
            if isinstance(args[0], list):
                # TODO: Insert deprecation warning here from great_expectations/warnings.py
                return self._orig_library_select(*args[0], **kwargs)
            return self._orig_library_select(*args, **kwargs)
        else:
            # Pass through for future versions (add more version specific logic
            # in another elif if necessary)
            return self._orig_library_select(*args, **kwargs)


# Global instances created at import time. Use these.
if sqlalchemy:
    sqlalchemy_shim = SQLAlchemyShim()
    sa = sqlalchemy
    sa.select = sqlalchemy_shim.select
else:
    sa = NotImported

# TODO: What happens if a user sidesteps and imports select instead of sa.select?
# TODO: Replace their import of sqlalchemy? Order of imports might make this not work.
# This might not be stable when sqlalchemy api changes more?
# Advantage: use the same method name as sqlalchemy is simpler.
# But can insulate from major changes if we use a different word.
# Use same word when possible, if not then use more
