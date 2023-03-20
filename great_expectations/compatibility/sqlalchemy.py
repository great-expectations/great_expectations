"""Compatibility shims for SQLAlchemy.

Currently supported v2.0.x, v1.4.x, v1.3.x.
"""
from __future__ import annotations

from packaging.version import Version

from great_expectations.util import NotImported

try:
    import sqlalchemy
except ImportError:
    sqlalchemy = NotImported("sqlalchemy not found, please install.")


class SQLAlchemyShim:
    """FILL ME IN"""

    def __init__(self):
        """FILL ME IN"""
        # This is where we determine which version of sqlalchemy is installed if any.
        # Alternatively, fill this in on first use of any method so it doesn't slow down loading
        # of the library.
        self._sqlalchemy_version: None | Version = None
        pass

    @property
    def sqlalchemy_version(self) -> None | Version:
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

    def is_compatible_with_1_3_x(self) -> bool:
        """"""
        return Version("1.3.18") <= self.sqlalchemy_version < Version("1.4.0")

    def is_compatible_with_1_4_x(self) -> bool:
        """"""
        return Version("1.4.0") <= self.sqlalchemy_version < Version("1.5.0")

    def is_compatible_with_2_0_x(self) -> bool:
        """"""
        return Version("2.0.0") <= self.sqlalchemy_version < Version("2.1.0")

    def select(self, *args, **kwargs):
        if self.is_compatible_with_1_3_x():
            # Convert args to a list for compatibility with 1.3.x
            return sqlalchemy.select(args, **kwargs)
        elif self.is_compatible_with_1_4_x() or self.is_compatible_with_2_0_x():
            return sqlalchemy.select(*args, **kwargs)
