"""Abstract base classes.

These are necessary to avoid circular imports between core.py and fields.py.

.. warning::

    This module is treated as private API.
    Users should not need to use this module directly.
"""


class FieldABC:
    """Abstract base class from which all Field classes inherit."""

    parent = None
    name = None

    def serialize(self, attr, obj, accessor=None) -> None:
        raise NotImplementedError

    def deserialize(self, value) -> None:
        raise NotImplementedError

    def _serialize(self, value, attr, obj, **kwargs) -> None:
        raise NotImplementedError

    def _deserialize(self, value, attr, data, **kwargs) -> None:
        raise NotImplementedError


class SchemaABC:
    """Abstract base class from which all Schemas inherit."""

    def dump(self, obj, *, many: bool = None) -> None:
        raise NotImplementedError

    def dumps(self, obj, *, many: bool = None) -> None:
        raise NotImplementedError

    def load(self, data, *, many: bool = None, partial=None, unknown=None) -> None:
        raise NotImplementedError

    def loads(
        self, json_data, *, many: bool = None, partial=None, unknown=None, **kwargs
    ) -> None:
        raise NotImplementedError
