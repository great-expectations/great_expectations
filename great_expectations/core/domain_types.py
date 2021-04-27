from enum import Enum, EnumMeta


# Reference: "https://stackoverflow.com/questions/33679930/how-to-extend-python-enum".
class MetaClsEnumJoin(EnumMeta):
    """
    Metaclass that creates a new `enum.Enum` from multiple existing Enums.

    @code
        from enum import Enum

        ENUMA = Enum('ENUMA', {'a': 1, 'b': 2})
        ENUMB = Enum('ENUMB', {'c': 3, 'd': 4})
        class ENUMJOINED(Enum, metaclass=MetaClsEnumJoin, enums=(ENUMA, ENUMB)):
            pass

        print(ENUMJOINED.a)
        print(ENUMJOINED.b)
        print(ENUMJOINED.c)
        print(ENUMJOINED.d)
    @endcode
    """

    # noinspection PyMethodParameters
    @classmethod
    def __prepare__(metaclass, name, bases, enums=None, **kwargs):
        """
        Generates the class's namespace.
        @param enums Iterable of `enum.Enum` classes to include in the new class.  Conflicts will
            be resolved by overriding existing values defined by Enums earlier in the iterable with
            values defined by Enums later in the iterable.
        """
        # kwargs = {"myArg1": 1, "myArg2": 2}
        if enums is None:
            raise ValueError(
                "Class keyword argument `enums` must be defined to use this metaclass."
            )

        # noinspection PyArgumentList
        ret = super().__prepare__(name, bases, **kwargs)
        for enm in enums:
            for item in enm:
                # noinspection PyUnresolvedReferences
                ret[item.name] = item.value  # Throws `TypeError` if conflict.
        return ret

    # noinspection PyMethodParameters
    def __new__(metaclass, name, bases, namespace, **kwargs):
        return super().__new__(metaclass, name, bases, namespace)
        # DO NOT send "**kwargs" to "type.__new__".  It won't catch them and
        # you'll get a "TypeError: type() takes 1 or 3 arguments" exception.

    def __init__(cls, name, bases, namespace, **kwargs):
        super().__init__(name, bases, namespace)
        # DO NOT send "**kwargs" to "type.__init__" in Python 3.5 and older.  You'll get a
        # "TypeError: type.__init__() takes no keyword arguments" exception.


class DomainTypes(Enum):
    """
    This is a base class for the different specific DomainTypes classes, each of which enumerates the particular variety
    of domain types (e.g., "StorageDomainTypes", "SemanticDomainTypes", "MetricDomainTypes", etc.).  Since the base
    "DomainTypes" extends "Enum", the JSON serialization, supported for the general "Enum" class, applies for all
    "DomainTypes" classes, too.
    """

    pass


class StorageDomainTypes(DomainTypes):
    SCHEMA = "schema"
    TABLE = "table"
    COLUMN = "column"
    COLUMN_PAIR = "column_pair"
    MULTICOLUMN = "multicolumn"


class SemanticDomainTypes(DomainTypes):
    NUMERIC = "numeric"
    VALUE_SET = "value_set"
    DATETIME = "datetime"


class MetricDomainTypes(
    Enum, metaclass=MetaClsEnumJoin, enums=(StorageDomainTypes, SemanticDomainTypes)
):
    pass
