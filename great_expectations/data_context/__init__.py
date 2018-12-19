from .pandas_context import PandasCSVDataContext
from .sqlalchemy_context import SqlAlchemyDataContext


def get_data_context(context_type, options, *args, **kwargs):
    """Return a data_context object which exposes options to list datasets and get a dataset from
    that context. This is a new API in Great Expectations 0.4, and is subject to rapid change.

    :param context_type: (string) one of "SqlAlchemy" or "PandasCSV"
    :param options: options to be passed to the data context's connect method.
    :return: a new DataContext object
    """
    if context_type == "SqlAlchemy":
        return SqlAlchemyDataContext(options, *args, **kwargs)
    elif context_type == "PandasCSV":
        return PandasCSVDataContext(options, *args, **kwargs)
    else:
        raise ValueError("Unknown data context.")
