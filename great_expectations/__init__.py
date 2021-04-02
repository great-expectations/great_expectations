# Set up version information immediately
from ._version import get_versions  # isort:skip

__version__ = get_versions()["version"]  # isort:skip
del get_versions  # isort:skip


# 20210401 Will - Temporary fix to warn users against current incompatibility with Sqlalchemy 1.4.0
# To be removed once a better fix is implemented

from packaging.version import parse as parse_version

from great_expectations.exceptions.exceptions import GreatExpectationsError

try:
    import sqlalchemy as sa

    if parse_version(sa.__version__) >= parse_version("1.4.0"):
        raise GreatExpectationsError(
            f"""

        Great Expectations version {__version__} is currently incompatible with SqlAlchemy 1.4.0 and higher.
        You currently have SqlAlchemy version {sa.__version__}. Please downgrade SqlAlchemy to < 1.4.0 while we work on a proper fix.
        """
        )
except (ImportError, ModuleNotFoundError):
    pass

from great_expectations.data_context import DataContext

from .util import (
    from_pandas,
    get_context,
    measure_execution_time,
    read_csv,
    read_excel,
    read_feather,
    read_json,
    read_parquet,
    read_pickle,
    read_table,
    validate,
)

# from great_expectations.expectations.core import *
# from great_expectations.expectations.metrics import *


rtd_url_ge_version = __version__.replace(".", "_")
