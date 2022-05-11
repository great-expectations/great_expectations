from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions
from great_expectations.data_context import DataContext

from .util import (
    from_pandas,
    get_context,
    read_csv,
    read_excel,
    read_feather,
    read_json,
    read_parquet,
    read_pickle,
    read_sas,
    read_table,
    validate,
)

rtd_url_ge_version = __version__.replace(".", "_")
