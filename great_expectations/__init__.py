from ._version import get_versions
__version__ = get_versions()['version']
rtd_url_ge_version = __version__.replace(".", "_")
del get_versions

from .util import from_pandas, read_csv, read_excel, read_json, read_parquet, read_table, validate

from great_expectations.data_context import DataContext
