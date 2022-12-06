import os
import tempfile

import great_expectations as gx
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DatasourceConfig,
    FilesystemStoreBackendDefaults,
)

"""
A simple test to verify that `context.build_data_docs()` works as expected.

As indicated in issue #3772, calling `context.build_data_docs()` raised an unexpected exception
when Great Expectations was installed in a non-filesystem location (i.e. it failed when
GX was installed inside a zip file -which is a location allowed by PEP 273-).

Therefore, this test is intended to be run after installing GX inside a zip file and
then setting the appropriate PYTHONPATH env variable. If desired, this test can also be
run after installing GX in a normal filesystem location (i.e. a directory).

This test is OK if it finishes without raising an exception.

To make it easier to debug this test, it prints:
* The location of the GX library: to verify that we are testing the library that we want
* The version of the GX library: idem
* data_docs url: If everything works, this will be a url (e.g. starting with file://...)


Additional info: https://github.com/great-expectations/great_expectations/issues/3772 and
https://www.python.org/dev/peps/pep-0273/
"""

print(f"Great Expectations location: {gx.__file__}")
print(f"Great Expectations version: {gx.__version__}")

data_context_config = DataContextConfig(
    datasources={"example_datasource": DatasourceConfig(class_name="PandasDatasource")},
    store_backend_defaults=FilesystemStoreBackendDefaults(
        root_directory=tempfile.mkdtemp() + os.sep + "my_greatexp_workdir"
    ),
)
context = BaseDataContext(project_config=data_context_config)
print(f"Great Expectations data_docs url: {context.build_data_docs()}")
