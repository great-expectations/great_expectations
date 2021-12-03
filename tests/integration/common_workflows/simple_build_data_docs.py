import os
import tempfile

import great_expectations as ge
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DatasourceConfig,
    FilesystemStoreBackendDefaults,
)

print(f"Great Expectations location: {ge.__file__}")
print(f"Great Expectations version: {ge.__version__}")

data_context_config = DataContextConfig(
    datasources={"example_datasource": DatasourceConfig(class_name="PandasDatasource")},
    store_backend_defaults=FilesystemStoreBackendDefaults(
        root_directory=tempfile.mkdtemp() + os.sep + "my_greatexp_workdir"
    ),
)
context = BaseDataContext(project_config=data_context_config)
print(f"Great Expectations data_docs url: {context.build_data_docs()}")
