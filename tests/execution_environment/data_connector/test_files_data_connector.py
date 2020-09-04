import logging

import pytest

try:
    from unittest import mock
except ImportError:
    import mock

import great_expectations.execution_environment.data_connector
from great_expectations.core.id_dict import BatchKwargs
from great_expectations.execution_environment.data_connector import FilesDataConnector
from great_expectations.execution_environment.execution_environment import (
    ExecutionEnvironment as exec,
)
from great_expectations.execution_environment.execution_environment import *
from great_expectations.execution_environment.types import (
    PandasDatasourceBatchKwargs,
    PathBatchKwargs,
    SparkDFDatasourceBatchKwargs,
)


@pytest.fixture(scope="module")
def mocked_glob_kwargs(basic_pandas_execution_engine):
    test_asset_globs = {
        "test_asset": {
            "glob": "*",
            "partition_regex": r"^((19|20)\d\d[- /.]?(0[1-9]|1[012])[- /.]?(0[1-9]|[12][0-9]|3[01]))_(.*)\.csv",
            "match_group_id": 1,
        }
    }

    with mock.patch("glob.glob") as mock_glob:
        mock_glob_match = [
            "20190101__my_data.csv",
            "20190102__my_data.csv",
            "20190103__my_data.csv",
            "20190104__my_data.csv",
            "20190105__my_data.csv",
        ]
        mock_glob.return_value = mock_glob_match

        glob_generator = FilesDataConnector(
            name="test_data_connector",
            base_directory=mock_glob,
            asset_globs=test_asset_globs,
        )

        kwargs = [
            kwargs
            for kwargs in glob_generator.get_iterator(data_asset_name="test_asset")
        ]
        print(kwargs)

    return kwargs
