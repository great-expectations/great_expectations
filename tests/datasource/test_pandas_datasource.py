import pytest

import os

import pandas as pd

from great_expectations.datasource import PandasDatasource
from great_expectations.datasource.types.batch_kwargs import (
    PathBatchKwargs,
    PathBatchId,
    BatchFingerprint
)
from great_expectations.dataset import PandasDataset


@pytest.fixture(scope="module")
def test_folder_connection_path(tmp_path_factory):
    df1 = pd.DataFrame(
        {'col_1': [1, 2, 3, 4, 5], 'col_2': ['a', 'b', 'c', 'd', 'e']})
    path = str(tmp_path_factory.mktemp("test_folder_connection_path"))
    df1.to_csv(os.path.join(path, "test.csv"))

    return str(path)


def test_standalone_pandas_datasource(test_folder_connection_path):
    datasource = PandasDatasource('PandasCSV', base_directory=test_folder_connection_path)

    assert datasource.get_available_data_asset_names() == {"default": {"test"}}
    manual_batch_kwargs = datasource.build_batch_kwargs(
        "default",
        os.path.join(str(test_folder_connection_path), "test.csv"))

    # Get the default (subdir_path) generator
    generator = datasource.get_generator()
    auto_batch_kwargs = generator.yield_batch_kwargs("test")

    assert manual_batch_kwargs["path"] == auto_batch_kwargs["path"]

    # Include some extra kwargs...
    # Note that we are using get_data_asset NOT get_batch here, since we are standalone (no batch concept)
    dataset = datasource.get_data_asset("test",
                                        generator_name="default", batch_kwargs=auto_batch_kwargs,
                                        sep=",", header=0, index_col=0)
    assert isinstance(dataset, PandasDataset)
    assert (dataset["col_1"] == [1, 2, 3, 4, 5]).all()

    ## A datasource should always return an object with a typed batch_id
    assert isinstance(dataset.batch_kwargs, PathBatchKwargs)
    assert isinstance(dataset.batch_id, PathBatchId)
    assert isinstance(dataset.batch_fingerprint, BatchFingerprint)
