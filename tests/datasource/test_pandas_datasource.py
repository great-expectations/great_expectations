# -*- coding: utf-8 -*-

import pytest

import os
from ruamel.yaml import YAML

import pandas as pd
from six import PY3
import shutil


from great_expectations.exceptions import BatchKwargsError
from great_expectations.datasource import PandasDatasource
from great_expectations.datasource.types.batch_kwargs import (
    PathBatchKwargs,
    BatchId,
    BatchFingerprint
)
from great_expectations.dataset import PandasDataset

yaml = YAML(typ='safe')


@pytest.fixture(scope="module")
def test_folder_connection_path(tmp_path_factory):
    df1 = pd.DataFrame(
        {'col_1': [1, 2, 3, 4, 5], 'col_2': ['a', 'b', 'c', 'd', 'e']})
    path = str(tmp_path_factory.mktemp("test_folder_connection_path"))
    df1.to_csv(os.path.join(path, "test.csv"))

    return str(path)


def test_standalone_pandas_datasource(test_folder_connection_path):
    datasource = PandasDatasource('PandasCSV', base_directory=test_folder_connection_path)

    assert datasource.get_available_data_asset_names() == {"default": ["test"]}
    manual_batch_kwargs = PathBatchKwargs(path=os.path.join(str(test_folder_connection_path), "test.csv"))

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
    assert isinstance(dataset.batch_id, BatchId)
    assert isinstance(dataset.batch_fingerprint, BatchFingerprint)


def test_create_pandas_datasource(data_context, tmp_path_factory):
    basedir = tmp_path_factory.mktemp('test_create_pandas_datasource')
    name = "test_pandas_datasource"
    class_name = "PandasDatasource"
    # OLD STYLE: Remove even from record later...
    # type_ = "pandas"
    # data_context.add_datasource(name, type_, base_directory=str(basedir))
    data_context.add_datasource(name, class_name=class_name, base_directory=str(basedir))
    data_context_config = data_context.get_config()

    assert name in data_context_config["datasources"]
    assert data_context_config["datasources"][name]["class_name"] == class_name
    # assert data_context_config["datasources"][name]["type"] == type_

    # We should now see updated configs
    # Finally, we should be able to confirm that the folder structure is as expected
    with open(os.path.join(data_context.root_directory, "great_expectations.yml"), "r") as data_context_config_file:
        data_context_file_config = yaml.load(data_context_config_file)

    assert data_context_file_config["datasources"][name] == data_context_config["datasources"][name]

    # We should have added a default generator built from the default config
    assert data_context_file_config["datasources"][name]["generators"]["default"]["class_name"] == \
        "SubdirReaderGenerator"


def test_pandas_datasource_custom_data_asset(data_context, test_folder_connection_path):
    name = "test_pandas_datasource"
    # type_ = "pandas"
    class_name = "PandasDatasource"

    data_asset_type_config = {
        "module_name": "custom_pandas_dataset",
        "class_name": "CustomPandasDataset"
    }
    data_context.add_datasource(name,
                                class_name=class_name,
                                base_directory=test_folder_connection_path,
                                data_asset_type=data_asset_type_config)

    # We should now see updated configs
    with open(os.path.join(data_context.root_directory, "great_expectations.yml"), "r") as data_context_config_file:
        data_context_file_config = yaml.load(data_context_config_file)

    assert data_context_file_config["datasources"][name]["data_asset_type"]["module_name"] == "custom_pandas_dataset"
    assert data_context_file_config["datasources"][name]["data_asset_type"]["class_name"] == "CustomPandasDataset"

    # We should be able to get a dataset of the correct type from the datasource.
    data_asset_name = "test_pandas_datasource/default/test"
    data_context.create_expectation_suite(data_asset_name=data_asset_name, expectation_suite_name="default")
    batch = data_context.get_batch(data_asset_name=data_asset_name,
                                   expectation_suite_name="default",
                                   batch_kwargs=data_context.yield_batch_kwargs(data_asset_name=data_asset_name)
    )
    assert type(batch).__name__ == "CustomPandasDataset"
    res = batch.expect_column_values_to_have_odd_lengths("col_2")
    assert res["success"] is True


def test_pandas_source_readcsv(data_context, tmp_path_factory):
    if not PY3:
        # We don't specifically test py2 unicode reading since this test is about our handling of kwargs *to* read_csv
        pytest.skip()
    basedir = tmp_path_factory.mktemp('test_create_pandas_datasource')
    shutil.copy("./tests/test_sets/unicode.csv", basedir)
    data_context.add_datasource("mysource",
                                module_name="great_expectations.datasource",
                                class_name="PandasDatasource",
                                reader_options={"encoding": "utf-8"},
                                base_directory=str(basedir))

    data_context.create_expectation_suite(data_asset_name="mysource/unicode", expectation_suite_name="default")
    batch = data_context.get_batch("mysource/unicode",
                                   "default",
                                   data_context.yield_batch_kwargs("mysource/unicode"))
    assert len(batch["Μ"] == 1)
    assert "😁" in list(batch["Μ"])

    data_context.add_datasource("mysource2",
                                module_name="great_expectations.datasource",
                                class_name="PandasDatasource",
                                base_directory=str(basedir))

    data_context.create_expectation_suite(data_asset_name="mysource2/unicode", expectation_suite_name="default")
    batch = data_context.get_batch("mysource2/unicode",
                                   "default",
                                   data_context.yield_batch_kwargs("mysource2/unicode")
    )
    assert "😁" in list(batch["Μ"])

    data_context.add_datasource("mysource3",
                                module_name="great_expectations.datasource",
                                class_name="PandasDatasource",
                                reader_options={"encoding": "utf-16"},
                                base_directory=str(basedir))
    with pytest.raises(UnicodeError, match="UTF-16 stream does not start with BOM"):
        data_context.create_expectation_suite(data_asset_name="mysource3/unicode", expectation_suite_name="default")
        batch = data_context.get_batch("mysource3/unicode",
                                       "default",
                                       data_context.yield_batch_kwargs("mysource3/unicode")
                                       )

    with pytest.raises(LookupError, match="unknown encoding: blarg"):
        batch = data_context.get_batch("mysource/unicode",
                                       "default",
                                       batch_kwargs=data_context.yield_batch_kwargs("mysource/unicode"),
                                       encoding='blarg')

    batch = data_context.get_batch("mysource2/unicode",
                                   "default",
                                   batch_kwargs=data_context.yield_batch_kwargs("mysource2/unicode"),
                                   encoding='utf-8'
                                   )
    assert "😁" in list(batch["Μ"])


def test_invalid_reader_pandas_datasource(tmp_path_factory):
    basepath = str(tmp_path_factory.mktemp("test_invalid_reader_pandas_datasource"))
    datasource = PandasDatasource('mypandassource', base_directory=basepath)

    with open(os.path.join(basepath, "idonotlooklikeacsvbutiam.notrecognized"), "w") as newfile:
        newfile.write("a,b\n1,2\n3,4\n")

    with pytest.raises(BatchKwargsError) as exc:
        datasource.get_data_asset("idonotlooklikeacsvbutiam.notrecognized", batch_kwargs={
            "path": os.path.join(basepath, "idonotlooklikeacsvbutiam.notrecognized")
        })
        assert "Unable to determine reader for path" in exc.message

    with pytest.raises(BatchKwargsError) as exc:
        datasource.get_data_asset("idonotlooklikeacsvbutiam.notrecognized", batch_kwargs={
            "path": os.path.join(basepath, "idonotlooklikeacsvbutiam.notrecognized")
        }, reader_method="blarg")
        assert "Unknown reader method: blarg" in exc.message

    dataset = datasource.get_data_asset("idonotlooklikeacsvbutiam.notrecognized", batch_kwargs={
            "path": os.path.join(basepath, "idonotlooklikeacsvbutiam.notrecognized")
        }, reader_method="csv", header=0)
    assert dataset["a"][0] == 1
