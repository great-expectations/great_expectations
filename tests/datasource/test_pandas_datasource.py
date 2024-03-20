import os
from functools import partial

import pandas as pd
import pytest

from great_expectations.core.batch import Batch, BatchMarkers
from great_expectations.core.util import nested_update
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from great_expectations.data_context.types.base import (
    DataContextConfigSchema,
    DatasourceConfig,
    datasourceConfigSchema,
)
from great_expectations.datasource import PandasDatasource
from great_expectations.datasource.datasource_serializer import (
    YAMLReadyDictDatasourceConfigSerializer,
)
from great_expectations.datasource.types import PathBatchKwargs
from great_expectations.exceptions import BatchKwargsError

yaml = YAMLHandler()


@pytest.mark.filesystem
def test_standalone_pandas_datasource(test_folder_connection_path_csv):
    datasource = PandasDatasource(
        "PandasCSV",
        batch_kwargs_generators={
            "subdir_reader": {
                "class_name": "SubdirReaderBatchKwargsGenerator",
                "base_directory": test_folder_connection_path_csv,
            }
        },
    )

    assert datasource.get_available_data_asset_names() == {
        "subdir_reader": {"names": [("test", "file")], "is_complete_list": True}
    }
    manual_batch_kwargs = PathBatchKwargs(
        path=os.path.join(  # noqa: PTH118
            str(test_folder_connection_path_csv), "test.csv"
        )
    )

    generator = datasource.get_batch_kwargs_generator("subdir_reader")
    auto_batch_kwargs = generator.yield_batch_kwargs("test")

    assert manual_batch_kwargs["path"] == auto_batch_kwargs["path"]

    # Include some extra kwargs...
    # auto_batch_kwargs.update(
    #     {"reader_options": {"sep": ",", "header": 0, "index_col": 0}}
    # )
    auto_batch_kwargs.update({"reader_options": {"sep": ","}})
    batch = datasource.get_batch(batch_kwargs=auto_batch_kwargs)
    assert isinstance(batch, Batch)
    dataset = batch.data
    assert (dataset["col_1"] == [1, 2, 3, 4, 5]).all()
    assert len(dataset) == 5

    # A datasource should always return an object with a typed batch_id
    assert isinstance(batch.batch_kwargs, PathBatchKwargs)
    assert isinstance(batch.batch_markers, BatchMarkers)


@pytest.mark.filesystem
def test_create_pandas_datasource(data_context_parameterized_expectation_suite, tmp_path_factory):
    basedir = tmp_path_factory.mktemp("test_create_pandas_datasource")
    name = "test_pandas_datasource"
    class_name = "PandasDatasource"
    data_context_parameterized_expectation_suite.add_datasource(
        name,
        class_name=class_name,
        batch_kwargs_generators={
            "subdir_reader": {
                "class_name": "SubdirReaderBatchKwargsGenerator",
                "base_directory": str(basedir),
            }
        },
    )

    data_context_config = data_context_parameterized_expectation_suite.get_config()

    assert name in data_context_config["datasources"]
    assert data_context_config["datasources"][name]["class_name"] == class_name
    # assert data_context_config["datasources"][name]["type"] == type_

    # We should now see updated configs
    # Finally, we should be able to confirm that the folder structure is as expected
    with open(
        os.path.join(  # noqa: PTH118
            data_context_parameterized_expectation_suite.root_directory,
            FileDataContext.GX_YML,
        ),
    ) as data_context_config_file:
        data_context_file_config = yaml.load(data_context_config_file)

    # To match what we expect out of the yaml file, we need to deserialize our config using the same mechanism  # noqa: E501
    serializer = YAMLReadyDictDatasourceConfigSerializer(schema=datasourceConfigSchema)
    datasource_config: DatasourceConfig = DataContextConfigSchema().dump(data_context_config)[
        "datasources"
    ][name]
    expected_serialized_datasource_config: dict = serializer.serialize(datasource_config)
    assert data_context_file_config["datasources"][name] == expected_serialized_datasource_config

    # We should have added a default generator built from the default config
    assert (
        data_context_file_config["datasources"][name]["batch_kwargs_generators"]["subdir_reader"][
            "class_name"
        ]
        == "SubdirReaderBatchKwargsGenerator"
    )


@pytest.mark.filesystem
def test_invalid_reader_pandas_datasource(tmp_path_factory):
    basepath = str(tmp_path_factory.mktemp("test_invalid_reader_pandas_datasource"))
    datasource = PandasDatasource(
        "mypandassource",
        batch_kwargs_generators={
            "subdir_reader": {
                "class_name": "SubdirReaderBatchKwargsGenerator",
                "base_directory": basepath,
            }
        },
    )

    with open(
        os.path.join(  # noqa: PTH118
            basepath, "idonotlooklikeacsvbutiam.notrecognized"
        ),
        "w",
    ) as newfile:
        newfile.write("a,b\n1,2\n3,4\n")

    with pytest.raises(BatchKwargsError) as exc:
        datasource.get_batch(
            batch_kwargs={
                "path": os.path.join(  # noqa: PTH118
                    basepath, "idonotlooklikeacsvbutiam.notrecognized"
                )
            }
        )
        assert "Unable to determine reader for path" in exc.value.message

    with pytest.raises(BatchKwargsError) as exc:
        datasource.get_batch(
            batch_kwargs={
                "path": os.path.join(  # noqa: PTH118
                    basepath, "idonotlooklikeacsvbutiam.notrecognized"
                ),
                "reader_method": "blarg",
            }
        )
        assert "Unknown reader method: blarg" in exc.value.message

    batch = datasource.get_batch(
        batch_kwargs={
            "path": os.path.join(  # noqa: PTH118
                basepath, "idonotlooklikeacsvbutiam.notrecognized"
            ),
            "reader_method": "read_csv",
            "reader_options": {"header": 0},
        }
    )
    assert batch.data["a"][0] == 1


@pytest.mark.filesystem
def test_read_limit(test_folder_connection_path_csv):
    datasource = PandasDatasource(
        "PandasCSV",
        batch_kwargs_generators={
            "subdir_reader": {
                "class_name": "SubdirReaderBatchKwargsGenerator",
                "base_directory": test_folder_connection_path_csv,
            }
        },
    )

    batch_kwargs = PathBatchKwargs(
        {
            "path": os.path.join(  # noqa: PTH118
                str(test_folder_connection_path_csv), "test.csv"
            ),
            # "reader_options": {"sep": ",", "header": 0, "index_col": 0},
            "reader_options": {"sep": ","},
        }
    )
    nested_update(batch_kwargs, datasource.process_batch_parameters(limit=1))

    batch = datasource.get_batch(batch_kwargs=batch_kwargs)
    assert isinstance(batch, Batch)
    dataset = batch.data
    assert (dataset["col_1"] == [1]).all()
    assert len(dataset) == 1

    # A datasource should always return an object with a typed batch_id
    assert isinstance(batch.batch_kwargs, PathBatchKwargs)
    assert isinstance(batch.batch_markers, BatchMarkers)


@pytest.mark.unit
def test_process_batch_parameters():
    batch_kwargs = PandasDatasource("test").process_batch_parameters(limit=1)
    assert batch_kwargs == {"reader_options": {"nrows": 1}}

    batch_kwargs = PandasDatasource("test").process_batch_parameters(
        dataset_options={"caching": False}
    )
    assert batch_kwargs == {"dataset_options": {"caching": False}}


@pytest.mark.unit
@pytest.mark.parametrize(
    "reader_fn",
    [pd.read_csv, pd.read_excel, pd.read_parquet, pd.read_pickle, pd.read_sas],
)
def test_infer_default_options_partial_functions(reader_fn):
    datasource = PandasDatasource()
    reader_fn_partial = partial(reader_fn)
    assert datasource._infer_default_options(
        reader_fn_partial, {}
    ) == datasource._infer_default_options(reader_fn, {})
