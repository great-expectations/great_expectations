import os

from great_expectations import DataContext
from great_expectations.cli.datasource import get_batch_kwargs


def test_get_batch_kwargs_for_specific_dataasset(empty_data_context, filesystem_csv):
    project_root_dir = empty_data_context.root_directory
    context = DataContext(project_root_dir)
    base_directory = str(filesystem_csv)

    context.add_datasource(
        "wow_a_datasource",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
        batch_kwargs_generators={
            "subdir_reader": {
                "class_name": "SubdirReaderBatchKwargsGenerator",
                "base_directory": base_directory,
            }
        },
    )

    batch = get_batch_kwargs(
        context,
        datasource_name=None,
        batch_kwargs_generator_name=None,
        data_asset_name="f1",
        additional_batch_kwargs={},
    )

    expected_batch = {
        "data_asset_name": "f1",
        "datasource": "wow_a_datasource",
        "path": os.path.join(filesystem_csv, "f1.csv"),
    }
    assert batch == expected_batch
