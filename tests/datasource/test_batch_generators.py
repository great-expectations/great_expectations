import os

import pytest

from great_expectations.datasource.batch_kwargs_generator import (
    SubdirReaderBatchKwargsGenerator,
)

try:
    from unittest import mock
except ImportError:
    from unittest import mock  # noqa: F401


@pytest.mark.big
def test_file_kwargs_generator(
    data_context_parameterized_expectation_suite, filesystem_csv
):
    base_dir = filesystem_csv

    datasource = data_context_parameterized_expectation_suite.add_datasource(
        "default",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
        batch_kwargs_generators={
            "subdir_reader": {
                "class_name": "SubdirReaderBatchKwargsGenerator",
                "base_directory": str(base_dir),
            }
        },
    )

    generator = datasource.get_batch_kwargs_generator("subdir_reader")
    known_data_asset_names = datasource.get_available_data_asset_names()

    # Use set to avoid order dependency
    assert set(known_data_asset_names["subdir_reader"]["names"]) == {
        ("f1", "file"),
        ("f2", "file"),
        ("f3", "directory"),
    }

    f1_batches = [
        batch_kwargs["path"]
        for batch_kwargs in generator.get_iterator(data_asset_name="f1")
    ]
    assert len(f1_batches) == 1
    expected_batches = [{"path": os.path.join(base_dir, "f1.csv")}]  # noqa: PTH118
    for batch in expected_batches:
        assert batch["path"] in f1_batches

    f3_batches = [
        batch_kwargs["path"]
        for batch_kwargs in generator.get_iterator(data_asset_name="f3")
    ]
    assert len(f3_batches) == 2
    expected_batches = [
        {"path": os.path.join(base_dir, "f3", "f3_20190101.csv")},  # noqa: PTH118
        {"path": os.path.join(base_dir, "f3", "f3_20190102.csv")},  # noqa: PTH118
    ]
    for batch in expected_batches:
        assert batch["path"] in f3_batches


@pytest.mark.big
def test_file_kwargs_generator_extensions(tmp_path_factory):
    """csv, xls, parquet, json should be recognized file extensions"""
    basedir = str(tmp_path_factory.mktemp("test_file_kwargs_generator_extensions"))

    # Do not include: invalid extension
    with open(os.path.join(basedir, "f1.blarg"), "w") as outfile:  # noqa: PTH118
        outfile.write("\n\n\n")
    # Include
    with open(os.path.join(basedir, "f2.csv"), "w") as outfile:  # noqa: PTH118
        outfile.write("\n\n\n")
    # Do not include: valid subdir, but no valid files in it
    os.mkdir(os.path.join(basedir, "f3"))  # noqa: PTH102, PTH118
    with open(
        os.path.join(basedir, "f3", "f3_1.blarg"), "w"  # noqa: PTH118
    ) as outfile:
        outfile.write("\n\n\n")
    with open(
        os.path.join(basedir, "f3", "f3_2.blarg"), "w"  # noqa: PTH118
    ) as outfile:
        outfile.write("\n\n\n")
    # Include: valid subdir with valid files
    os.mkdir(os.path.join(basedir, "f4"))  # noqa: PTH102, PTH118
    with open(os.path.join(basedir, "f4", "f4_1.csv"), "w") as outfile:  # noqa: PTH118
        outfile.write("\n\n\n")
    with open(os.path.join(basedir, "f4", "f4_2.csv"), "w") as outfile:  # noqa: PTH118
        outfile.write("\n\n\n")
    # Do not include: valid extension, but dot prefix
    with open(os.path.join(basedir, ".f5.csv"), "w") as outfile:  # noqa: PTH118
        outfile.write("\n\n\n")

    # Include: valid extensions
    with open(os.path.join(basedir, "f6.tsv"), "w") as outfile:  # noqa: PTH118
        outfile.write("\n\n\n")
    with open(os.path.join(basedir, "f7.xls"), "w") as outfile:  # noqa: PTH118
        outfile.write("\n\n\n")
    with open(os.path.join(basedir, "f8.parquet"), "w") as outfile:  # noqa: PTH118
        outfile.write("\n\n\n")
    with open(os.path.join(basedir, "f9.xls"), "w") as outfile:  # noqa: PTH118
        outfile.write("\n\n\n")
    with open(os.path.join(basedir, "f0.json"), "w") as outfile:  # noqa: PTH118
        outfile.write("\n\n\n")

    g1 = SubdirReaderBatchKwargsGenerator(datasource="foo", base_directory=basedir)

    g1_assets = g1.get_available_data_asset_names()
    # Use set in test to avoid order issues
    assert set(g1_assets["names"]) == {
        ("f7", "file"),
        ("f4", "directory"),
        ("f6", "file"),
        ("f0", "file"),
        ("f2", "file"),
        ("f9", "file"),
        ("f8", "file"),
    }
