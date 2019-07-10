import pytest

import os

from great_expectations.exceptions import DataContextError
from great_expectations.datasource import SubdirReaderGenerator, GlobReaderGenerator


def test_file_kwargs_generator(data_context, filesystem_csv):
    base_dir = filesystem_csv

    datasource = data_context.add_datasource("default", "pandas", base_directory=str(base_dir))
    generator = datasource.get_generator("default")
    known_data_asset_names = datasource.get_available_data_asset_names()

    assert known_data_asset_names["default"] == {"f1", "f2", "f3"}

    f1_batches = [batch_kwargs for batch_kwargs in generator.get_iterator("f1")]
    assert len(f1_batches) == 1
    assert "timestamp" in f1_batches[0]
    del f1_batches[0]["timestamp"]
    assert f1_batches[0] == {
            "path": os.path.join(base_dir, "f1.csv"),
            "sep": None,
            "engine": "python"
        }

    f3_batches = [batch_kwargs["path"] for batch_kwargs in generator.get_iterator("f3")]
    expected_batches = [
        {
            "path": os.path.join(base_dir, "f3", "f3_20190101.csv")
        },
        {
            "path": os.path.join(base_dir, "f3", "f3_20190102.csv")
        }
    ]
    for batch in expected_batches:
        assert batch["path"] in f3_batches
    assert len(f3_batches) == 2


def test_file_kwargs_generator_error(data_context, filesystem_csv):
    base_dir = filesystem_csv
    data_context.add_datasource("default", "pandas", base_directory=str(base_dir))

    with pytest.raises(DataContextError) as exc:
        data_context.get_batch("f4")
        assert "f4" in exc.message


def test_glob_reader_generator(tmp_path_factory):
    """Provides an example of how glob generator works: we specify our own
    names for data_assets, and an associated glob; the generator
    will take care of providing batches consisting of one file per
    batch corresponding to the glob."""
    
    basedir = str(tmp_path_factory.mktemp("test_glob_reader_generator"))

    with open(os.path.join(basedir, "f1.blarg"), "w") as outfile:
        outfile.write("\n\n\n")
    with open(os.path.join(basedir, "f2.csv"), "w") as outfile:
        outfile.write("\n\n\n")
    with open(os.path.join(basedir, "f3.blarg"), "w") as outfile:
        outfile.write("\n\n\n")
    with open(os.path.join(basedir, "f4.blarg"), "w") as outfile:
        outfile.write("\n\n\n")
    with open(os.path.join(basedir, "f5.blarg"), "w") as outfile:
        outfile.write("\n\n\n")
    with open(os.path.join(basedir, "f6.blarg"), "w") as outfile:
        outfile.write("\n\n\n")
    with open(os.path.join(basedir, "f7.xls"), "w") as outfile:
        outfile.write("\n\n\n")
    with open(os.path.join(basedir, "f8.parquet"), "w") as outfile:
        outfile.write("\n\n\n")
    with open(os.path.join(basedir, "f9.xls"), "w") as outfile:
        outfile.write("\n\n\n")
    with open(os.path.join(basedir, "f0.json"), "w") as outfile:
        outfile.write("\n\n\n")

    g2 = GlobReaderGenerator(base_directory=basedir, asset_globs={
        "blargs": "*.blarg",
        "fs": "f*"
    })

    g2_assets = g2.get_available_data_asset_names()
    assert g2_assets == {"blargs", "fs"}

    blargs_kwargs = [x["path"] for x in g2.get_iterator("blargs")]
    real_blargs = [
        os.path.join(basedir, "f1.blarg"),
        os.path.join(basedir, "f3.blarg"),
        os.path.join(basedir, "f4.blarg"),
        os.path.join(basedir, "f5.blarg"),
        os.path.join(basedir, "f6.blarg")
    ]
    for kwargs in real_blargs:
        assert kwargs in blargs_kwargs
    assert len(blargs_kwargs) == len(real_blargs)


def test_file_kwargs_generator_extensions(tmp_path_factory):
    """csv, xls, parquet, json should be recognized file extensions"""
    basedir = str(tmp_path_factory.mktemp("test_file_kwargs_generator_extensions"))

    # Do not include: invalid extension
    with open(os.path.join(basedir, "f1.blarg"), "w") as outfile:
        outfile.write("\n\n\n")
    # Include
    with open(os.path.join(basedir, "f2.csv"), "w") as outfile:
        outfile.write("\n\n\n")
    # Do not include: valid subdir, but no valid files in it
    os.mkdir(os.path.join(basedir, "f3"))
    with open(os.path.join(basedir, "f3", "f3_1.blarg"), "w") as outfile:
        outfile.write("\n\n\n")
    with open(os.path.join(basedir, "f3", "f3_2.blarg"), "w") as outfile:
        outfile.write("\n\n\n")
    # Include: valid subdir with valid files
    os.mkdir(os.path.join(basedir, "f4"))
    with open(os.path.join(basedir, "f4", "f4_1.csv"), "w") as outfile:
        outfile.write("\n\n\n")
    with open(os.path.join(basedir, "f4", "f4_2.csv"), "w") as outfile:
        outfile.write("\n\n\n")
    # Do not include: valid extension, but dot prefix
    with open(os.path.join(basedir, ".f5.csv"), "w") as outfile:
        outfile.write("\n\n\n")
    
    # Include: valid extensions
    with open(os.path.join(basedir, "f6.tsv"), "w") as outfile:
        outfile.write("\n\n\n")
    with open(os.path.join(basedir, "f7.xls"), "w") as outfile:
        outfile.write("\n\n\n")
    with open(os.path.join(basedir, "f8.parquet"), "w") as outfile:
        outfile.write("\n\n\n")
    with open(os.path.join(basedir, "f9.xls"), "w") as outfile:
        outfile.write("\n\n\n")
    with open(os.path.join(basedir, "f0.json"), "w") as outfile:
        outfile.write("\n\n\n")

    g1 = SubdirReaderGenerator(base_directory=basedir)

    g1_assets = g1.get_available_data_asset_names()
    assert g1_assets == {"f2", "f4", "f6", "f7", "f8", "f9", "f0"}
