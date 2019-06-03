import pytest

import os

@pytest.fixture()
def filesystem_csv(tmp_path_factory):
    base_dir = tmp_path_factory.mktemp('test_file_kwargs_generator')
    # Put a few files in the directory
    with open(os.path.join(base_dir, "f1.csv"), "w") as outfile:
        outfile.writelines(["a\tb\tc\n"])
    with open(os.path.join(base_dir, "f2.csv"), "w") as outfile:
        outfile.writelines(["a\tb\tc\n"])

    os.makedirs(os.path.join(base_dir, "f3"))
    with open(os.path.join(base_dir, "f3", "f3_20190101.csv"), "w") as outfile:
        outfile.writelines(["a\tb\tc\n"])
    with open(os.path.join(base_dir, "f3", "f3_20190102.csv"), "w") as outfile:
        outfile.writelines(["a\tb\tc\n"])

    return base_dir


def test_file_kwargs_generator(data_context, filesystem_csv):
    base_dir = filesystem_csv

    datasource = data_context.add_datasource("default", "pandas", base_directory=str(base_dir))
    generator = datasource.get_generator("default")
    known_data_asset_names = set(datasource.list_data_asset_names())

    assert known_data_asset_names == set([
        "f1", "f2", "f3"
    ])

    f1_batches = [batch_kwargs for batch_kwargs in generator.get_iterator("f1")]
    assert f1_batches[0] == {
            "path": os.path.join(base_dir, "f1.csv")
        }
    assert len(f1_batches) == 1

    f3_batches = [batch_kwargs for batch_kwargs in generator.get_iterator("f3")]
    expected_batches = [
        {
            "path": os.path.join(base_dir, "f3", "f3_20190101.csv")
        },
        {
            "path": os.path.join(base_dir, "f3", "f3_20190102.csv")
        }
    ]
    for batch in expected_batches:
        assert batch in f3_batches
    assert len(f3_batches) == 2

def test_file_kwargs_generator_error(data_context, filesystem_csv):
    base_dir = filesystem_csv
    data_context.add_datasource("default", "pandas", base_directory=str(base_dir))

    with pytest.raises(FileNotFoundError, match="f4"):
        data_context.get_data_asset("default", "f4")