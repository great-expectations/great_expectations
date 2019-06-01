import pytest

def test_file_kwargs_genenrator(data_context, tmpdir):
    # Put a few files in the directory
    with open(os.path.join(tmpdir, "f1.csv"), "w") as outfile:
        outfile.writelines(["a\tb\tc\n"])
    with open(os.path.join(tmpdir, "f2.csv"), "w") as outfile:
        outfile.writelines(["a\tb\tc\n"])

    os.makedirs(os.path.join(tmpdir, "f3"))
    with open(os.path.join(tmpdir, "f3", "f3_20190101.csv"), "w") as outfile:
        outfile.writelines(["a\tb\tc\n"])
    with open(os.path.join(tmpdir, "f3", "f3_20190102.csv"), "w") as outfile:
        outfile.writelines(["a\tb\tc\n"])

    datasource = data_context.add_datasource("default", "pandas", base_directory=str(tmpdir))
    generator = datasource.add_generator("defaut", "filesystem")

    known_data_asset_names = set(generator.list_data_asset_names())

    assert known_data_asset_names == set([
        "f1", "f2", "f3"
    ])

    f1_batches = [batch_kwargs for batch_kwargs in generator.yield_batch_kwargs("f1")]
    assert f1_batches[0] == {
            "path": os.path.join(tmpdir, "f1.csv")
        }
    assert len(f1_batches) == 1

    f3_batches = [batch_kwargs for batch_kwargs in generator.yield_batch_kwargs("f3")]
    expected_batches = [
        {
            "path": os.path.join(tmpdir, "f3", "f3_20190101.csv")
        },
        {
            "path": os.path.join(tmpdir, "f3", "f3_20190102.csv")
        }
    ]
    for batch in expected_batches:
        assert batch in f3_batches
    assert len(f3_batches) == 2
