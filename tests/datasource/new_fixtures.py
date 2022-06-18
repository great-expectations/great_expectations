import pytest

from tests.test_utils import create_files_in_directory


@pytest.fixture
def test_dir_alpha(tmp_path_factory):
    # Multiple assets, no concept of batches within assets

    base_directory = str(tmp_path_factory.mktemp("test_alpha"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "A.csv",
            "B.csv",
            "C.csv",
            "D.csv",
        ],
    )

    return base_directory


@pytest.fixture
def test_dir_beta(tmp_path_factory):
    # Multiple assets, with multiple batches. No nesting

    base_directory = str(tmp_path_factory.mktemp("test_bravo"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "A1.csv",
            "A2.csv",
            "A3.csv",
            "B1.csv",
            "B2.csv",
            "B3.csv",
            "C1.csv",
            "C2.csv",
            "C3.csv",
            "D1.csv",
            "D2.csv",
            "D3.csv",
        ],
    )

    return base_directory


@pytest.fixture
def test_dir_charlie(tmp_path_factory):
    # Multiple assets, with multiple batches. Nested by asset name

    base_directory = str(tmp_path_factory.mktemp("test_charlie"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "A/A-1.csv",
            "A/A-2.csv",
            "A/A-3.csv",
            "B/B-1.csv",
            "B/B-2.csv",
            "B/B-3.csv",
            "C/C-1.csv",
            "C/C-2.csv",
            "C/C-3.csv",
            "D/D-1.csv",
            "D/D-2.csv",
            "D/D-3.csv",
        ],
    )

    return base_directory


@pytest.fixture
def test_dir_oscar(tmp_path_factory):
    # Multiple assets, with multiple batches. Nested by asset name

    base_directory = str(tmp_path_factory.mktemp("test_oscar"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "A/data-202101.csv",
            "A/data-202102.csv",
            "A/data-202103.csv",
            "A/data-202104.csv",
            "A/data-202105.csv",
            "A/data-202106.csv",
            "A/data-202107.csv",
            "A/data-202108.csv",
            "A/data-202109.csv",
            "A/data-202110.csv",
            "A/data-202111.csv",
            "A/data-202112.csv",
            "A/data-202201.csv",
            "A/data-202202.csv",
            "A/data-202203.csv",
            "B/data-202201.csv",
            "B/data-202202.csv",
            "B/data-202203.csv",
            "C/data-202201.csv",
            "C/data-202202.csv",
            "C/data-202203.csv",
            "D/data-202201.csv",
            "D/data-202202.csv",
            "D/data-202203.csv",
        ],
    )

    return base_directory
