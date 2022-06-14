import pytest

from tests.test_utils import (
    create_files_in_directory,
)

@pytest.fixture
def test_dir_alpha(tmp_path_factory):

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
