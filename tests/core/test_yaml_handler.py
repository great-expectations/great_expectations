import os.path
from pathlib import Path
from typing import Optional

import pytest

from great_expectations.core.yaml_handler import YamlHandler


@pytest.fixture
def simple_yaml() -> str:
    simple_yaml: str = f"""
    name: test
    class_name: test_class
    module_name: test.test_class
    """
    return simple_yaml


@pytest.fixture
def simple_dict() -> dict:
    simple_dict: dict = {
        "name": "test",
        "class_name": "test_class",
        "module_name": "test.test_class",
    }
    return simple_dict


def test_load_correct(simple_yaml: str, simple_dict: dict) -> None:
    res: dict = YamlHandler.load(simple_yaml)

    assert res == simple_dict


def test_load_incorrect_input() -> None:
    with pytest.raises(TypeError):
        YamlHandler.load(12345)


def test_file_output(tmp_path: Path) -> None:
    simplest_yaml: str = "abc: 1"
    test_file: str = os.path.join(tmp_path, "out.yaml")
    out: Path = Path(test_file)

    data: dict = YamlHandler.load(simplest_yaml)
    YamlHandler.dump(data, out)

    # check the output
    with open(test_file) as f:
        line = f.readline().strip()
        data_from_file: dict = YamlHandler.load(line)

    assert data_from_file == data


def test_dump_correct_from_dict_default_stream() -> None:
    # when we specify no stream, then StringIO
    simplest_dict: dict = dict(abc=1)
    dumped: Optional[str] = YamlHandler.dump(simplest_dict)

    assert dumped == "{abc: 1}\n"
