import os.path
from pathlib import Path

import pytest

from great_expectations.core.yaml_handler import YamlHandler


@pytest.fixture
def simple_yaml():
    simple_yaml: str = f"""
    name: test
    class_name: test_class
    module_name: test.test_class"""
    return simple_yaml


@pytest.fixture
def simple_dict():
    simple_dict: dict = {
        "name": "test",
        "class_name": "test_class",
        "module_name": "test.test_class",
    }
    return simple_dict


def test_load_correct(simple_yaml, simple_dict):
    res: dict = YamlHandler.load(simple_yaml)
    assert res == simple_dict


def test_load_incorrect_input():
    with pytest.raises(TypeError):
        res: dict = YamlHandler.load(12345)


def test_file_output(tmp_path):
    simplest_yaml: str = "abc: 1"
    data: dict = YamlHandler.load(simplest_yaml)
    test_file: str = os.path.join(tmp_path, "out.yaml")
    out: Path = Path(test_file)
    YamlHandler.dump(data, out)

    # check the output
    with open(test_file) as f:
        line = f.readline().strip()
        data_from_file: dict = YamlHandler.load(line)
    assert data_from_file == data


def test_dump_correct_from_dict_default_stream():
    # when we specify no stream, then StringIO
    simplest_dict: dict = dict(abc=1)
    dumped: str = YamlHandler.dump(simplest_dict)
    print(dumped.strip())
