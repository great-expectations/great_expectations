import os.path
import sys
from pathlib import Path
from typing import Any, AnyStr, Optional

import pytest

from great_expectations.core.yaml_handler import YAMLHandler


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


def test_load_correct_input(simple_yaml: str, simple_dict: dict) -> None:
    yaml_handler: YAMLHandler = YAMLHandler()
    res: dict = yaml_handler.load(simple_yaml)

    assert res == simple_dict


def test_load_incorrect_input() -> None:
    yaml_handler: YAMLHandler = YAMLHandler()

    with pytest.raises(TypeError):
        yaml_handler.load(12345)


def test_file_output(tmp_path: Path) -> None:
    yaml_handler: YAMLHandler = YAMLHandler()

    simplest_yaml: str = "abc: 1"
    test_file: str = os.path.join(tmp_path, "out.yaml")
    out: Path = Path(test_file)

    data: dict = yaml_handler.load(simplest_yaml)
    yaml_handler.dump(data, out)

    # check the output
    with open(test_file) as f:
        line = f.readline().strip()
        data_from_file: dict = yaml_handler.load(line)

    assert data_from_file == data


def test_dump_default_behavior_with_no_stream_specified() -> None:
    yaml_handler: YAMLHandler = YAMLHandler()

    # when we specify no stream, then StringIO is used by default
    simplest_dict: dict = dict(abc=1)
    dumped: Optional[str] = yaml_handler.dump(simplest_dict)
    assert dumped == "abc: 1\n"


def test_dump_stdout_specified(capsys) -> None:
    yaml_handler: YAMLHandler = YAMLHandler()

    # ruamel documentation recommends that we specify the stream as stdout when we are using YAML to return a string.
    simplest_dict: dict = dict(abc=1)
    yaml_handler.dump(simplest_dict, stream=sys.stdout)
    captured: Any = capsys.readouterr()
    assert captured.out == "abc: 1\n"
