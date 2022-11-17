import os.path
import sys
from pathlib import Path
from typing import Any, Optional

import pytest

from great_expectations.core.yaml_handler import YAMLHandler


@pytest.fixture
def simple_yaml() -> str:
    simple_yaml: str = """
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


@pytest.fixture
def yaml_handler() -> YAMLHandler:
    return YAMLHandler()


@pytest.mark.unit
def test_load_correct_input(
    simple_yaml: str, simple_dict: dict, yaml_handler: YAMLHandler
) -> None:
    res: dict = yaml_handler.load(simple_yaml)

    assert res == simple_dict


@pytest.mark.unit
def test_load_incorrect_input(yaml_handler: YAMLHandler) -> None:
    with pytest.raises(TypeError):
        yaml_handler.load(12345)


@pytest.mark.integration
def test_file_output(tmp_path: Path, yaml_handler: YAMLHandler) -> None:
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


@pytest.mark.unit
def test_dump_default_behavior_with_no_stream_specified(
    yaml_handler: YAMLHandler,
) -> None:
    # when we specify no stream, then StringIO is used by default
    simplest_dict: dict = dict(abc=1)
    dumped: Optional[str] = yaml_handler.dump(simplest_dict)
    assert dumped == "abc: 1\n"


@pytest.mark.unit
def test_dump_stdout_specified(capsys, yaml_handler: YAMLHandler) -> None:
    # ruamel documentation recommends that we specify the stream as stdout when we are using YAML to return a string.
    simplest_dict: dict = dict(abc=1)
    yaml_handler.dump(simplest_dict, stream=sys.stdout)
    captured: Any = capsys.readouterr()
    assert captured.out == "abc: 1\n"
