import pytest
import os

from great_expectations.data_context.util import (
    safe_mmkdir,
)

def test_safe_mmkdir(tmp_path_factory):
    project_path = str(tmp_path_factory.mktemp('empty_dir'))

    first_path = os.path.join(project_path,"first_path")

    safe_mmkdir(first_path)
    assert os.path.isdir(first_path)

    with pytest.raises(TypeError):
        safe_mmkdir(1)

    #This should trigger python 2
    second_path = os.path.join(project_path,"second_path")
    print(second_path)
    print(type(second_path))
    safe_mmkdir(os.path.dirname(second_path))
