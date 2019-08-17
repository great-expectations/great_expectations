import pytest
import os
import six

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
    if six.PY2:
        with pytest.raises(TypeError) as e:
            next_project_path = tmp_path_factory.mktemp('test_safe_mmkdir__dir_b')
            safe_mmkdir(next_project_path)

        assert e.value.message == "directory must be of type str, not {'directory_type': \"<class 'pathlib2.PosixPath'>\"}"
