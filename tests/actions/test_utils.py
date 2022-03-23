import pytest

from great_expectations.checkpoint.util import validate_run

def test_validate_run():
    @validate_run(int, z=int)
    def spam(x, y, z=42):
        print(x, y, z)
    with pytest.raises(TypeError):
        spam(1, 'hello', 'world')

