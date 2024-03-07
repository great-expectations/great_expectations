import pytest

from great_expectations.checkpoint.v1_checkpoint import Checkpoint


def test_checkpoint_no_validations_raises_error():
    with pytest.raises(ValueError):
        Checkpoint(name="my_checkpoint", validations=[], actions=[])
    pass


"""
1. empty validations, empty actions
2. validations, empty actions
3. empty validations, actions
4. validations, actions

"""


class TestCheckpointSerialization:
    def test_checkpoint_serialization_success(self):
        pass

    def test_checkpoint_serialization_failure(self):
        pass

    def test_checkpoint_deserialization_success(self):
        pass

    def test_checkpoint_deserialization_failure(self):
        pass
