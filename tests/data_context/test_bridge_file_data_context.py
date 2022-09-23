import pytest

from great_expectations.data_context.data_context.base_data_context import (
    BaseDataContext,
)
from great_expectations.data_context.data_context.bridge_file_data_context import (
    _BridgeFileDataContext,
)
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from great_expectations.exceptions import DataContextError

"""
 # NOTE: <DataContextRefactor> 09232022

    The _BridgeFileDataContext was created as part of the DataContextRefactor work:
        - It is most closely related to the FileDataContext, but intends to preserve behavior that already exists in
        BaseDataContext.

    A more detailed docstring can be found at `great_expectations/data_context/data_context/bridge_file_data_context.py`

    These tests can be *deleted* once the DataContextRefactor is complete.
"""


@pytest.mark.unit
def test_bridge_file_data_context_good_instantiation(
    basic_in_memory_data_context_config_just_stores, empty_data_context_directory
):
    my_context: _BridgeFileDataContext = _BridgeFileDataContext(
        project_config=basic_in_memory_data_context_config_just_stores,
        context_root_dir=empty_data_context_directory,
        from_base_data_context=True,
    )
    assert my_context


@pytest.mark.unit
def test_bridge_file_data_context_bad_instantiation(
    basic_in_memory_data_context_config_just_stores, empty_data_context_directory
):
    with pytest.raises(DataContextError):
        _BridgeFileDataContext(
            project_config=basic_in_memory_data_context_config_just_stores,
            context_root_dir="",
        )


@pytest.mark.unit
def test_base_data_context_with_bridge_file_data_context(
    basic_in_memory_data_context_config_just_stores, empty_data_context_directory
):
    my_context: BaseDataContext = BaseDataContext(
        project_config=basic_in_memory_data_context_config_just_stores,
        context_root_dir=empty_data_context_directory,
    )
    assert isinstance(my_context._data_context, _BridgeFileDataContext)


@pytest.mark.unit
def test_base_data_context_with_file_data_context(empty_data_context_directory):
    my_context: BaseDataContext = BaseDataContext(
        context_root_dir=empty_data_context_directory
    )
    assert isinstance(my_context._data_context, FileDataContext)
