import os
import shutil

import pytest

from great_expectations.data_context.util import file_relative_path


@pytest.fixture
def empty_context_with_checkpoint_v1_stats_enabled(
    empty_data_context_stats_enabled, monkeypatch
):
    try:
        monkeypatch.delenv("VAR")
        monkeypatch.delenv("MY_PARAM")
        monkeypatch.delenv("OLD_PARAM")
    except:
        pass

    monkeypatch.setenv("VAR", "test")
    monkeypatch.setenv("MY_PARAM", "1")
    monkeypatch.setenv("OLD_PARAM", "2")

    context = empty_data_context_stats_enabled
    root_dir = context.root_directory
    fixture_name = "my_v1_checkpoint.yml"
    fixture_path = file_relative_path(
        __file__, f"../data_context/fixtures/contexts/{fixture_name}"
    )
    checkpoints_file = os.path.join(root_dir, "checkpoints", fixture_name)
    shutil.copy(fixture_path, checkpoints_file)
    # # noinspection PyProtectedMember
    context._save_project_config()
    return context
