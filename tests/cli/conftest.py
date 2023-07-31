import pathlib
import shutil

import pytest
from pytest import MonkeyPatch, TempPathFactory

from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from great_expectations.data_context.util import file_relative_path


@pytest.fixture
def empty_context_with_checkpoint_v1_stats_enabled(
    empty_data_context_stats_enabled: FileDataContext, monkeypatch: MonkeyPatch
):
    monkeypatch.delenv("VAR", raising=False)
    monkeypatch.delenv("MY_PARAM", raising=False)
    monkeypatch.delenv("OLD_PARAM", raising=False)

    monkeypatch.setenv("VAR", "test")
    monkeypatch.setenv("MY_PARAM", "1")
    monkeypatch.setenv("OLD_PARAM", "2")

    context = empty_data_context_stats_enabled
    root_dir = context.root_directory
    fixture_name = "my_v1_checkpoint.yml"
    fixture_path = file_relative_path(
        __file__, f"../data_context/fixtures/contexts/{fixture_name}"
    )
    checkpoints_file = pathlib.Path(root_dir, "checkpoints", fixture_name)
    shutil.copy(fixture_path, checkpoints_file)
    # # noinspection PyProtectedMember
    context._save_project_config()
    return context


@pytest.fixture
def v10_project_directory(tmp_path_factory: TempPathFactory):
    """
    GX 0.10.x project for testing upgrade helper
    """

    project_path = tmp_path_factory.mktemp("v10_project")
    context_root_dir = project_path / FileDataContext.GX_DIR

    shutil.copytree(
        file_relative_path(
            __file__, "../test_fixtures/upgrade_helper/great_expectations_v10_project/"
        ),
        context_root_dir,
    )
    shutil.copy(
        file_relative_path(
            __file__, "../test_fixtures/upgrade_helper/great_expectations_v1_basic.yml"
        ),
        context_root_dir / FileDataContext.GX_YML,
    )
    return context_root_dir


@pytest.fixture(scope="function")
def misc_directory(tmp_path: pathlib.Path):
    misc_dir = tmp_path / "random"
    misc_dir.mkdir()
    assert misc_dir.is_absolute()
    return misc_dir
