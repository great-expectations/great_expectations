from unittest.mock import Mock  # noqa: TID251

import pytest

from great_expectations.data_context import AbstractDataContext
from great_expectations.data_context.data_context.context_factory import ProjectManager
from great_expectations.exceptions.exceptions import DataContextRequiredError


class TestProjectManagerStores:
    @pytest.mark.unit
    def test_get_expectations_store_success(self):
        context = Mock(spec=AbstractDataContext)
        project_manager = ProjectManager()
        project_manager.set_project(project=context)

        store = project_manager.get_expectations_store()

        assert store == context.expectations_store

    @pytest.mark.unit
    def test_get_expectations_store_fails_without_context(self):
        project_manager = ProjectManager()

        with pytest.raises(DataContextRequiredError):
            project_manager.get_expectations_store()

    @pytest.mark.unit
    def test_get_checkpoints_store_success(self):
        context = Mock(spec=AbstractDataContext)
        project_manager = ProjectManager()
        project_manager.set_project(project=context)

        store = project_manager.get_checkpoints_store()

        assert store == context.checkpoint_store

    @pytest.mark.unit
    def test_get_checkpoints_store_fails_without_context(self):
        project_manager = ProjectManager()

        with pytest.raises(DataContextRequiredError):
            project_manager.get_checkpoints_store()

    @pytest.mark.unit
    def test_get_validation_results_store_success(self):
        context = Mock(spec=AbstractDataContext)
        project_manager = ProjectManager()
        project_manager.set_project(project=context)

        store = project_manager.get_validation_results_store()

        assert store == context.validation_results_store

    @pytest.mark.unit
    def test_get_validation_results_store_fails_without_context(self):
        project_manager = ProjectManager()

        with pytest.raises(DataContextRequiredError):
            project_manager.get_validation_results_store()
