from unittest.mock import Mock  # noqa: TID251

import pytest

from great_expectations.data_context import AbstractDataContext
from great_expectations.data_context.data_context.context_factory import ProjectManager


class TestProjectManagerStores:
    missing_project_error_str = "This action requires an active DataContext."

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

        with pytest.raises(RuntimeError, match=self.missing_project_error_str):
            project_manager.get_expectations_store()

    @pytest.mark.unit
    def test_get_checkpoints_store_success(self):
        context = Mock(spec=AbstractDataContext)
        project_manager = ProjectManager()
        project_manager.set_project(project=context)

        store = project_manager.get_checkpoints_store()

        assert store == context.v1_checkpoint_store

    @pytest.mark.unit
    def test_get_checkpoints_store_fails_without_context(self):
        project_manager = ProjectManager()

        with pytest.raises(RuntimeError, match=self.missing_project_error_str):
            project_manager.get_checkpoints_store()

    @pytest.mark.unit
    def test_get_validations_store_success(self):
        context = Mock(spec=AbstractDataContext)
        project_manager = ProjectManager()
        project_manager.set_project(project=context)

        store = project_manager.get_validations_store()

        assert store == context.validations_store

    @pytest.mark.unit
    def test_get_validations_store_fails_without_context(self):
        project_manager = ProjectManager()

        with pytest.raises(RuntimeError, match=self.missing_project_error_str):
            project_manager.get_validations_store()

    @pytest.mark.unit
    def test_get_suite_parameters_store_success(self):
        context = Mock(spec=AbstractDataContext)
        project_manager = ProjectManager()
        project_manager.set_project(project=context)

        store = project_manager.get_suite_parameters_store()

        assert store == context.suite_parameter_store

    @pytest.mark.unit
    def test_get_suite_parameters_store_fails_without_context(self):
        project_manager = ProjectManager()

        with pytest.raises(RuntimeError, match=self.missing_project_error_str):
            project_manager.get_suite_parameters_store()
