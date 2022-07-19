"""This file is meant for integration tests related to datasource CRUD."""


import pytest

# TODO: Fixtures
# empty_cloud_data_context for a BaseDataContext in cloud mode
# ?? new fixture for a CloudDataContext data context
# ?? for DataContext in cloud mode


@pytest.mark.integration
def test_cloud_mode_data_context_add_datasource_save_changes_true():
    """A BaseDataContext in cloud mode should save to the Datasource store when calling add_datasource
    with save_changes=True."""
    # 1. setup context
    # 2. call add_datasource with mocked `requests.set`
    # 3. assert the right call was made to the store backend
    raise NotImplementedError


@pytest.mark.integration
def test_cloud_mode_data_context_add_datasource_save_changes_false():
    """A BaseDataContext in cloud mode should not save to the Datasource store when calling add_datasource
    with save_changes=False."""

    # Assert no call was made to the store backend
    raise NotImplementedError


@pytest.mark.integration
def test_cloud_mode_base_data_context_add_datasource_save_changes_true():
    """A BaseDataContext in cloud mode should save to the Datasource store when calling add_datasource
    with save_changes=True."""
    raise NotImplementedError


@pytest.mark.integration
def test_cloud_mode_base_data_context_add_datasource_save_changes_false():
    """A BaseDataContext in cloud mode should not save to the Datasource store when calling add_datasource
    with save_changes=False."""
    raise NotImplementedError


@pytest.mark.integration
def test_cloud_data_context_add_datasources_save_changes_true():
    raise NotImplementedError


@pytest.mark.integration
def test_cloud_data_context_add_datasources_save_changes_false():
    raise NotImplementedError
