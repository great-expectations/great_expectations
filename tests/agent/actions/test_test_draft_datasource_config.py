import os
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from great_expectations.agent.actions.test_draft_datasource_config import (
    TestDraftDatasourceConfigAction,
)
from great_expectations.agent.agent import GxAgentEnvVars
from great_expectations.agent.models import TestDatasourceConfig
from great_expectations.data_context import CloudDataContext
from great_expectations.datasource.fluent import SQLDatasource
from great_expectations.datasource.fluent.interfaces import TestConnectionError


@pytest.fixture(scope="function")
def context():
    return MagicMock(autospec=CloudDataContext)


@pytest.fixture
def org_id():
    return "81f4e105-e37d-4168-85a0-2526943f9956"


@pytest.fixture
def token():
    return "MTg0NDkyYmYtNTBiOS00ZDc1LTk3MmMtYjQ0M2NhZDA2NjJk"


@pytest.fixture(autouse=True)
def set_required_env_vars(monkeypatch, org_id, token):
    env_vars = {
        "GX_CLOUD_ORGANIZATION_ID": org_id,
        "GX_CLOUD_ACCESS_TOKEN": token,
    }
    monkeypatch.setattr(os, "environ", env_vars)


pytestmark = pytest.mark.cloud


def test_test_draft_datasource_config_success(context, mocker):
    datasource_config = {"type": "pandas", "name": "test-1-2-3"}
    create_session = mocker.patch("great_expectations.agent.actions.test_draft_datasource_config.create_session")
    session = create_session.return_value
    response = session.get.return_value
    response.ok = True
    response.json.return_value = datasource_config
    env_vars = GxAgentEnvVars()
    action = TestDraftDatasourceConfigAction(context=context)
    job_id = UUID("87657a8e-f65e-4e64-b21f-e83a54738b75")
    config_id = UUID("df02b47c-e1b8-48a8-9aaa-b6ed9c49ffa5")
    event = TestDatasourceConfig(config_id=config_id)
    expected_url = f"{env_vars.gx_cloud_base_url}/organizations/{env_vars.gx_cloud_organization_id}/draft-configs/{config_id}"

    action_result = action.run(event=event, id=str(job_id))

    assert action_result.id == str(job_id)
    assert action_result.type == event.type
    assert action_result.created_resources == []

    session.get.assert_called_with(expected_url)


def test_test_draft_datasource_config_failure(context, mocker):
    ds_type = "sql"
    datasource_config = {"type": ds_type, "name": "test-1-2-3"}
    create_session = mocker.patch("great_expectations.agent.actions.test_draft_datasource_config.create_session")
    session = create_session.return_value
    response = session.get.return_value
    response.ok = True
    response.json.return_value = datasource_config
    env_vars = GxAgentEnvVars()
    action = TestDraftDatasourceConfigAction(context=context)
    job_id = UUID("87657a8e-f65e-4e64-b21f-e83a54738b75")
    config_id = UUID("df02b47c-e1b8-48a8-9aaa-b6ed9c49ffa5")
    event = TestDatasourceConfig(config_id=config_id)
    expected_url = f"{env_vars.gx_cloud_base_url}/organizations/{env_vars.gx_cloud_organization_id}/draft-configs/{config_id}"
    datasource_cls = MagicMock(autospec=SQLDatasource)
    context.sources.type_lookup = {ds_type: datasource_cls}
    datasource_cls.return_value.test_connection.side_effect = TestConnectionError

    with pytest.raises(TestConnectionError):
        action.run(event=event, id=str(job_id))

    session.get.assert_called_with(expected_url)


def test_test_draft_datasource_config_raises_for_non_fds(context, mocker):
    datasource_config = {"name": "test-1-2-3", "connection_string": ""}
    create_session = mocker.patch("great_expectations.agent.actions.test_draft_datasource_config.create_session")
    session = create_session.return_value
    response = session.get.return_value
    response.ok = True
    response.json.return_value = datasource_config
    env_vars = GxAgentEnvVars()
    action = TestDraftDatasourceConfigAction(context=context)
    job_id = UUID("87657a8e-f65e-4e64-b21f-e83a54738b75")
    config_id = UUID("df02b47c-e1b8-48a8-9aaa-b6ed9c49ffa5")
    event = TestDatasourceConfig(config_id=config_id)
    expected_url = f"{env_vars.gx_cloud_base_url}/organizations/{env_vars.gx_cloud_organization_id}/draft-configs/{config_id}"

    with pytest.raises(ValueError, match="fluent-style datasource"):
        action.run(event=event, id=str(job_id))

    session.get.assert_called_with(expected_url)


def test_test_draft_datasource_config_raises_for_unknown_type(context, mocker):
    datasource_config = {"type": "not a datasource", "name": "test-1-2-3"}
    create_session = mocker.patch("great_expectations.agent.actions.test_draft_datasource_config.create_session")
    session = create_session.return_value
    response = session.get.return_value
    response.ok = True
    response.json.return_value = datasource_config  # todo
    env_vars = GxAgentEnvVars()
    action = TestDraftDatasourceConfigAction(context=context)
    job_id = UUID("87657a8e-f65e-4e64-b21f-e83a54738b75")
    config_id = UUID("df02b47c-e1b8-48a8-9aaa-b6ed9c49ffa5")
    event = TestDatasourceConfig(config_id=config_id)
    expected_url = f"{env_vars.gx_cloud_base_url}/organizations/{env_vars.gx_cloud_organization_id}/draft-configs/{config_id}"
    context.sources.type_lookup = dict()

    with pytest.raises(ValueError, match="unknown datasource type"):
        action.run(event=event, id=str(job_id))

    session.get.assert_called_with(expected_url)


def test_test_draft_datasource_config_raises_for_cloud_backend_error(context, mocker):
    datasource_config = {"type": "not a datasource", "name": "test-1-2-3"}
    create_session = mocker.patch("great_expectations.agent.actions.test_draft_datasource_config.create_session")
    session = create_session.return_value
    response = session.get.return_value
    response.ok = False
    response.json.return_value = datasource_config  # todo
    env_vars = GxAgentEnvVars()
    action = TestDraftDatasourceConfigAction(context=context)
    job_id = UUID("87657a8e-f65e-4e64-b21f-e83a54738b75")
    config_id = UUID("df02b47c-e1b8-48a8-9aaa-b6ed9c49ffa5")
    event = TestDatasourceConfig(config_id=config_id)
    expected_url = f"{env_vars.gx_cloud_base_url}/organizations/{env_vars.gx_cloud_organization_id}/draft-configs/{config_id}"

    with pytest.raises(RuntimeError, match="error while connecting to GX-Cloud"):
        action.run(event=event, id=str(job_id))

    session.get.assert_called_with(expected_url)
