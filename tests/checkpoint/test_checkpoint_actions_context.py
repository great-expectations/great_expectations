"""An action runs against the Data Context that owns its Checkpoint (#12209).

A Checkpoint hands its Data Context to each of its actions before running them, so Data Docs are
built in, and Data Docs URLs are read from, the Checkpoint's own context, and a configuration
variable in an action's settings is resolved through that context's configuration, whichever
context is current. An action run directly, outside any Checkpoint, belongs to no context and
resolves through the current one.
"""

from __future__ import annotations

import pathlib
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

import pandas as pd
import pytest

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.checkpoint.actions import (
    EmailAction,
    MicrosoftTeamsNotificationAction,
    SlackNotificationAction,
    UpdateDataDocsAction,
    ValidationAction,
)
from great_expectations.checkpoint.checkpoint import Checkpoint
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.core.validation_definition import ValidationDefinition
from great_expectations.data_context import AbstractDataContext
from great_expectations.data_context.data_context.context_factory import project_manager
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.datasource.fluent.config_str import ConfigStr
from great_expectations.exceptions import DataContextRequiredError

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

C1_VALUE = "c1"
C2_VALUE = "c2"


@pytest.fixture
def restore_current_context(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Put the process-wide current Data Context back as the test found it."""
    monkeypatch.setattr(
        project_manager,
        "_ProjectManager__project",
        getattr(project_manager, "_ProjectManager__project"),  # noqa: B009
    )
    yield


@pytest.fixture
def no_current_context(monkeypatch: pytest.MonkeyPatch) -> None:
    """Empty the process-wide current Data Context slot."""
    monkeypatch.setattr(project_manager, "_ProjectManager__project", None)


def _root(context: AbstractDataContext) -> pathlib.Path:
    assert context.root_directory is not None
    return pathlib.Path(context.root_directory)


def _file_context(
    root: pathlib.Path, config_variables: dict[str, str] | None = None
) -> AbstractDataContext:
    """Create a file-backed context, optionally with config variables, and make it current."""
    context = gx.get_context(mode="file", project_root_dir=root)
    if config_variables:
        variables_file = _root(context) / "uncommitted" / "config_variables.yml"
        with variables_file.open("a") as f:
            f.writelines(f"{name}: {value}\n" for name, value in config_variables.items())
        # The variables are read when a context is opened, so open it again.
        context = gx.get_context(mode="file", project_root_dir=root)
    return context


def _add_checkpoint(context: AbstractDataContext, actions: list[ValidationAction]) -> Checkpoint:
    """Add a data source, suite and validation definition to `context`, then a Checkpoint."""
    datasource = context.data_sources.add_pandas("datasource")
    asset = datasource.add_dataframe_asset("asset")
    batch_definition = asset.add_batch_definition_whole_dataframe("batch_definition")
    suite = context.suites.add(
        ExpectationSuite(name="suite", expectations=[gxe.ExpectColumnValuesToNotBeNull(column="a")])
    )
    validation_definition = context.validation_definitions.add(
        ValidationDefinition(name="validation_definition", data=batch_definition, suite=suite)
    )
    return context.checkpoints.add(
        Checkpoint(
            name="checkpoint", validation_definitions=[validation_definition], actions=actions
        )
    )


def _index_html(context: AbstractDataContext) -> pathlib.Path:
    return _root(context) / "uncommitted" / "data_docs" / "local_site" / "index.html"


def _run(checkpoint: Checkpoint) -> None:
    result = checkpoint.run(batch_parameters={"dataframe": pd.DataFrame({"a": [1, 2, 3]})})
    assert result.success


@pytest.mark.filesystem
def test_data_docs_are_built_in_the_checkpoints_own_context(
    tmp_path: pathlib.Path, restore_current_context: None
) -> None:
    c1 = _file_context(tmp_path / "one")
    checkpoint = _add_checkpoint(c1, [UpdateDataDocsAction(name="update_data_docs")])
    c2 = _file_context(tmp_path / "two")
    assert project_manager.get_current_project() is c2
    assert not _index_html(c1).exists()
    assert not _index_html(c2).exists()

    _run(checkpoint)

    assert _index_html(c1).exists()
    assert not _index_html(c2).exists()
    assert project_manager.get_current_project() is c2


@pytest.mark.filesystem
def test_data_docs_urls_are_read_from_the_checkpoints_own_context(
    tmp_path: pathlib.Path, restore_current_context: None, mocker: MockerFixture
) -> None:
    c1 = _file_context(tmp_path / "one")
    checkpoint = _add_checkpoint(c1, [UpdateDataDocsAction(name="update_data_docs")])
    c2 = _file_context(tmp_path / "two")
    assert project_manager.get_current_project() is c2
    c1_urls = mocker.spy(c1, "get_docs_sites_urls")
    c2_urls = mocker.spy(c2, "get_docs_sites_urls")

    _run(checkpoint)

    c1_urls.assert_called_once()
    c2_urls.assert_not_called()


@pytest.mark.filesystem
def test_a_config_variable_in_an_action_resolves_through_the_checkpoints_own_context(
    tmp_path: pathlib.Path, restore_current_context: None, mocker: MockerFixture
) -> None:
    c1 = _file_context(tmp_path / "one", {"teams_url": f"http://{C1_VALUE}.example/hook"})
    checkpoint = _add_checkpoint(
        c1, [MicrosoftTeamsNotificationAction(name="teams", teams_webhook="${teams_url}")]
    )
    c2 = _file_context(tmp_path / "two", {"teams_url": f"http://{C2_VALUE}.example/hook"})
    assert project_manager.get_current_project() is c2
    assert (
        c2.config_provider.get_values()["teams_url"] != c1.config_provider.get_values()["teams_url"]
    )
    post = mocker.patch("requests.Session.post")

    _run(checkpoint)

    post.assert_called_once()
    assert post.call_args.kwargs["url"] == f"http://{C1_VALUE}.example/hook"


@pytest.mark.filesystem
def test_every_slack_setting_resolves_through_the_checkpoints_own_context(
    tmp_path: pathlib.Path, restore_current_context: None, mocker: MockerFixture
) -> None:
    c1 = _file_context(tmp_path / "one", {"token": C1_VALUE, "channel": f"channel-{C1_VALUE}"})
    checkpoint = _add_checkpoint(
        c1,
        [SlackNotificationAction(name="slack", slack_token="${token}", slack_channel="${channel}")],
    )
    _file_context(tmp_path / "two", {"token": C2_VALUE, "channel": f"channel-{C2_VALUE}"})
    post = mocker.patch("requests.Session.post")

    _run(checkpoint)

    post.assert_called_once()
    assert post.call_args.kwargs["headers"] == {"Authorization": f"Bearer {C1_VALUE}"}
    assert post.call_args.kwargs["json"]["channel"] == f"channel-{C1_VALUE}"


@pytest.mark.filesystem
def test_a_slack_webhook_resolves_through_the_checkpoints_own_context(
    tmp_path: pathlib.Path, restore_current_context: None, mocker: MockerFixture
) -> None:
    c1 = _file_context(tmp_path / "one", {"hook": f"http://{C1_VALUE}.example/slack"})
    checkpoint = _add_checkpoint(
        c1, [SlackNotificationAction(name="slack", slack_webhook="${hook}")]
    )
    _file_context(tmp_path / "two", {"hook": f"http://{C2_VALUE}.example/slack"})
    post = mocker.patch("requests.Session.post")

    _run(checkpoint)

    post.assert_called_once()
    assert post.call_args.kwargs["url"] == f"http://{C1_VALUE}.example/slack"


@pytest.mark.filesystem
def test_every_email_setting_resolves_through_the_checkpoints_own_context(
    tmp_path: pathlib.Path, restore_current_context: None, mocker: MockerFixture
) -> None:
    def variables(value: str) -> dict[str, str]:
        return {
            "smtp_host": f"smtp.{value}.example",
            "smtp_port": "2525" if value == C1_VALUE else "2626",
            "login": f"login-{value}",
            "password": f"password-{value}",
            "alias": f"alias-{value}@example.com",
            "receivers": f"{value}@example.com",
        }

    c1 = _file_context(tmp_path / "one", variables(C1_VALUE))
    checkpoint = _add_checkpoint(
        c1,
        [
            EmailAction(
                name="email",
                smtp_address="${smtp_host}",
                smtp_port="${smtp_port}",
                sender_login="${login}",
                sender_password="${password}",
                sender_alias="${alias}",
                receiver_emails="${receivers}",
            )
        ],
    )
    _file_context(tmp_path / "two", variables(C2_VALUE))
    smtp = mocker.patch("smtplib.SMTP")

    _run(checkpoint)

    smtp.assert_called_once_with("smtp.c1.example", 2525)
    server = smtp.return_value
    server.login.assert_called_once_with("login-c1", "password-c1")
    server.sendmail.assert_called_once()
    sender, receivers, _message = server.sendmail.call_args.args
    assert sender == "alias-c1@example.com"
    assert receivers == ["c1@example.com"]


@pytest.mark.filesystem
def test_a_stamped_action_keeps_its_checkpoints_context_while_another_is_current(
    tmp_path: pathlib.Path, restore_current_context: None
) -> None:
    c1 = _file_context(tmp_path / "one")
    action = UpdateDataDocsAction(name="update_data_docs")
    checkpoint = _add_checkpoint(c1, [action])
    _file_context(tmp_path / "two")

    _run(checkpoint)

    (stamped,) = checkpoint.actions
    assert stamped._resolve_context().context is c1
    assert stamped._resolve_context().bound is True


@pytest.mark.unit
class TestDirectRun:
    """An action run outside any Checkpoint resolves through the current context."""

    @pytest.fixture
    def checkpoint_result(self, mocker: MockerFixture) -> Any:
        identifier = ValidationResultIdentifier(
            expectation_suite_identifier=ExpectationSuiteIdentifier(name="suite"),
            run_id=RunIdentifier(run_name="run"),
            batch_identifier="batch",
        )
        validation_result = mocker.Mock()
        validation_result.suite_name = "suite"
        return mocker.Mock(run_results={identifier: validation_result})

    def test_a_mock_current_context_receives_the_keywords_it_always_has(
        self, mocker: MockerFixture, checkpoint_result: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        context = mocker.Mock(spec=AbstractDataContext)
        context.get_docs_sites_urls.return_value = []
        monkeypatch.setattr(project_manager, "_ProjectManager__project", context)
        (identifier,) = checkpoint_result.run_results

        UpdateDataDocsAction(name="update_data_docs", site_names=["site"]).run(
            checkpoint_result=checkpoint_result
        )

        context.build_data_docs.assert_called_once_with(
            site_names=["site"],
            resource_identifiers=[identifier, ExpectationSuiteIdentifier(name="suite")],
            dry_run=False,
            build_index=True,
        )
        context.get_docs_sites_urls.assert_called_once_with(
            resource_identifier=identifier,
            site_name=None,
            only_if_exists=True,
            site_names=["site"],
        )

    def test_an_unstamped_action_is_not_bound(
        self, mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        context = mocker.Mock(spec=AbstractDataContext)
        monkeypatch.setattr(project_manager, "_ProjectManager__project", context)

        resolved = UpdateDataDocsAction(name="update_data_docs")._resolve_context()

        assert resolved.context is context
        assert resolved.bound is False

    def test_the_context_is_not_part_of_the_actions_configuration(
        self, mocker: MockerFixture
    ) -> None:
        action = UpdateDataDocsAction(name="update_data_docs")
        before = (action.dict(), action.json(), sorted(UpdateDataDocsAction.__fields__))

        action._data_context = mocker.Mock(spec=AbstractDataContext)

        assert (action.dict(), action.json(), sorted(UpdateDataDocsAction.__fields__)) == before
        assert action == UpdateDataDocsAction(name="update_data_docs")

    @pytest.mark.parametrize(
        "call",
        [
            pytest.param(lambda a: a._build_data_docs(), id="build_data_docs"),
            pytest.param(lambda a: a._get_docs_sites_urls(), id="get_docs_sites_urls"),
            pytest.param(
                lambda a: a._substitute_config_str_if_needed(ConfigStr("${missing}")),
                id="substitution",
            ),
            pytest.param(
                lambda a: a._substitute_config_str_if_needed("plain"),
                id="substitution_of_a_plain_string",
            ),
        ],
    )
    def test_with_no_current_context_the_error_is_the_one_it_always_was(
        self, no_current_context: None, call: Any
    ) -> None:
        with pytest.raises(DataContextRequiredError) as exc_info:
            call(UpdateDataDocsAction(name="update_data_docs"))

        assert str(exc_info.value) == str(DataContextRequiredError())
