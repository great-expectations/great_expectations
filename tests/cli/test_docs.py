# -*- coding: utf-8 -*-
import os

from click.testing import CliRunner

from great_expectations import DataContext
from great_expectations.cli import cli
from tests.cli.utils import assert_no_logging_messages_or_tracebacks


def test_docs_help_output(caplog):
    runner = CliRunner()
    result = runner.invoke(cli, ["docs"])
    assert result.exit_code == 0
    assert "build  Build Data Docs for a project." in result.stdout
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_docs_build(caplog, site_builder_data_context_with_html_store_titanic_random):
    root_dir = site_builder_data_context_with_html_store_titanic_random.root_directory

    runner = CliRunner()
    result = runner.invoke(cli, ["docs", "build", "-d", root_dir, "--no-view"])
    stdout = result.stdout

    assert result.exit_code == 0
    assert "Building" in stdout
    assert "The following Data Docs sites were built" in stdout
    assert "great_expectations/uncommitted/data_docs/local_site/index.html" in stdout

    context = DataContext(root_dir)
    obs_urls = context.get_docs_sites_urls()

    assert len(obs_urls) == 1
    assert (
        "great_expectations/uncommitted/data_docs/local_site/index.html" in obs_urls[0]
    )

    expected_index_path = os.path.join(
        root_dir, "uncommitted/data_docs/local_site/index.html"
    )
    assert os.path.isfile(expected_index_path)
    assert_no_logging_messages_or_tracebacks(caplog, result)
