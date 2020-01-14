# -*- coding: utf-8 -*-
import os

from click.testing import CliRunner

from great_expectations import DataContext
from great_expectations.cli import cli


def test_suite_help_output():
    runner = CliRunner()
    result = runner.invoke(cli, ["suite"])
    assert result.exit_code == 0
    assert (
        """\
Commands:
  edit  Edit an existing suite with a jupyter notebook.
  new   Create a new expectation suite."""
        in result.stdout
    )


def test_suite_new(site_builder_data_context_with_html_store_titanic_random):
    root_dir = site_builder_data_context_with_html_store_titanic_random.root_directory
    os.chdir(root_dir)
    context = DataContext(root_dir)
    runner = CliRunner()
    result = runner.invoke(
        cli, ["suite", "new", "-d", root_dir], input="2\n1\nmy_new_suite\n\n"
    )
    stdout = result.stdout

    assert result.exit_code == 0
    assert "Select data source" in stdout
    assert "Which data would you like to use" in stdout
    assert "Name the new expectation suite [warning]" in stdout
    assert (
        "Great Expectations will choose a couple of columns and generate expectations"
        in stdout
    )
    assert "Profiling" in stdout
    assert "Building" in stdout
    assert "The following Data Docs sites were built" in stdout

    obs_urls = context.get_docs_sites_urls()

    assert len(obs_urls) == 1
    assert (
        "great_expectations/uncommitted/data_docs/local_site/index.html" in obs_urls[0]
    )

    expected_index_path = os.path.join(
        root_dir, "uncommitted/data_docs/local_site/index.html"
    )
    assert os.path.isfile(expected_index_path)

    expected_suite_path = os.path.join(
        root_dir, "expectations/random/default/f2/my_new_suite.json"
    )
    assert os.path.isfile(expected_suite_path)
