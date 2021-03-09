import os

import pytest
from click.testing import CliRunner

from great_expectations import DataContext
from great_expectations.cli import cli
from tests.cli.test_cli import yaml
from tests.cli.utils import assert_no_logging_messages_or_tracebacks


@pytest.mark.xfail(
    reason="This command is not yet implemented for the modern API",
    run=True,
    strict=True,
)
def test_cli_datasource_list(caplog, monkeypatch, empty_data_context, filesystem_csv_2):
    """Test an empty project and after adding a single datasource."""
    project_root_dir = empty_data_context.root_directory
    context = DataContext(project_root_dir)

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        ["--v3-api", "datasource", "list"],
        catch_exceptions=False,
    )

    stdout = result.output.strip()
    assert "No Datasources found" in stdout
    assert context.list_datasources() == []

    base_directory = str(filesystem_csv_2)

    context.add_datasource(
        "wow_a_datasource",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
        batch_kwargs_generators={
            "subdir_reader": {
                "class_name": "SubdirReaderBatchKwargsGenerator",
                "base_directory": base_directory,
            }
        },
    )

    datasources = context.list_datasources()

    assert datasources == [
        {
            "name": "wow_a_datasource",
            "class_name": "PandasDatasource",
            "data_asset_type": {
                "class_name": "PandasDataset",
                "module_name": "great_expectations.dataset",
            },
            "batch_kwargs_generators": {
                "subdir_reader": {
                    "base_directory": base_directory,
                    "class_name": "SubdirReaderBatchKwargsGenerator",
                }
            },
            "module_name": "great_expectations.datasource",
        }
    ]

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        [
            "--v3-api",
            "datasource",
            "list",
        ],
        catch_exceptions=False,
    )
    expected_output = """
1 Datasource found:[0m
[0m
 - [36mname:[0m wow_a_datasource[0m
   [36mmodule_name:[0m great_expectations.datasource[0m
   [36mclass_name:[0m PandasDatasource[0m
   [36mbatch_kwargs_generators:[0m[0m
     [36msubdir_reader:[0m[0m
       [36mclass_name:[0m SubdirReaderBatchKwargsGenerator[0m
       [36mbase_directory:[0m {}[0m
   [36mdata_asset_type:[0m[0m
     [36mmodule_name:[0m great_expectations.dataset[0m
     [36mclass_name:[0m PandasDataset[0m""".format(
        base_directory
    ).strip()
    stdout = result.output.strip()
    assert stdout == expected_output
    assert_no_logging_messages_or_tracebacks(caplog, result)


@pytest.mark.xfail(
    reason="This command is not yet implemented for the modern API",
    run=True,
    strict=True,
)
def test_cli_datasorce_new(caplog, monkeypatch, empty_data_context, filesystem_csv_2):
    project_root_dir = empty_data_context.root_directory
    context = DataContext(project_root_dir)
    assert context.list_datasources() == []

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        [
            "--v3-api",
            "datasource",
            "new",
        ],
        input="1\n1\n%s\nmynewsource\n" % str(filesystem_csv_2),
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert "What data would you like Great Expectations to connect to?" in stdout
    assert "What are you processing your files with?" in stdout
    assert "Give your new Datasource a short name." in stdout
    assert "A new datasource 'mynewsource' was added to your project." in stdout

    assert result.exit_code == 0

    config_path = os.path.join(project_root_dir, DataContext.GE_YML)
    config = yaml.load(open(config_path))
    datasources = config["datasources"]
    assert "mynewsource" in datasources.keys()
    data_source_class = datasources["mynewsource"]["data_asset_type"]["class_name"]
    assert data_source_class == "PandasDataset"
    assert_no_logging_messages_or_tracebacks(caplog, result)
