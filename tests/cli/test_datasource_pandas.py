import os

from click.testing import CliRunner

from great_expectations import DataContext
from great_expectations.cli import cli
from tests.cli.test_cli import yaml
from tests.cli.utils import (
    assert_dict_key_and_val_in_stdout,
    assert_no_logging_messages_or_tracebacks,
    assert_no_tracebacks,
)


def test_cli_datasource_list(caplog, empty_data_context, filesystem_csv_2):
    """Test an empty project and after adding a single datasource."""
    project_root_dir = empty_data_context.root_directory
    context = DataContext(project_root_dir)

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli, ["datasource", "list", "-d", project_root_dir], catch_exceptions=False
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
    result = runner.invoke(
        cli, ["datasource", "list", "-d", project_root_dir], catch_exceptions=False
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


def test_cli_datasorce_new(caplog, empty_data_context, filesystem_csv_2):
    project_root_dir = empty_data_context.root_directory
    context = DataContext(project_root_dir)
    assert context.list_datasources() == []

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["datasource", "new", "-d", project_root_dir],
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


def test_cli_datasource_profile_answering_no(
    caplog, empty_data_context, filesystem_csv_2
):
    empty_data_context.add_datasource(
        "my_datasource",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
        batch_kwargs_generators={
            "subdir_reader": {
                "class_name": "SubdirReaderBatchKwargsGenerator",
                "base_directory": str(filesystem_csv_2),
            }
        },
    )

    not_so_empty_data_context = empty_data_context
    project_root_dir = not_so_empty_data_context.root_directory

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        [
            "datasource",
            "profile",
            "my_datasource",
            "-d",
            project_root_dir,
            "--no-view",
        ],
        input="n\n",
        catch_exceptions=False,
    )

    stdout = result.stdout
    print(stdout)
    assert result.exit_code == 0
    assert "Profiling 'my_datasource'" in stdout
    assert "Skipping profiling for now." in stdout
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_cli_datasource_profile_with_datasource_arg(
    caplog, empty_data_context, filesystem_csv_2
):
    empty_data_context.add_datasource(
        "my_datasource",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
        batch_kwargs_generators={
            "subdir_reader": {
                "class_name": "SubdirReaderBatchKwargsGenerator",
                "base_directory": str(filesystem_csv_2),
            }
        },
    )

    not_so_empty_data_context = empty_data_context
    project_root_dir = not_so_empty_data_context.root_directory

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        [
            "datasource",
            "profile",
            "my_datasource",
            "-d",
            project_root_dir,
            "--no-view",
        ],
        input="Y\n",
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    stdout = result.stdout
    assert "Profiling 'my_datasource'" in stdout
    assert result.exit_code == 0

    context = DataContext(project_root_dir)
    assert len(context.list_datasources()) == 1

    expectations_store = context.stores["expectations_store"]
    suites = expectations_store.list_keys()
    assert len(suites) == 1
    assert (
        suites[0].expectation_suite_name
        == "my_datasource.subdir_reader.f1.BasicDatasetProfiler"
    )

    validations_store = context.stores["validations_store"]
    validation_keys = validations_store.list_keys()
    assert len(validation_keys) == 1

    validation = validations_store.get(validation_keys[0])
    assert (
        validation.meta["expectation_suite_name"]
        == "my_datasource.subdir_reader.f1.BasicDatasetProfiler"
    )
    assert validation.success is False
    assert len(validation.results) == 8

    assert "Preparing column 1 of 1" in caplog.messages[0]
    assert_no_tracebacks(result)


def test_cli_datasource_profile_with_no_datasource_args(
    caplog, empty_data_context, filesystem_csv_2
):
    empty_data_context.add_datasource(
        "my_datasource",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
        batch_kwargs_generators={
            "subdir_reader": {
                "class_name": "SubdirReaderBatchKwargsGenerator",
                "base_directory": str(filesystem_csv_2),
            }
        },
    )

    not_so_empty_data_context = empty_data_context

    project_root_dir = not_so_empty_data_context.root_directory

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["datasource", "profile", "-d", project_root_dir, "--no-view"],
        input="Y\n",
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    stdout = result.stdout

    assert (
        "Profiling 'my_datasource' will create expectations and documentation."
        in stdout
    )
    assert "Would you like to profile 'my_datasource'" in stdout
    assert (
        "Great Expectations is building Data Docs from the data you just profiled!"
        in stdout
    )

    context = DataContext(project_root_dir)
    assert len(context.list_datasources()) == 1

    expectations_store = context.stores["expectations_store"]
    suites = expectations_store.list_keys()
    assert len(suites) == 1
    assert (
        suites[0].expectation_suite_name
        == "my_datasource.subdir_reader.f1.BasicDatasetProfiler"
    )

    validations_store = context.stores["validations_store"]
    validation_keys = validations_store.list_keys()
    assert len(validation_keys) == 1

    validation = validations_store.get(validation_keys[0])
    assert (
        validation.meta["expectation_suite_name"]
        == "my_datasource.subdir_reader.f1.BasicDatasetProfiler"
    )
    assert validation.success is False
    assert len(validation.results) == 8

    assert "Preparing column 1 of 1" in caplog.messages[0]
    assert len(caplog.messages) == 1
    assert_no_tracebacks(result)

def test_cli_datasource_profile_with_skip_prompt_flag(
    caplog, empty_data_context, filesystem_csv_2
):
    empty_data_context.add_datasource(
        "my_datasource",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
        batch_kwargs_generators={
            "subdir_reader": {
                "class_name": "SubdirReaderBatchKwargsGenerator",
                "base_directory": str(filesystem_csv_2),
            }
        },
    )

    not_so_empty_data_context = empty_data_context

    project_root_dir = not_so_empty_data_context.root_directory

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["datasource", "profile", "-d", project_root_dir, "--no-view",'-y'],
        input="Y\n",
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    stdout = result.stdout

    assert (
        "Profiling 'my_datasource' will create expectations and documentation."
        in stdout
    )
    assert "Would you like to profile 'my_datasource'" not in stdout
    assert (
        "Great Expectations is building Data Docs from the data you just profiled!"
        in stdout
    )

    context = DataContext(project_root_dir)
    assert len(context.list_datasources()) == 1

    expectations_store = context.stores["expectations_store"]
    suites = expectations_store.list_keys()
    assert len(suites) == 1
    assert (
        suites[0].expectation_suite_name
        == "my_datasource.subdir_reader.f1.BasicDatasetProfiler"
    )

    validations_store = context.stores["validations_store"]
    validation_keys = validations_store.list_keys()
    assert len(validation_keys) == 1

    validation = validations_store.get(validation_keys[0])
    assert (
        validation.meta["expectation_suite_name"]
        == "my_datasource.subdir_reader.f1.BasicDatasetProfiler"
    )
    assert validation.success is False
    assert len(validation.results) == 8

    assert "Preparing column 1 of 1" in caplog.messages[0]
    assert len(caplog.messages) == 1
    assert_no_tracebacks(result)



def test_cli_datasource_profile_with_additional_batch_kwargs(
    caplog, empty_data_context, filesystem_csv_2
):
    empty_data_context.add_datasource(
        "my_datasource",
        class_name="PandasDatasource",
        batch_kwargs_generators={
            "subdir_reader": {
                "class_name": "SubdirReaderBatchKwargsGenerator",
                "base_directory": str(filesystem_csv_2),
            }
        },
    )

    not_so_empty_data_context = empty_data_context

    project_root_dir = not_so_empty_data_context.root_directory

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        [
            "datasource",
            "profile",
            "-d",
            project_root_dir,
            "--additional-batch-kwargs",
            '{"reader_options": {"sep": ",", "parse_dates": [0]}}',
            "--no-view",
        ],
        input="Y\n",
        catch_exceptions=False,
    )
    stdout = result.output
    assert result.exit_code == 0

    assert (
        "Profiling 'my_datasource' will create expectations and documentation."
        in stdout
    )
    assert "Would you like to profile 'my_datasource'" in stdout
    assert (
        "Great Expectations is building Data Docs from the data you just profiled!"
        in stdout
    )

    context = DataContext(project_root_dir)
    assert len(context.list_datasources()) == 1

    expectations_store = context.stores["expectations_store"]
    suites = expectations_store.list_keys()
    assert len(suites) == 1
    expected_suite_name = "my_datasource.subdir_reader.f1.BasicDatasetProfiler"
    assert suites[0].expectation_suite_name == expected_suite_name

    validations_store = context.stores["validations_store"]
    validation_keys = validations_store.list_keys()
    assert len(validation_keys) == 1

    validation = validations_store.get(validation_keys[0])
    assert validation.meta["expectation_suite_name"] == expected_suite_name
    assert validation.success is False
    assert len(validation.results) == 9

    batch_id = validation_keys[0].batch_identifier
    evr = context.get_validation_result(
        expectation_suite_name=expected_suite_name, batch_identifier=batch_id
    )
    reader_options = evr.meta["batch_kwargs"]["reader_options"]
    assert reader_options["parse_dates"] == [0]
    assert reader_options["sep"] == ","

    assert "Preparing column 1 of 1" in caplog.messages[0]
    assert len(caplog.messages) == 1
    assert_no_tracebacks(result)


def test_cli_datasource_profile_with_valid_data_asset_arg(
    caplog, empty_data_context, filesystem_csv_2
):
    empty_data_context.add_datasource(
        "my_datasource",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
        batch_kwargs_generators={
            "subdir_reader": {
                "class_name": "SubdirReaderBatchKwargsGenerator",
                "base_directory": str(filesystem_csv_2),
            }
        },
    )
    context = empty_data_context

    project_root_dir = context.root_directory

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        [
            "datasource",
            "profile",
            "my_datasource",
            "--data-assets",
            "f1",
            "-d",
            project_root_dir,
            "--no-view",
        ],
        input="\n",
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    stdout = result.stdout
    assert "Profiling 'my_datasource'" in stdout
    assert "The following Data Docs sites will be built:\n" in stdout
    assert "local_site:" in stdout

    context = DataContext(project_root_dir)
    assert len(context.list_datasources()) == 1

    expectations_store = context.stores["expectations_store"]
    suites = expectations_store.list_keys()
    assert len(suites) == 1
    assert (
        suites[0].expectation_suite_name
        == "my_datasource.subdir_reader.f1.BasicDatasetProfiler"
    )

    validations_store = context.stores["validations_store"]
    validation_keys = validations_store.list_keys()
    assert len(validation_keys) == 1

    validation = validations_store.get(validation_keys[0])
    suite_name = validation.meta["expectation_suite_name"]
    assert suite_name == "my_datasource.subdir_reader.f1.BasicDatasetProfiler"
    assert validation.success is False
    assert len(validation.results) == 8

    assert "Preparing column 1 of 1" in caplog.messages[0]
    assert len(caplog.messages) == 1
    assert_no_tracebacks(result)


def test_cli_datasource_profile_with_invalid_data_asset_arg_answering_no(
    caplog, empty_data_context, filesystem_csv_2
):
    empty_data_context.add_datasource(
        "my_datasource",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
        batch_kwargs_generators={
            "subdir_reader": {
                "class_name": "SubdirReaderBatchKwargsGenerator",
                "base_directory": str(filesystem_csv_2),
            }
        },
    )

    not_so_empty_data_context = empty_data_context

    project_root_dir = not_so_empty_data_context.root_directory

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        [
            "datasource",
            "profile",
            "my_datasource",
            "--data-assets",
            "bad-bad-asset",
            "-d",
            project_root_dir,
            "--no-view",
        ],
        input="2\n",
        catch_exceptions=False,
    )

    stdout = result.stdout
    assert (
        "Some of the data assets you specified were not found: bad-bad-asset" in stdout
    )
    assert "Choose how to proceed" in stdout
    assert "Skipping profiling for now." in stdout

    context = DataContext(project_root_dir)
    assert len(context.list_datasources()) == 1

    expectations_store = context.stores["expectations_store"]
    suites = expectations_store.list_keys()
    assert len(suites) == 0
    assert_no_logging_messages_or_tracebacks(caplog, result)
