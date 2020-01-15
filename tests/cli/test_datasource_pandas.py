from click.testing import CliRunner

from great_expectations import DataContext
from great_expectations.cli import cli


def test_cli_datasorce_list(empty_data_context, filesystem_csv_2, capsys):
    """Test an empty project and a project with a single datasource."""
    project_root_dir = empty_data_context.root_directory
    context = DataContext(project_root_dir)

    with capsys.disabled():
        runner = CliRunner()
        result = runner.invoke(cli, ["datasource", "list", "-d", project_root_dir])

        obs = result.output.strip()
        assert "[]" in obs
    assert context.list_datasources() == []

    context.add_datasource(
        "wow_a_datasource",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
        base_directory=str(filesystem_csv_2),
    )
    assert context.list_datasources() == [
        {"name": "wow_a_datasource", "class_name": "PandasDatasource"}
    ]

    with capsys.disabled():
        runner = CliRunner()
        result = runner.invoke(cli, ["datasource", "list", "-d", project_root_dir])

        obs = result.output.strip()
        assert "[{'name': 'wow_a_datasource', 'class_name': 'PandasDatasource'}]" in obs


def test_cli_datasorce_new(empty_data_context, filesystem_csv_2, capsys):
    project_root_dir = empty_data_context.root_directory
    context = DataContext(project_root_dir)
    assert context.list_datasources() == []

    with capsys.disabled():
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["datasource", "new", "-d", project_root_dir],
            input="1\n1\n%s\nmynewsource\n" % str(filesystem_csv_2),
        )
        stdout = result.stdout

        assert "What data would you like Great Expectations to connect to?" in stdout
        assert "What are you processing your files with?" in stdout
        assert "Give your new data source a short name." in stdout
        assert "A new datasource 'mynewsource' was added to your project." in stdout

        assert result.exit_code == 0


def test_cli_datasource_profile_answering_no(
    empty_data_context, filesystem_csv_2, capsys
):
    empty_data_context.add_datasource(
        "my_datasource",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
        base_directory=str(filesystem_csv_2),
    )
    not_so_empty_data_context = empty_data_context
    project_root_dir = not_so_empty_data_context.root_directory

    with capsys.disabled():
        runner = CliRunner()
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
        )

        stdout = result.stdout
        assert "Profiling 'my_datasource'" in stdout
        assert "Skipping profiling for now." in stdout


def test_cli_datasource_profile_with_datasource_arg(
    empty_data_context, filesystem_csv_2, capsys
):
    empty_data_context.add_datasource(
        "my_datasource",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
        base_directory=str(filesystem_csv_2),
    )
    not_so_empty_data_context = empty_data_context
    project_root_dir = not_so_empty_data_context.root_directory

    with capsys.disabled():
        runner = CliRunner()
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
    assert suites[0].expectation_suite_name == "BasicDatasetProfiler"

    validations_store = context.stores["validations_store"]
    validation_keys = validations_store.list_keys()
    assert len(validation_keys) == 1

    validation = validations_store.get(validation_keys[0])
    assert validation.meta["expectation_suite_name"] == "BasicDatasetProfiler"
    assert validation.success is False
    assert len(validation.results) == 13


def test_cli_datasource_profile_with_no_datasource_args(
    empty_data_context, filesystem_csv_2, capsys
):
    empty_data_context.add_datasource(
        "my_datasource",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
        base_directory=str(filesystem_csv_2),
    )
    not_so_empty_data_context = empty_data_context

    project_root_dir = not_so_empty_data_context.root_directory

    with capsys.disabled():
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["datasource", "profile", "-d", project_root_dir, "--no-view"],
            input="Y\n",
        )
        assert result.exit_code == 0
        stdout = result.stdout
        assert "Profiling 'my_datasource'" in stdout
        assert "The following Data Docs sites were built:\n- local_site:" in stdout

    context = DataContext(project_root_dir)
    assert len(context.list_datasources()) == 1

    expectations_store = context.stores["expectations_store"]
    suites = expectations_store.list_keys()
    assert len(suites) == 1
    assert suites[0].expectation_suite_name == "BasicDatasetProfiler"

    validations_store = context.stores["validations_store"]
    validation_keys = validations_store.list_keys()
    assert len(validation_keys) == 1

    validation = validations_store.get(validation_keys[0])
    assert validation.meta["expectation_suite_name"] == "BasicDatasetProfiler"
    assert validation.success is False
    assert len(validation.results) == 13


def test_cli_datasource_profile_with_additional_batch_kwargs(
    empty_data_context, filesystem_csv_2
):
    empty_data_context.add_datasource(
        "my_datasource",
        class_name="PandasDatasource",
        base_directory=str(filesystem_csv_2),
    )
    not_so_empty_data_context = empty_data_context

    project_root_dir = not_so_empty_data_context.root_directory

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "datasource",
            "profile",
            "-d",
            project_root_dir,
            "--batch_kwargs",
            '{"reader_options": {"sep": ",", "parse_dates": [0]}}',
            "--no-view",
        ],
        input="Y\n",
    )
    assert result.exit_code == 0

    context = DataContext(project_root_dir)
    assert len(context.list_datasources()) == 1

    expectations_store = context.stores["expectations_store"]
    suites = expectations_store.list_keys()
    assert len(suites) == 1
    assert suites[0].expectation_suite_name == "BasicDatasetProfiler"

    validations_store = context.stores["validations_store"]
    validation_keys = validations_store.list_keys()
    assert len(validation_keys) == 1

    validation = validations_store.get(validation_keys[0])
    assert validation.meta["expectation_suite_name"] == "BasicDatasetProfiler"
    assert validation.success is False
    assert len(validation.results) == 8

    evr = context.get_validation_result(
        "f1", expectation_suite_name="BasicDatasetProfiler"
    )
    reader_options = evr.meta["batch_kwargs"]["reader_options"]
    assert reader_options["parse_dates"] == [0]
    assert reader_options["sep"] == ","


def test_cli_datasource_profile_with_valid_data_asset_arg(
    empty_data_context, filesystem_csv_2, capsys
):
    empty_data_context.add_datasource(
        "my_datasource",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
        base_directory=str(filesystem_csv_2),
    )
    not_so_empty_data_context = empty_data_context

    project_root_dir = not_so_empty_data_context.root_directory

    with capsys.disabled():
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "datasource",
                "profile",
                "my_datasource",
                "--data_assets",
                "f1",
                "-d",
                project_root_dir,
                "--no-view",
            ],
        )

        assert result.exit_code == 0
        stdout = result.stdout
        assert "Profiling 'my_datasource'" in stdout
        assert "The following Data Docs sites were built:\n- local_site:" in stdout

    context = DataContext(project_root_dir)
    assert len(context.list_datasources()) == 1

    expectations_store = context.stores["expectations_store"]
    suites = expectations_store.list_keys()
    assert len(suites) == 1
    assert suites[0].expectation_suite_name == "BasicDatasetProfiler"

    validations_store = context.stores["validations_store"]
    validation_keys = validations_store.list_keys()
    assert len(validation_keys) == 1

    validation = validations_store.get(validation_keys[0])
    assert validation.meta["expectation_suite_name"] == "BasicDatasetProfiler"
    assert validation.success is False
    assert len(validation.results) == 13


def test_cli_datasource_profile_with_invalid_data_asset_arg_answering_no(
    empty_data_context, filesystem_csv_2
):
    empty_data_context.add_datasource(
        "my_datasource",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
        base_directory=str(filesystem_csv_2),
    )
    not_so_empty_data_context = empty_data_context

    project_root_dir = not_so_empty_data_context.root_directory

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "datasource",
            "profile",
            "my_datasource",
            "--data_assets",
            "bad-bad-asset",
            "-d",
            project_root_dir,
            "--no-view",
        ],
        input="2\n",
    )

    stdout = result.stdout
    assert (
        "Some of the data assets you specified were not found: bad-bad-asset" in stdout
    )
    assert "Skipping profiling for now." in stdout

    context = DataContext(project_root_dir)
    assert len(context.list_datasources()) == 1

    expectations_store = context.stores["expectations_store"]
    suites = expectations_store.list_keys()
    assert len(suites) == 0
