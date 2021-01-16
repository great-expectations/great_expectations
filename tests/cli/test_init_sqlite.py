import os
import re
import shutil

import pytest
from click.testing import CliRunner
from freezegun import freeze_time

from great_expectations import DataContext
from great_expectations.cli import cli
from great_expectations.data_context.util import file_relative_path
from great_expectations.util import gen_directory_tree_str
from tests.cli.test_cli import yaml
from tests.cli.test_datasource_sqlite import _add_datasource_and_credentials_to_context
from tests.cli.test_init_pandas import _delete_and_recreate_dir
from tests.cli.utils import assert_no_logging_messages_or_tracebacks

try:
    from unittest import mock
except ImportError:
    from unittest import mock


@pytest.fixture
def titanic_sqlite_db_file(sa, tmp_path_factory):
    temp_dir = str(tmp_path_factory.mktemp("foo_path"))
    fixture_db_path = file_relative_path(__file__, "../test_sets/titanic.db")

    db_path = os.path.join(temp_dir, "titanic.db")
    shutil.copy(fixture_db_path, db_path)

    engine = sa.create_engine("sqlite:///{}".format(db_path), pool_recycle=3600)
    assert engine.execute("select count(*) from titanic").fetchall()[0] == (1313,)
    return db_path


@mock.patch("webbrowser.open", return_value=True, side_effect=None)
@freeze_time("09/26/2019 13:42:41")
def test_cli_init_on_new_project(
    mock_webbrowser, caplog, tmp_path_factory, titanic_sqlite_db_file, sa
):
    project_dir = str(tmp_path_factory.mktemp("test_cli_init_diff"))
    ge_dir = os.path.join(project_dir, "great_expectations")

    database_path = os.path.join(project_dir, "titanic.db")
    shutil.copy(titanic_sqlite_db_file, database_path)
    engine = sa.create_engine("sqlite:///{}".format(database_path), pool_recycle=3600)

    inspector = sa.inspect(engine)

    # get the default schema and table for testing
    schemas = inspector.get_schema_names()
    default_schema = schemas[0]

    tables = [
        table_name for table_name in inspector.get_table_names(schema=default_schema)
    ]
    default_table = tables[0]

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["init", "-d", project_dir],
        input="\n\n2\n6\ntitanic\n{url}\n\n\n1\n{schema}\n{table}\nwarning\n\n\n\n".format(
            url=engine.url, schema=default_schema, table=default_table
        ),
        catch_exceptions=False,
    )
    stdout = result.output
    assert len(stdout) < 6000, "CLI output is unreasonably long."

    assert "Always know what to expect from your data" in stdout
    assert "What data would you like Great Expectations to connect to" in stdout
    assert "Which database backend are you using" in stdout
    assert "Give your new Datasource a short name" in stdout
    assert "What is the url/connection string for the sqlalchemy connection" in stdout
    assert "Attempting to connect to your database." in stdout
    assert "Great Expectations connected to your database" in stdout
    assert (
        "You have selected a datasource that is a SQL database. How would you like to specify the data?"
        in stdout
    )
    assert "Name the new Expectation Suite [main.titanic.warning]" in stdout
    assert (
        "Great Expectations will choose a couple of columns and generate expectations about them"
        in stdout
    )
    assert "Generating example Expectation Suite..." in stdout
    assert "Building" in stdout
    assert "Data Docs" in stdout
    assert "Great Expectations is now set up" in stdout

    context = DataContext(ge_dir)
    assert len(context.list_datasources()) == 1
    assert context.list_datasources()[0]["class_name"] == "SqlAlchemyDatasource"
    assert context.list_datasources()[0]["name"] == "titanic"

    first_suite = context.list_expectation_suites()[0]
    suite = context.get_expectation_suite(first_suite.expectation_suite_name)
    assert len(suite.expectations) == 14

    assert os.path.isdir(ge_dir)
    config_path = os.path.join(project_dir, "great_expectations/great_expectations.yml")
    assert os.path.isfile(config_path)

    config = yaml.load(open(config_path))
    data_source_class = config["datasources"]["titanic"]["data_asset_type"][
        "class_name"
    ]
    assert data_source_class == "SqlAlchemyDataset"

    obs_tree = gen_directory_tree_str(ge_dir)

    # Instead of monkey patching guids, just regex out the guids
    guid_safe_obs_tree = re.sub(
        r"[a-z0-9]{32}(?=\.(json|html))", "foobarbazguid", obs_tree
    )
    # print(guid_safe_obs_tree)
    assert (
        guid_safe_obs_tree
        == """\
great_expectations/
    .gitignore
    great_expectations.yml
    checkpoints/
    expectations/
        .ge_store_backend_id
        warning.json
    notebooks/
        pandas/
            validation_playground.ipynb
        spark/
            validation_playground.ipynb
        sql/
            validation_playground.ipynb
    plugins/
        custom_data_docs/
            renderers/
            styles/
                data_docs_custom_styles.css
            views/
    uncommitted/
        config_variables.yml
        data_docs/
            local_site/
                index.html
                expectations/
                    warning.html
                static/
                    fonts/
                        HKGrotesk/
                            HKGrotesk-Bold.otf
                            HKGrotesk-BoldItalic.otf
                            HKGrotesk-Italic.otf
                            HKGrotesk-Light.otf
                            HKGrotesk-LightItalic.otf
                            HKGrotesk-Medium.otf
                            HKGrotesk-MediumItalic.otf
                            HKGrotesk-Regular.otf
                            HKGrotesk-SemiBold.otf
                            HKGrotesk-SemiBoldItalic.otf
                    images/
                        favicon.ico
                        glossary_scroller.gif
                        iterative-dev-loop.png
                        logo-long-vector.svg
                        logo-long.png
                        short-logo-vector.svg
                        short-logo.png
                        validation_failed_unexpected_values.gif
                    styles/
                        data_docs_custom_styles_template.css
                        data_docs_default_styles.css
                validations/
                    warning/
                        20190926T134241.000000Z/
                            20190926T134241.000000Z/
                                foobarbazguid.html
        validations/
            .ge_store_backend_id
            warning/
                20190926T134241.000000Z/
                    20190926T134241.000000Z/
                        foobarbazguid.json
"""
    )

    assert_no_logging_messages_or_tracebacks(caplog, result)

    assert result.exit_code == 0
    assert mock_webbrowser.call_count == 1
    assert (
        "{}/great_expectations/uncommitted/data_docs/local_site/validations/warning/".format(
            project_dir
        )
        in mock_webbrowser.call_args[0][0]
    )


@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_cli_init_on_new_project_extra_whitespace_in_url(
    mock_webbrowser, caplog, tmp_path_factory, titanic_sqlite_db_file, sa
):
    project_dir = str(tmp_path_factory.mktemp("test_cli_init_diff"))
    ge_dir = os.path.join(project_dir, "great_expectations")

    database_path = os.path.join(project_dir, "titanic.db")
    shutil.copy(titanic_sqlite_db_file, database_path)
    engine = sa.create_engine("sqlite:///{}".format(database_path), pool_recycle=3600)
    engine_url_with_added_whitespace = "    " + str(engine.url) + "  "

    inspector = sa.inspect(engine)

    # get the default schema and table for testing
    schemas = inspector.get_schema_names()
    default_schema = schemas[0]

    tables = [
        table_name for table_name in inspector.get_table_names(schema=default_schema)
    ]
    default_table = tables[0]

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["init", "-d", project_dir],
        input="\n\n2\n6\ntitanic\n{url}\n\n\n1\n{schema}\n{table}\nwarning\n\n\n\n".format(
            url=engine_url_with_added_whitespace,
            schema=default_schema,
            table=default_table,
        ),
        catch_exceptions=False,
    )
    stdout = result.output
    assert len(stdout) < 6000, "CLI output is unreasonably long."

    assert "Always know what to expect from your data" in stdout
    assert "What data would you like Great Expectations to connect to" in stdout
    assert "Which database backend are you using" in stdout
    assert "Give your new Datasource a short name" in stdout
    assert "What is the url/connection string for the sqlalchemy connection" in stdout
    assert "Attempting to connect to your database." in stdout
    assert "Great Expectations connected to your database" in stdout
    assert (
        "You have selected a datasource that is a SQL database. How would you like to specify the data?"
        in stdout
    )
    assert "Name the new Expectation Suite [main.titanic.warning]" in stdout
    assert (
        "Great Expectations will choose a couple of columns and generate expectations about them"
        in stdout
    )
    assert "Generating example Expectation Suite..." in stdout
    assert "Building" in stdout
    assert "Data Docs" in stdout
    assert "Great Expectations is now set up" in stdout

    context = DataContext(ge_dir)
    assert len(context.list_datasources()) == 1
    assert context.list_datasources() == [
        {
            "class_name": "SqlAlchemyDatasource",
            "name": "titanic",
            "module_name": "great_expectations.datasource",
            "credentials": {"url": str(engine.url)},
            "data_asset_type": {
                "class_name": "SqlAlchemyDataset",
                "module_name": "great_expectations.dataset",
            },
        }
    ]

    first_suite = context.list_expectation_suites()[0]
    suite = context.get_expectation_suite(first_suite.expectation_suite_name)
    assert len(suite.expectations) == 14

    assert os.path.isdir(ge_dir)
    config_path = os.path.join(project_dir, "great_expectations/great_expectations.yml")
    assert os.path.isfile(config_path)

    config = yaml.load(open(config_path))
    data_source_class = config["datasources"]["titanic"]["data_asset_type"][
        "class_name"
    ]
    assert data_source_class == "SqlAlchemyDataset"

    assert_no_logging_messages_or_tracebacks(caplog, result)

    assert result.exit_code == 0
    assert mock_webbrowser.call_count == 1
    assert (
        "{}/great_expectations/uncommitted/data_docs/local_site/validations/warning/".format(
            project_dir
        )
        in mock_webbrowser.call_args[0][0]
    )


@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_init_on_existing_project_with_no_datasources_should_continue_init_flow_and_add_one(
    mock_webbrowser, caplog, initialized_sqlite_project, titanic_sqlite_db_file, sa
):
    project_dir = initialized_sqlite_project
    ge_dir = os.path.join(project_dir, DataContext.GE_DIR)

    _remove_all_datasources(ge_dir)
    os.remove(os.path.join(ge_dir, "expectations", "warning.json"))
    context = DataContext(ge_dir)
    assert not context.list_expectation_suites()

    runner = CliRunner(mix_stderr=False)
    url = "sqlite:///{}".format(titanic_sqlite_db_file)

    inspector = sa.inspect(sa.create_engine(url))

    # get the default schema and table for testing
    schemas = inspector.get_schema_names()
    default_schema = schemas[0]

    tables = [
        table_name for table_name in inspector.get_table_names(schema=default_schema)
    ]
    default_table = tables[0]

    with pytest.warns(
        UserWarning, match="Warning. An existing `great_expectations.yml` was found"
    ):
        result = runner.invoke(
            cli,
            ["init", "-d", project_dir],
            input="\n\n2\n6\nsqlite\n{url}\n\n\n1\n{schema}\n{table}\nmy_suite\n\n\n\n".format(
                url=url, schema=default_schema, table=default_table
            ),
            catch_exceptions=False,
        )
    stdout = result.stdout

    assert result.exit_code == 0
    assert mock_webbrowser.call_count == 1
    assert (
        "{}/great_expectations/uncommitted/data_docs/local_site/validations/my_suite/".format(
            project_dir
        )
        in mock_webbrowser.call_args[0][0]
    )

    assert "Error: invalid input" not in stdout
    assert "Always know what to expect from your data" in stdout
    assert "What data would you like Great Expectations to connect to" in stdout
    assert (
        "Next, we will configure database credentials and store them in the `sqlite` section"
        in stdout
    )
    assert "What is the url/connection string for the sqlalchemy connection?" in stdout
    assert (
        "You have selected a datasource that is a SQL database. How would you like to specify the data?"
        in stdout
    )
    assert "Great Expectations connected to your database" in stdout
    assert "This looks like an existing project that" not in stdout

    config = _load_config_file(os.path.join(ge_dir, DataContext.GE_YML))
    assert "sqlite" in config["datasources"].keys()

    context = DataContext(ge_dir)
    assert context.list_datasources() == [
        {
            "class_name": "SqlAlchemyDatasource",
            "name": "sqlite",
            "module_name": "great_expectations.datasource",
            "credentials": {"url": url},
            "data_asset_type": {
                "class_name": "SqlAlchemyDataset",
                "module_name": "great_expectations.dataset",
            },
        }
    ]
    assert context.list_expectation_suites()[0].expectation_suite_name == "my_suite"
    assert len(context.list_expectation_suites()) == 1

    assert_no_logging_messages_or_tracebacks(caplog, result)


def _remove_all_datasources(ge_dir):
    config_path = os.path.join(ge_dir, DataContext.GE_YML)

    config = _load_config_file(config_path)
    config["datasources"] = {}

    with open(config_path, "w") as f:
        yaml.dump(config, f)

    context = DataContext(ge_dir)
    assert context.list_datasources() == []


def _load_config_file(config_path):
    assert os.path.isfile(config_path), "Config file is missing. Check path"

    with open(config_path) as f:
        read = f.read()
        config = yaml.load(read)

    assert isinstance(config, dict)
    return config


@pytest.fixture
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def initialized_sqlite_project(
    mock_webbrowser, caplog, tmp_path_factory, titanic_sqlite_db_file, sa
):
    """This is an initialized project through the CLI."""
    project_dir = str(tmp_path_factory.mktemp("my_rad_project"))

    engine = sa.create_engine(
        "sqlite:///{}".format(titanic_sqlite_db_file), pool_recycle=3600
    )

    inspector = sa.inspect(engine)

    # get the default schema and table for testing
    schemas = inspector.get_schema_names()
    default_schema = schemas[0]

    tables = [
        table_name for table_name in inspector.get_table_names(schema=default_schema)
    ]
    default_table = tables[0]

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["init", "-d", project_dir],
        input="\n\n2\n6\ntitanic\n{url}\n\n\n1\n{schema}\n{table}\nwarning\n\n\n\n".format(
            url=engine.url, schema=default_schema, table=default_table
        ),
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert mock_webbrowser.call_count == 1
    assert (
        "{}/great_expectations/uncommitted/data_docs/local_site/validations/warning/".format(
            project_dir
        )
        in mock_webbrowser.call_args[0][0]
    )

    assert_no_logging_messages_or_tracebacks(caplog, result)

    context = DataContext(os.path.join(project_dir, DataContext.GE_DIR))
    assert isinstance(context, DataContext)
    assert len(context.list_datasources()) == 1
    assert context.list_datasources() == [
        {
            "class_name": "SqlAlchemyDatasource",
            "name": "titanic",
            "module_name": "great_expectations.datasource",
            "credentials": {"url": str(engine.url)},
            "data_asset_type": {
                "class_name": "SqlAlchemyDataset",
                "module_name": "great_expectations.dataset",
            },
        }
    ]
    return project_dir


@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_init_on_existing_project_with_multiple_datasources_exist_do_nothing(
    mock_webbrowser,
    caplog,
    initialized_sqlite_project,
    titanic_sqlite_db,
    empty_sqlite_db,
):
    project_dir = initialized_sqlite_project
    ge_dir = os.path.join(project_dir, DataContext.GE_DIR)

    context = DataContext(ge_dir)
    datasource_name = "wow_a_datasource"
    context = _add_datasource_and_credentials_to_context(
        context, datasource_name, empty_sqlite_db
    )
    assert len(context.list_datasources()) == 2

    runner = CliRunner(mix_stderr=False)
    with pytest.warns(
        UserWarning, match="Warning. An existing `great_expectations.yml` was found"
    ):
        result = runner.invoke(
            cli,
            ["init", "-d", project_dir],
            input="n\n",
            catch_exceptions=False,
        )
    stdout = result.stdout

    assert result.exit_code == 0
    assert mock_webbrowser.call_count == 0

    assert "Error: invalid input" not in stdout

    assert "Always know what to expect from your data" in stdout
    assert "This looks like an existing project that" in stdout
    assert "appears complete" in stdout
    assert "Would you like to build & view this project's Data Docs" in stdout

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_init_on_existing_project_with_datasource_with_existing_suite_offer_to_build_docs_answer_no(
    mock_webbrowser,
    caplog,
    initialized_sqlite_project,
):
    project_dir = initialized_sqlite_project

    runner = CliRunner(mix_stderr=False)
    with pytest.warns(
        UserWarning, match="Warning. An existing `great_expectations.yml` was found"
    ):
        result = runner.invoke(
            cli,
            ["init", "-d", project_dir],
            input="n\n",
            catch_exceptions=False,
        )
    stdout = result.stdout

    assert result.exit_code == 0
    assert mock_webbrowser.call_count == 0

    assert "Error: invalid input" not in stdout

    assert "Always know what to expect from your data" in stdout
    assert "This looks like an existing project that" in stdout
    assert "appears complete" in stdout
    assert "Would you like to build & view this project's Data Docs" in stdout

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_init_on_existing_project_with_datasource_with_existing_suite_offer_to_build_docs_answer_yes(
    mock_webbrowser,
    caplog,
    initialized_sqlite_project,
):
    project_dir = initialized_sqlite_project

    runner = CliRunner(mix_stderr=False)
    with pytest.warns(
        UserWarning, match="Warning. An existing `great_expectations.yml` was found"
    ):
        result = runner.invoke(
            cli,
            ["init", "-d", project_dir],
            input="\n\n",
            catch_exceptions=False,
        )
    stdout = result.stdout

    assert result.exit_code == 0
    assert mock_webbrowser.call_count == 1
    assert (
        "{}/great_expectations/uncommitted/data_docs/local_site/index.html".format(
            project_dir
        )
        in mock_webbrowser.call_args[0][0]
    )

    assert "Error: invalid input" not in stdout

    assert "Always know what to expect from your data" in stdout
    assert "This looks like an existing project that" in stdout
    assert "appears complete" in stdout
    assert "Would you like to build & view this project's Data Docs" in stdout

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_init_on_existing_project_with_datasource_with_no_suite_create_one(
    mock_webbrowser, caplog, initialized_sqlite_project, sa
):
    project_dir = initialized_sqlite_project
    ge_dir = os.path.join(project_dir, DataContext.GE_DIR)
    uncommitted_dir = os.path.join(ge_dir, "uncommitted")

    # mangle the setup to remove all traces of any suite
    expectations_dir = os.path.join(ge_dir, "expectations")
    data_docs_dir = os.path.join(uncommitted_dir, "data_docs")
    validations_dir = os.path.join(uncommitted_dir, "validations")

    _delete_and_recreate_dir(expectations_dir)
    _delete_and_recreate_dir(data_docs_dir)
    _delete_and_recreate_dir(validations_dir)

    context = DataContext(ge_dir)

    # get the datasource from data context
    all_datasources = context.list_datasources()
    datasource = all_datasources[0] if all_datasources else None

    # create a sqlalchemy engine using the URL of existing datasource
    engine = sa.create_engine(datasource.get("credentials", dict()).get("url"))
    inspector = sa.inspect(engine)

    # get the default schema and table for testing
    schemas = inspector.get_schema_names()
    default_schema = schemas[0]

    tables = [
        table_name for table_name in inspector.get_table_names(schema=default_schema)
    ]
    default_table = tables[0]

    assert context.list_expectation_suites() == []

    runner = CliRunner(mix_stderr=False)
    with pytest.warns(
        UserWarning, match="Warning. An existing `great_expectations.yml` was found"
    ):
        result = runner.invoke(
            cli,
            ["init", "-d", project_dir],
            input="\n1\n{schema}\n{table}\nsink_me\n\n\n\n".format(
                os.path.join(project_dir, "data/Titanic.csv"),
                schema=default_schema,
                table=default_table,
            ),
            catch_exceptions=False,
        )
    stdout = result.stdout

    assert result.exit_code == 0
    assert mock_webbrowser.call_count == 1
    assert (
        "{}/great_expectations/uncommitted/data_docs/local_site/validations/sink_me/".format(
            project_dir
        )
        in mock_webbrowser.call_args[0][0]
    )

    assert "Always know what to expect from your data" in stdout
    assert (
        "You have selected a datasource that is a SQL database. How would you like to specify the data?"
        in stdout
    )
    assert "Generating example Expectation Suite..." in stdout
    assert "The following Data Docs sites will be built" in stdout
    assert "Great Expectations is now set up" in stdout

    assert "Error: invalid input" not in stdout
    assert "This looks like an existing project that" not in stdout

    assert_no_logging_messages_or_tracebacks(caplog, result)

    context = DataContext(ge_dir)
    assert len(context.list_expectation_suites()) == 1
