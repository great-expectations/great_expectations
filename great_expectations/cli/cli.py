# -*- coding: utf-8 -*-
import click
import json
import logging
import os
import shutil
import sys
import warnings
import webbrowser

from great_expectations.cli.init_messages import (
    BUILD_DOCS_PROMPT,
    COMPLETE_ONBOARDING_PROMPT,
    GREETING,
    LETS_BEGIN_PROMPT,
    NEW_TEMPLATE_INSTALLED,
    NEW_TEMPLATE_PROMPT,
    ONBOARDING_COMPLETE,
    PROJECT_IS_COMPLETE,
    RUN_INIT_AGAIN,
)
from .datasource import (
    add_datasource as add_datasource_impl,
    profile_datasource,
    build_docs as build_documentation_impl,
    MSG_GO_TO_NOTEBOOK,
)
from great_expectations.cli.util import cli_message
from great_expectations.render.view import DefaultJinjaPageView
from great_expectations.render.renderer import (
    ProfilingResultsPageRenderer,
    ExpectationSuitePageRenderer,
)
from great_expectations.data_context import DataContext
from great_expectations.data_asset import FileDataAsset
from great_expectations.dataset import Dataset, PandasDataset
import great_expectations.exceptions as ge_exceptions
from great_expectations import __version__ as ge_version
from great_expectations import read_csv
#FIXME: This prevents us from seeing a huge stack of these messages in python 2. We'll need to fix that later.
# tests/test_cli.py::test_cli_profile_with_datasource_arg
#   /Users/abe/Documents/superconductive/tools/great_expectations/tests/test_cli.py:294: Warning: Click detected the use of the unicode_literals __future__ import.  This is heavily discouraged because it can introduce subtle bugs in your code.  You should instead use explicit u"" literals for your unicode strings.  For more information see https://click.palletsprojects.com/python3/
#     cli, ["profile", "my_datasource", "-d", project_root_dir])
click.disable_unicode_literals_warning = True

# from collections import OrderedDict

warnings.filterwarnings('ignore')

try:
    from termcolor import colored
except ImportError:
    colored = None


# Take over the entire GE module logging namespace when running CLI
logger = logging.getLogger("great_expectations")

# class NaturalOrderGroup(click.Group):
#     def __init__(self, name=None, commands=None, **attrs):
#         if commands is None:
#             commands = OrderedDict()
#         elif not isinstance(commands, OrderedDict):
#             commands = OrderedDict(commands)
#         click.Group.__init__(self, name=name,
#                              commands=commands,
#                              **attrs)
#
#     def list_commands(self, ctx):
#         return self.commands.keys()

# TODO: consider using a specified-order supporting class for help (but wasn't working with python 2)
# @click.group(cls=NaturalOrderGroup)
@click.group()
@click.version_option(version=ge_version)
@click.option('--verbose', '-v', is_flag=True, default=False,
              help='Set great_expectations to use verbose output.')
def cli(verbose):
    """great_expectations command-line interface"""
    if verbose:
        logger.setLevel(logging.DEBUG)


@cli.command()
@click.argument('dataset')
@click.argument('expectation_suite_file')
@click.option('--evaluation_parameters', '-p', default=None,
              help='Path to a file containing JSON object used to evaluate parameters in expectations config.')
@click.option('--result_format', '-o', default="SUMMARY",
              help='Result format to use when building evaluation responses.')
@click.option('--catch_exceptions', '-e', default=True, type=bool,
              help='Specify whether to catch exceptions raised during evaluation of expectations (defaults to True).')
@click.option('--only_return_failures', '-f', default=False, type=bool,
              help='Specify whether to only return expectations that are not met during evaluation '
                   '(defaults to False).')
@click.option('--custom_dataset_module', '-m', default=None,
              help='Path to a python module containing a custom dataset class.')
@click.option('--custom_dataset_class', '-c', default=None,
              help='Name of the custom dataset class to use during evaluation.')
def validate(
        dataset,
        expectation_suite_file,
        evaluation_parameters,
        result_format,
        catch_exceptions, only_return_failures, custom_dataset_module, custom_dataset_class):
    """Validate a CSV file against an expectation suite.

    DATASET: Path to a file containing a CSV file to validate using the provided expectation_suite_file.

    EXPECTATION_SUITE_FILE: Path to a file containing a valid great_expectations expectations suite to use to \
validate the data.
    """

    """
    Read a dataset file and validate it using an expectation suite saved in another file. Uses parameters defined in 
    the dispatch method.

    :param parsed_args: A Namespace object containing parsed arguments from the dispatch method.
    :return: The number of unsuccessful expectations
    """
    expectation_suite_file = expectation_suite_file

    expectation_suite = json.load(open(expectation_suite_file))

    if evaluation_parameters is not None:
        evaluation_parameters = json.load(
            open(evaluation_parameters, "r"))

    # Use a custom data_asset module and class if provided. Otherwise infer from the expectation suite
    if custom_dataset_module:
        sys.path.insert(0, os.path.dirname(
            custom_dataset_module))
        module_name = os.path.basename(
            custom_dataset_module).split('.')[0]
        custom_module = __import__(str(module_name))
        dataset_class = getattr(
            custom_module, custom_dataset_class)
    elif "data_asset_type" in expectation_suite:
        if (expectation_suite["data_asset_type"] == "Dataset" or
                expectation_suite["data_asset_type"] == "PandasDataset"):
            dataset_class = PandasDataset
        elif expectation_suite["data_asset_type"].endswith("Dataset"):
            logger.info("Using PandasDataset to validate dataset of type %s." %
                        expectation_suite["data_asset_type"])
            dataset_class = PandasDataset
        elif expectation_suite["data_asset_type"] == "FileDataAsset":
            dataset_class = FileDataAsset
        else:
            logger.critical("Unrecognized data_asset_type %s. You may need to specify custom_dataset_module and \
                custom_dataset_class." % expectation_suite["data_asset_type"])
            return -1
    else:
        dataset_class = PandasDataset

    if issubclass(dataset_class, Dataset):
        da = read_csv(dataset, expectation_suite=expectation_suite,
                      dataset_class=dataset_class)
    else:
        da = dataset_class(dataset, config=expectation_suite)

    result = da.validate(
        evaluation_parameters=evaluation_parameters,
        result_format=result_format,
        catch_exceptions=catch_exceptions,
        only_return_failures=only_return_failures,
    )

    # Note: Should this be rendered through cli_message?
    # Probably not, on the off chance that the JSON object contains <color> tags
    print(json.dumps(result, indent=2))
    sys.exit(result['statistics']['unsuccessful_expectations'])


@cli.command()
@click.option(
    '--target_directory',
    '-d',
    default="./",
    help='The root of the project directory where you want to initialize Great Expectations.'
)
def init(target_directory):
    """
    Create a new project and help with onboarding.

    This guided input walks the user through setting up a new project and also
    onboards a new developer in an existing project.

    It scaffolds directories, sets up notebooks, creates a project file, and
    appends to a `.gitignore` file.
    """
    target_directory = os.path.abspath(target_directory)
    ge_dir = os.path.join(target_directory, "great_expectations")
    ge_yml = os.path.join(ge_dir, "great_expectations.yml")

    cli_message(GREETING)

    # TODO this should be a property
    if os.path.isfile(ge_yml):
        if DataContext.do_all_uncommitted_directories_exist(ge_dir) and \
                DataContext.does_config_variables_yml_exist(ge_dir):
            cli_message(PROJECT_IS_COMPLETE)
        else:
            _complete_onboarding(target_directory)

        try:
            # if expectations exist, offer to build docs
            context = DataContext(ge_dir)
            if context.list_expectation_suite_keys():
                if click.confirm(BUILD_DOCS_PROMPT, default=True):
                    context.build_data_docs()
                    _open_data_docs_in_browser(ge_dir)
        except ge_exceptions.DataContextError as e:
            cli_message("<red>{}</red>".format(e))
    else:
        if not click.confirm(LETS_BEGIN_PROMPT, default=True):
            cli_message(RUN_INIT_AGAIN)
            exit(0)
        else:
            context, data_source_name = _create_new_project(target_directory)
            if not data_source_name:  # no datasource was created
                return

            profile_datasource(context, data_source_name)
            cli_message(MSG_GO_TO_NOTEBOOK)


def _open_data_docs_in_browser(ge_dir):
    """A stdlib cross-platform way to open a file in a browser."""
    ge_dir = os.path.abspath(ge_dir)
    data_docs_index = "file://{}/uncommitted/data_docs/local_site/index.html".format(ge_dir)
    cli_message("Opening Data Docs found here: {}".format(data_docs_index))
    webbrowser.open(data_docs_index)


def _create_new_project(target_directory):
    try:
        context = DataContext.create(target_directory)
        data_source_name = add_datasource_impl(context)
        return context, data_source_name
    except ge_exceptions.DataContextError as err:
        logger.critical(err.message)
        sys.exit(-1)


def _complete_onboarding(target_dir):
    if click.confirm(COMPLETE_ONBOARDING_PROMPT, default=True):
        DataContext.create(target_dir)
        cli_message(ONBOARDING_COMPLETE)
    else:
        cli_message(RUN_INIT_AGAIN)


@cli.command()
@click.option('--directory', '-d', default="./great_expectations",
              help='The root of a project directory containing a great_expectations/ config.')
def add_datasource(directory):
    """Add a new datasource to the data context."""
    try:
        context = DataContext(directory)
    except ge_exceptions.ConfigNotFoundError as err:
        cli_message("<red>{}</red>".format(err.message))
        return
    except ge_exceptions.ZeroDotSevenConfigVersionError as err:
        _offer_to_install_new_template(err, directory)

    data_source_name = add_datasource_impl(context)

    if not data_source_name: # no datasource was created
        return

    profile_datasource(context, data_source_name)


@cli.command()
@click.argument('datasource_name', default=None, required=False)
@click.option('--data_assets', '-l', default=None,
              help='Comma-separated list of the names of data assets that should be profiled. Requires datasource_name specified.')
@click.option('--profile_all_data_assets', '-A', is_flag=True, default=False,
              help='Profile ALL data assets within the target data source. '
                   'If True, this will override --max_data_assets.')
@click.option('--directory', '-d', default="./great_expectations",
              help='The root of a project directory containing a great_expectations/ config.')
@click.option('--batch_kwargs', default=None,
              help='Additional keyword arguments to be provided to get_batch when loading the data asset.')
def profile(datasource_name, data_assets, profile_all_data_assets, directory, batch_kwargs):
    """
    Profile datasources from the specified context.

    If the optional data_assets and profile_all_data_assets arguments are not specified, the profiler will check
    if the number of data assets in the datasource exceeds the internally defined limit. If it does, it will
    prompt the user to either specify the list of data assets to profile or to profile all.
    If the limit is not exceeded, the profiler will profile all data assets in the datasource.

    :param datasource_name: name of the datasource to profile
    :param data_assets: if this comma-separated list of data asset names is provided, only the specified data assets will be profiled
    :param profile_all_data_assets: if provided, all data assets will be profiled
    :param directory:
    :param batch_kwargs: Additional keyword arguments to be provided to get_batch when loading the data asset.
    :return:
    """

    try:
        context = DataContext(directory)
    except ge_exceptions.ConfigNotFoundError as err:
        cli_message("<red>{}</red>".format(err.message))
        return
    except ge_exceptions.ZeroDotSevenConfigVersionError as err:
        _offer_to_install_new_template(err, directory)
        return

    if batch_kwargs is not None:
        batch_kwargs = json.loads(batch_kwargs)

    if datasource_name is None:
        datasources = [datasource["name"] for datasource in context.list_datasources()]
        if len(datasources) > 1:
            cli_message("Error: please specify the datasource to profile. Available datasources: " + ", ".join(datasources))
            return
        else:
            profile_datasource(context, datasources[0], data_assets=data_assets, profile_all_data_assets=profile_all_data_assets, additional_batch_kwargs=batch_kwargs)
    else:
        profile_datasource(context, datasource_name, data_assets=data_assets, profile_all_data_assets=profile_all_data_assets, additional_batch_kwargs=batch_kwargs)


@cli.command()
@click.option('--directory', '-d', default="./great_expectations",
              help='The root of a project directory containing a great_expectations/ config.')
@click.option('--site_name', '-s',
              help='The site for which to generate documentation. See data_docs section in great_expectations.yml')
@click.option('--data_asset_name', '-dan',
              help='The data asset for which to generate documentation. Must also specify --site_name.')
@click.option(
    "--view/--no-view",
    help="By default open in browser unless you specify the --no-view flag",
    default=True
)
def build_docs(directory, site_name, data_asset_name, view=True):
    """Build Data Docs for a project."""
    logger.debug("Starting cli.build_docs")

    if data_asset_name is not None and site_name is None:
        cli_message("<red>Error: When specifying `data_asset_name`, `site_name` is required.</red>")
        sys.exit(1)
        
    try:
        context = DataContext(directory)
        build_documentation_impl(
            context,
            site_name=site_name,
            data_asset_name=data_asset_name
        )
        if view:
            ge_dir = os.path.abspath(directory)
            _open_data_docs_in_browser(ge_dir)
    except ge_exceptions.ConfigNotFoundError as err:
        cli_message("<red>{}</red>".format(err.message))
        sys.exit(1)
    except ge_exceptions.ZeroDotSevenConfigVersionError as err:
        _offer_to_install_new_template(err, directory)
        return
    except ge_exceptions.PluginModuleNotFoundError as err:
        cli_message(err.cli_colored_message)
        sys.exit(1)
    except ge_exceptions.PluginClassNotFoundError as err:
        cli_message(err.cli_colored_message)
        sys.exit(1)



@cli.command()
@click.argument('render_object')
def render(render_object):
    """Render a great expectations object to documentation.

    RENDER_OBJECT: path to a GE object to render
    """
    with open(render_object, "r") as infile:
        raw = json.load(infile)

    if "results" in raw:
        model = ProfilingResultsPageRenderer.render(raw)
    else:
        model = ExpectationSuitePageRenderer.render(raw)
    print(DefaultJinjaPageView.render(model))


@cli.command()
@click.option(
    '--target_directory',
    '-d',
    default="./",
    help='The root of the project directory where you want to initialize Great Expectations.'
)
def check_config(target_directory):
    """Check a config for validity and help with migrations."""
    cli_message("Checking your config files for validity...\n")

    try:
        is_config_ok, error_message = do_config_check(target_directory)
        if is_config_ok:
            cli_message("Your config file appears valid!")
        else:
            cli_message("Unfortunately, your config appears to be invalid:\n")
            cli_message("<red>{}</red>".format(error_message))
            sys.exit(1)
    except ge_exceptions.ZeroDotSevenConfigVersionError as err:
        _offer_to_install_new_template(err, target_directory)


def _offer_to_install_new_template(err, target_directory):
    cli_message("<red>{}</red>".format(err.message))
    ge_yml = "great_expectations.yml"
    archived_yml = "great_expectations.yml.archive"
    if click.confirm(
        NEW_TEMPLATE_PROMPT.format(ge_yml, archived_yml),
        default=True
    ):
        _archive_existing_project_config(archived_yml, target_directory)
        DataContext.write_project_template_to_disk(target_directory)

        cli_message(
            NEW_TEMPLATE_INSTALLED.format(ge_yml, archived_yml)
        )
    else:
        # FIXME/TODO insert doc url here
        cli_message(
            """\nOK. To continue, you will need to upgrade your config file to the latest format.
  - Please see the docs here:
  - We are super sorry about this breaking change! :]
  - If you are running into any problems, please reach out on Slack and we can
    help you in realtime: https://greatexpectations.io/slack""")
    sys.exit(0)


def _archive_existing_project_config(archive_filename, target_directory):
    base = os.path.join(target_directory, "great_expectations")
    source = os.path.join(base, "great_expectations.yml")
    destination = os.path.join(base, archive_filename)
    shutil.move(source, destination)


def do_config_check(target_directory):
    try:
        DataContext(
            context_root_dir="{}/great_expectations/".format(target_directory)
        )
        return True, None
    except (
            ge_exceptions.InvalidConfigurationYamlError,
            ge_exceptions.InvalidTopLevelConfigKeyError,
            ge_exceptions.MissingTopLevelConfigKeyError,
            ge_exceptions.InvalidConfigValueTypeError,
            ge_exceptions.InvalidConfigVersionError,
            ge_exceptions.UnsupportedConfigVersionError,
            ge_exceptions.DataContextError,
            ge_exceptions.PluginClassNotFoundError
            ) as err:
        return False, err.message


def main():
    handler = logging.StreamHandler()
    # Just levelname and message Could re-add other info if we want
    formatter = logging.Formatter(
        '%(message)s')
    # '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    cli()


if __name__ == '__main__':
    main()