# -*- coding: utf-8 -*-
"""
# Style guide for the Great Expectations CLI.

###
#
# The CLI never writes to disk without asking first.
# Questions are always phrased as conversational sentences.
# Sections are divided by headers: "========== Profiling =========="
# We use punctuation: Please finish sentences with periods, questions marks, or an occasional exclamation point.
# Keep indentation consistent! (We're pythonistas, natch.)
# Include exactly one blank line after every question.
# Within those constraints, shorter is better. When in doubt, shorten.
# Clickable links (usually to documentation) are blue.
# Copyable bash commands are green.
#
###

"""
from .datasource import (
    add_datasource,
    profile_datasource,
    build_documentation,
    msg_go_to_notebook
)
from .init import (
    scaffold_directories_and_notebooks,
    greeting_1,
    msg_prompt_lets_begin,
)
from .util import cli_message
from great_expectations.render.view import DefaultJinjaPageView
from great_expectations.render.renderer import DescriptivePageRenderer, PrescriptivePageRenderer
from great_expectations.data_context import DataContext
from great_expectations.data_asset import FileDataAsset
from great_expectations.dataset import Dataset, PandasDataset
from great_expectations.exceptions import DataContextError, ConfigNotFoundError
from great_expectations import __version__, read_csv
from pyfiglet import figlet_format
import click
import six
import os
import json
import logging
import sys
import warnings
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
@click.version_option(version=__version__)
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
    """Initialize a new Great Expectations project.

    This guided input walks the user through setting up a project.

    It scaffolds directories, sets up notebooks, creates a project file, and
    appends to a `.gitignore` file.
    """
    six.print_(colored(
        figlet_format("Great Expectations", font="big"),
        color="cyan"
    ))

    cli_message(greeting_1)

    if not click.confirm(msg_prompt_lets_begin, default=True):
        cli_message(
            "OK - run great_expectations init again when ready. Exiting..."
        )
        exit(0)

    try:
        context = DataContext.create(target_directory)
    except DataContextError as err:
        logger.critical(err.message)
        sys.exit(-1)

    base_dir = context.root_directory
    scaffold_directories_and_notebooks(base_dir)
    cli_message(
        "\nDone.",
    )

    data_source_name = add_datasource(context)

    if not data_source_name: # no datasource was created
        return

    profile_datasource(context, data_source_name)

    cli_message(msg_go_to_notebook)


@cli.command()
@click.argument('datasource_name', default=None, required=False)
@click.option('--data_assets', '-l', default=None,
              help='Comma-separated list of the names of data assets that should be profiled. Requires datasource_name specified.')
@click.option('--profile_all_data_assets', '-A', is_flag=True, default=False,
              help='Profile ALL data assets within the target data source. '
                   'If True, this will override --max_data_assets.')
@click.option('--directory', '-d', default="./great_expectations",
              help='The root of a project directory containing a great_expectations/ config.')
def profile(datasource_name, data_assets, profile_all_data_assets, directory):
    """Profile datasources from the specified context.


    DATASOURCE_NAME: the datasource to profile, or leave blank to profile all datasources."""


    if profile_all_data_assets:
        max_data_assets = None

    try:
        context = DataContext(directory)
    except ConfigNotFoundError:
        cli_message("Error: no great_expectations context configuration found in the specified directory.")
        return

    if datasource_name is None:
        datasources = [datasource["name"] for datasource in context.list_datasources()]
        for datasource_name in datasources:
            profile_datasource(context, datasource_name, max_data_assets=max_data_assets)
    else:
        profile_datasource(context, datasource_name, data_assets=data_assets)


@cli.command()
@click.option('--directory', '-d', default="./great_expectations",
              help='The root of a project directory containing a great_expectations/ config.')
def documentation(directory):
    """Build data documentation for a project.
    """
    try:
        context = DataContext(directory)
    except ConfigNotFoundError:
        cli_message("Error: no great_expectations context configuration found in the specified directory.")
        return

    build_documentation(context)


@cli.command()
@click.argument('render_object')
def render(render_object):
    """Render a great expectations object to documentation.

    RENDER_OBJECT: path to a GE object to render
    """
    with open(render_object, "r") as infile:
        raw = json.load(infile)

    if "results" in raw:
        model = DescriptivePageRenderer.render(raw)
    else:
        model = PrescriptivePageRenderer.render(raw)
    print(DefaultJinjaPageView.render(model))


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
