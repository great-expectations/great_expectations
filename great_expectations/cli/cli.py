# -*- coding: utf-8 -*-

import click
import six
import os
import json
import logging
import sys


from great_expectations import __version__, read_csv
from great_expectations.dataset import Dataset, PandasDataset
from great_expectations.data_asset import FileDataAsset
from great_expectations.data_context import DataContext

from great_expectations.render.renderer import DescriptivePageRenderer, PrescriptivePageRenderer
from great_expectations.render.view import DescriptivePageView


from .util import cli_message
from .init import (
    scaffold_directories_and_notebooks,
    greeting_1,
    msg_prompt_lets_begin,
    # msg_spark_go_to_notebook,
    # msg_sqlalchemy_go_to_notebook,
    # msg_filesys_go_to_notebook,
)
from .datasource import (
    add_datasource
)

# Take over the entire GE module logging namespace when running CLI
logger = logging.getLogger("great_expectations")


@click.group()
@click.version_option(version=__version__)
def cli():
    """great_expectations command-line interface"""
    pass


@cli.command()
@click.argument('dataset')
@click.argument('expectations_config_file')
@click.option('--evaluation_parameters', '-p', default=None,
              help='Path to a file containing JSON object used to evaluate parameters in expectations config.')
@click.option('--result_format', '-o', default="SUMMARY",
              help='Result format to use when building evaluation responses.')
@click.option('--catch_exceptions', '-e', default=True, type=bool,
              help='Specify whether to catch exceptions raised during evaluation of expectations (defaults to True).')
@click.option('--only_return_failures', '-f', default=False, type=bool,
              help='Specify whether to only return expectations that are not met during evaluation \
              (defaults to False).')
@click.option('--custom_dataset_module', '-m', default=None,
              help='Path to a python module containing a custom dataset class.')
@click.option('--custom_dataset_class', '-c', default=None,
              help='Name of the custom dataset class to use during evaluation.')
def validate(dataset, expectations_config_file, evaluation_parameters, result_format,
             catch_exceptions, only_return_failures, custom_dataset_module, custom_dataset_class):
    """Validate a CSV file against an expectations configuration.

    DATASET: Path to a file containing a CSV file to validate using the provided expectations_config_file.

    EXPECTATIONS_CONFIG_FILE: Path to a file containing a valid great_expectations expectations config to use to \
validate the data.
    """

    """
    Read a dataset file and validate it using a config saved in another file. Uses parameters defined in the dispatch
    method.

    :param parsed_args: A Namespace object containing parsed arguments from the dispatch method.
    :return: The number of unsucessful expectations
    """
    expectations_config_file = expectations_config_file

    expectations_config = json.load(open(expectations_config_file))

    if evaluation_parameters is not None:
        evaluation_parameters = json.load(
            open(evaluation_parameters, "r"))

    # Use a custom dataasset module and class if provided. Otherwise infer from the config.
    if custom_dataset_module:
        sys.path.insert(0, os.path.dirname(
            custom_dataset_module))
        module_name = os.path.basename(
            custom_dataset_module).split('.')[0]
        custom_module = __import__(str(module_name))
        dataset_class = getattr(
            custom_module, custom_dataset_class)
    elif "data_asset_type" in expectations_config:
        if (expectations_config["data_asset_type"] == "Dataset" or
                expectations_config["data_asset_type"] == "PandasDataset"):
            dataset_class = PandasDataset
        elif expectations_config["data_asset_type"].endswith("Dataset"):
            logger.info("Using PandasDataset to validate dataset of type %s." %
                        expectations_config["data_asset_type"])
            dataset_class = PandasDataset
        elif expectations_config["data_asset_type"] == "FileDataAsset":
            dataset_class = FileDataAsset
        else:
            logger.critical("Unrecognized data_asset_type %s. You may need to specifcy custom_dataset_module and \
                custom_dataset_class." % expectations_config["data_asset_type"])
            return -1
    else:
        dataset_class = PandasDataset

    if issubclass(dataset_class, Dataset):
        da = read_csv(dataset, expectations_config=expectations_config,
                      dataset_class=dataset_class)
    else:
        da = dataset_class(dataset, config=expectations_config)

    result = da.validate(
        evaluation_parameters=evaluation_parameters,
        result_format=result_format,
        catch_exceptions=catch_exceptions,
        only_return_failures=only_return_failures,
    )

    print(json.dumps(result, indent=2))
    sys.exit(result['statistics']['unsuccessful_expectations'])


@cli.command()
@click.option('--target_directory', '-d', default="./",
              help='The root of the project directory where you want to initialize Great Expectations.')
def init(target_directory):
    """Initialze a new Great Expectations project.

    This guided input walks the user through setting up a project.

    It scaffolds directories, sets up notebooks, creates a project file, and
    appends to a `.gitignore` file.
    """

    context = DataContext.create(target_directory)
    base_dir = os.path.join(target_directory, "great_expectations")

    cli_message("Great Expectations", color="cyan", figlet=True)
    cli_message(greeting_1, color="blue")

    if not click.confirm(msg_prompt_lets_begin, default=True):
        cli_message(
            "OK - run great_expectations init again when ready. Exiting...", color="blue")
        exit(0)

    scaffold_directories_and_notebooks(base_dir)
    cli_message(
        "\nDone.",
        color="blue")

    # Shows a list of options to select from

    add_datasource(context)


@cli.command()
@click.argument('render_object')
def render(render_object):
    """Render a great expectations object.

    RENDER_OBJECT: path to a GE object to render
    """
    with open(render_object, "r") as infile:
        raw = json.load(infile)

    # model = DescriptivePageRenderer.render(raw)
    model = PrescriptivePageRenderer.render(raw)
    print(DescriptivePageView.render(model))


@cli.command()
@click.argument('datasource_name')
@click.option('--max_data_assets', '-m', default=10,
              help='Maximum number of named data assets to profile.')
@click.option('--profile_all_data_assets', '-A', is_flag=True, default=False,
              help='Profile ALL data assets within the target data source. If True, this will override --max_data_assets.')
@click.option('--target_directory', '-d', default="./",
              help='The root of a project directory containing a great_expectations/ config.')
def profile(datasource_name, max_data_assets, profile_all_data_assets, target_directory):
    """Profile a great expectations object.

    datasource_name: A datasource within this GE context to profile.
    """

    if profile_all_data_assets:
        max_data_assets = None

    # FIXME: By default, this should iterate over all datasources
    context = DataContext(target_directory)
    context.profile_datasource(
        datasource_name, max_data_assets=max_data_assets)


def main():
    handler = logging.StreamHandler()
    # Just levelname and message Could re-add other info if we want
    formatter = logging.Formatter(
        '%(levelname)s %(message)s')
    # '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    cli()


if __name__ == '__main__':
    main()
