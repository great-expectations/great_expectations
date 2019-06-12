# -*- coding: utf-8 -*-

import click
import six
import os
import json
import logging
import sys

from pyfiglet import figlet_format

try:
    from termcolor import colored
except ImportError:
    colored = None

from .supporting_methods import _scaffold_directories_and_notebooks
from great_expectations import __version__, read_csv
from great_expectations.dataset import Dataset, PandasDataset
from great_expectations.data_asset import FileDataAsset
from great_expectations.data_context import DataContext

from great_expectations.render.renderer import DescriptivePageRenderer, PrescriptivePageRenderer
from great_expectations.render.view import DescriptivePageView

# Take over the entire GE module logging namespace when running CLI
logger = logging.getLogger("great_expectations")

def cli_message(string, color, font="big", figlet=False):
    if colored:
        if not figlet:
            six.print_(colored(string, color))
        else:
            six.print_(colored(figlet_format(
                string, font=font), color))
    else:
        six.print_(string)


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

    #!!! This injects a version tag into the docs. We should test that those versioned docs exist in RTD.
    greeting_1 = """
Always know what to expect from your data.

If you're new to Great Expectations, this tutorial is a good place to start:

    https://great-expectations.readthedocs.io/en/v%s/intro.html#how-do-i-get-started
    """ % __version__

    msg_prompt_lets_begin = """
Let's add Great Expectations to your project, by scaffolding a new great_expectations directory:

    great_expectations
        ├── great_expectations.yml
        ├── datasources
        ├── expectations
        ├── fixtures
        ├── notebooks
        ├── plugins
        ├── uncommitted
        │   ├── validations        
        │   ├── credentials        
        │   └── samples        
        └── .gitignore
    
OK to proceed?    
    """

    msg_prompt_choose_data_source = """
Configure a data source
    1. Pandas data frames from local filesystem (CSV files)
    2. Relational database (SQL)
    3. Spark DataFrames from local filesystem (CSV files)
    4. None of the above
    """

#     msg_prompt_choose_data_source = """
# Time to create expectations for your data. This is done in Jupyter Notebook/Jupyter Lab.
#
# Before we point you to the right notebook, what data does your project work with?
#     1. Directory on local filesystem
#     2. Relational database (SQL)
#     3. DBT (data build tool) models
#     4. None of the above
#     """


#     msg_prompt_dbt_choose_profile = """
# Please specify the name of the dbt profile (from your ~/.dbt/profiles.yml file Great Expectations \
# should use to connect to the database
#     """

#     msg_dbt_go_to_notebook = """
# To create expectations for your dbt models start Jupyter and open notebook
# great_expectations/notebooks/using_great_expectations_with_dbt.ipynb -
# it will walk you through next steps.
#     """

    msg_prompt_filesys_enter_base_path = """
Enter the path of the root directory where the data files are stored 
(the path may be either absolute or relative to current directory)          
    """

    msg_filesys_go_to_notebook = """
To create expectations for your CSV files start Jupyter and open the notebook
great_expectations/notebooks/using_great_expectations_with_pandas.ipynb.
it will walk you through configuring the database connection and next steps. 

To launch with jupyter notebooks:
    jupyter notebook great_expectations/notebooks/create_expectations_for_csv_files.ipynb

To launch with jupyter lab: 
    jupyter lab great_expectations/notebooks/create_expectations_for_csv_files.ipynb
    """

    msg_prompt_datasource_name = """
Give your new data source a short name    
    """

    msg_sqlalchemy_config_connection = """
Great Expectations relies on sqlalchemy to connect to relational databases.
Please make sure that you have it installed.         

Next, we will configure database credentials and store them in the "{0:s}" section
of this config file: great_expectations/uncommitted/credentials/profiles.yml:
     """

    msg_sqlalchemy_go_to_notebook = """
To create expectations for your SQL queries start Jupyter and open notebook 
great_expectations/notebooks/using_great_expectations_with_sql.ipynb - 
it will walk you through configuring the database connection and next steps. 
"""

    msg_unknown_data_source = """
We are looking for more types of data types to support. 
Please create a GitHub issue here: 
https://github.com/great-expectations/great_expectations/issues/new
In the meantime you can see what Great Expectations can do on CSV files.
To create expectations for your CSV files start Jupyter and open notebook
great_expectations/notebooks/using_great_expectations_with_pandas.ipynb - 
it will walk you through configuring the database connection and next steps. 
     """
    msg_spark_go_to_notebook = """
To create expectations for your CSV files start Jupyter and open the notebook
great_expectations/notebooks/using_great_expectations_with_pandas.ipynb.
it will walk you through configuring the database connection and next steps. 

To launch with jupyter notebooks:
    jupyter notebook great_expectations/notebooks/create_expectations_for_spark_dataframes.ipynb

To launch with jupyter lab: 
    jupyter lab great_expectations/notebooks/create_expectations_for_spark_dataframes.ipynb
    """
    context = DataContext.create('.')

    base_dir = os.path.join(target_directory, "great_expectations")

    cli_message("Great Expectations", color="cyan", figlet=True)

    cli_message(greeting_1, color="blue")

    if not click.confirm(msg_prompt_lets_begin, default=True):
        cli_message(
            "OK - run great_expectations init again when ready. Exiting...", color="blue")
        exit(0)

    _scaffold_directories_and_notebooks(base_dir)
    cli_message(
        "\nDone.",
        color="blue")


    # Shows a list of options to select from

    data_source_selection = click.prompt(msg_prompt_choose_data_source, type=click.Choice(["1", "2", "3", "4"]),
                                         show_choices=False)

    print(data_source_selection)

    # if data_source_selection == "5": # dbt
    #     dbt_profile = click.prompt(msg_prompt_dbt_choose_profile)
    #     log_message(msg_dbt_go_to_notebook, color="blue")
    #     context.add_datasource("dbt", "dbt", profile=dbt_profile)
    if data_source_selection == "3":  # Spark
        path = click.prompt(msg_prompt_filesys_enter_base_path, default='/data/', type=click.Path(exists=True,
                                                                                                  file_okay=False,
                                                                                                  dir_okay=True,
                                                                                                  readable=True),
                            show_default=True)
        if path.startswith("./"):
            path = path[2:]

        if path.endswith("/"):
            basenamepath = path[:-1]
        default_data_source_name = os.path.basename(basenamepath)
        data_source_name = click.prompt(
            msg_prompt_datasource_name, default=default_data_source_name, show_default=True)

        cli_message(msg_spark_go_to_notebook, color="blue")
        context.add_datasource(data_source_name, "spark", base_directory=path)

    elif data_source_selection == "2":  # sqlalchemy
        data_source_name = click.prompt(
            msg_prompt_datasource_name, default="mydb", show_default=True)

        cli_message(msg_sqlalchemy_config_connection.format(
            data_source_name), color="blue")

        drivername = click.prompt("What is the driver for the sqlalchemy connection?", default="postgres",
                                  show_default=True)
        host = click.prompt("What is the host for the sqlalchemy connection?", default="localhost",
                            show_default=True)
        port = click.prompt("What is the port for the sqlalchemy connection?", default="5432",
                            show_default=True)
        username = click.prompt("What is the username for the sqlalchemy connection?", default="postgres",
                                show_default=True)
        password = click.prompt("What is the password for the sqlalchemy connection?", default="",
                                show_default=False, hide_input=True)
        database = click.prompt("What is the database name for the sqlalchemy connection?", default="postgres",
                                show_default=True)

        credentials = {
            "drivername": drivername,
            "host": host,
            "port": port,
            "username": username,
            "password": password,
            "database": database
        }
        context.add_profile_credentials(data_source_name, **credentials)

        cli_message(msg_sqlalchemy_go_to_notebook, color="blue")

        context.add_datasource(
            data_source_name, "sqlalchemy", profile=data_source_name)

    elif data_source_selection == "1":  # csv
        path = click.prompt(msg_prompt_filesys_enter_base_path, default='/data/', type=click.Path(exists=False,
                                                                                                  file_okay=False,
                                                                                                  dir_okay=True,
                                                                                                  readable=True),
                            show_default=True)
        if path.startswith("./"):
            path = path[2:]

        default_data_source_name = os.path.basename(path)
        data_source_name = click.prompt(
            msg_prompt_datasource_name, default=default_data_source_name, show_default=True)

        cli_message(msg_filesys_go_to_notebook, color="blue")
        context.add_datasource(data_source_name, "pandas", base_directory=path)

    else:
        cli_message(msg_unknown_data_source, color="blue")


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
