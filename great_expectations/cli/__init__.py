import glob
import json
import shutil
import sys
import os
import argparse
import logging
from pyfiglet import figlet_format
import six
import click
# from clint.textui import prompt, validators
# from clint.textui import colored as clint_colored, puts, indent

try:
    from termcolor import colored
except ImportError:
    colored = None

from great_expectations import read_csv, script_relative_path
from great_expectations import __version__
from great_expectations.dataset import Dataset, PandasDataset
from great_expectations.data_asset import FileDataAsset
from great_expectations.data_context import DataContext

from .supporting_methods import (
    _scaffold_directories_and_notebooks,
    _yml_template,
)

logger = logging.getLogger(__name__)


def log_message(string, color, font="big", figlet=False):
    if colored:
        if not figlet:
            six.print_(colored(string, color))
        else:
            six.print_(colored(figlet_format(
                string, font=font), color))
    else:
        six.print_(string)


def dispatch(args):
    parser = argparse.ArgumentParser(
        description='great_expectations command-line interface')

    subparsers = parser.add_subparsers(dest='command')
    subparsers.required = True

    validate_parser = subparsers.add_parser(
        'validate', description='Validate expectations for your dataset.')
    validate_parser.set_defaults(func=validate)

    validate_parser.add_argument('dataset',
                                 help='Path to a file containing a CSV file to validate using the provided expectations_config_file.')
    validate_parser.add_argument('expectations_config_file',
                                 help='Path to a file containing a valid great_expectations expectations config to use to validate the data.')

    validate_parser.add_argument('--evaluation_parameters', '-p', default=None,
                                 help='Path to a file containing JSON object used to evaluate parameters in expectations config.')
    validate_parser.add_argument('--result_format', '-o', default="SUMMARY",
                                 help='Result format to use when building evaluation responses.')
    validate_parser.add_argument('--catch_exceptions', '-e', default=True, type=bool,
                                 help='Specify whether to catch exceptions raised during evaluation of expectations (defaults to True).')
    validate_parser.add_argument('--only_return_failures', '-f', default=False, type=bool,
                                 help='Specify whether to only return expectations that are not met during evaluation (defaults to False).')
    # validate_parser.add_argument('--no_catch_exceptions', '-e', default=True, action='store_false')
    # validate_parser.add_argument('--only_return_failures', '-f', default=False, action='store_true')
    custom_dataset_group = validate_parser.add_argument_group(
        'custom_dataset', description='Arguments defining a custom dataset to use for validation.')
    custom_dataset_group.add_argument('--custom_dataset_module', '-m', default=None,
                                      help='Path to a python module containing a custom dataset class.')
    custom_dataset_group.add_argument('--custom_dataset_class', '-c', default=None,
                                      help='Name of the custom dataset class to use during evaluation.')

    version_parser = subparsers.add_parser('version')
    version_parser.set_defaults(func=version)

    scaffold_parser = subparsers.add_parser('init')
    scaffold_parser.set_defaults(func=initialize_project)
    scaffold_parser.add_argument('--target_directory', '-d', default="./",
                                 help='The root of the project directory where you want to initialize Great Expectations.')
    parsed_args = parser.parse_args(args)

    return parsed_args.func(parsed_args)


def initialize_project(parsed_args):
    """
    This guided input walks the user through setting up a project.

    It scaffolds directories, sets up notebooks, creates a project file, and
    appends to a `.gitignore` file.
    """

    greeting_1 = """
Welcome to Great Expectations! Always know what to expect from your data.

When you develop data pipelines, ML models, ETLs and other data products, 
Great Expectations helps you express what you expect your data to look like 
(e.g., "column X should not have more than 5% null values"). 
It produces tests and documentation.

When your data product runs in production, 
Great Expectations uses the tests that you created to validate data and protect 
your code against data that it was not written to deal with.

    """

    msg_prompt_lets_begin = """
Let's add Great Expectations to your project. 
We will add great_expectations directory that will look like that: 

    great_expectations
        ├── great_expectations.yml
        ├── expectations
        ├── notebooks
        ├── plugins
        ├── uncommitted
        ├── .gitignore
    
OK to proceed?    
    """

    msg_prompt_choose_data_source = """
Time to create expectations for your data. This is done in Jupyter Notebook/Jupyter Lab. 

Before we point you to the right notebook, what data does your project work with?    
    1. Directory on local filesystem
    2. Relational database (SQL)
    3. DBT (data build tool) models
    4. None of the above
    """

    msg_prompt_dbt_choose_profile = """
Please specify the name of the dbt profile (from your ~/.dbt/profiles.yml file Great Expectations should use to connect to the database    
    """

    msg_dbt_go_to_notebook = """
To create expectations for your dbt models start Jupyter and open notebook
great_expectations/notebooks/using_great_expectations_with_dbt.ipynb - 
it will walk you through next steps. 
    """

    msg_prompt_filesys_enter_base_path = """
Enter full path of the root directory where the data files are stored        
    """

    msg_filesys_go_to_notebook = """
To create expectations for your CSV files start Jupyter and open notebook
great_expectations/notebooks/using_great_expectations_with_pandas.ipynb - 
it will walk you through configuring the database connection and next steps. 
    """

    msg_prompt_datasource_name = """
Give your new data source a short name    
    """

    msg_sqlalchemy_config_connection = """
Great Expectations relies on sqlalchemy to connect to relational databases.
Please make sure that you have it installed.         

Configure the database credentials in the "{0:s}" section of this config file: 
great_expectations/uncommitted/credentials/profiles.yml:
    
    {0:s}:
        drivername: postgres
        host: localhost 
        port: 5432
        username: postgres  
        password: ****
        database: postgres

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

    parsed_args = vars(parsed_args)
    target_directory = parsed_args['target_directory']

    project_yml_filename = os.path.join(target_directory,
                                        "great_expectations/great_expectations.yml")
    base_dir = os.path.join(target_directory, "great_expectations")
    sql_alchemy_profile = None
    dbt_profile = None

    log_message("Great Expectations", color="cyan", figlet=True)

    log_message(greeting_1, color="blue")

    if not click.confirm(msg_prompt_lets_begin, default=True):
        log_message(
            "OK - run great_expectations init again when ready. Exiting...", color="blue")
        exit(0)

    _scaffold_directories_and_notebooks(base_dir)
    template_args = {}
    with open(project_yml_filename, 'w') as ff:
        ff.write(_yml_template(**template_args))
    log_message(
        "\nDone. Later you can check out great_expectations/great_expectations.yml config file for useful options.", color="blue")

    context = DataContext('.')

    # Shows a list of options to select from

    data_source_selection = click.prompt(msg_prompt_choose_data_source, type=click.Choice(["1", "2", "3", "4"]), show_choices=False)

    print(data_source_selection)

    if data_source_selection == "3": # dbt
        dbt_profile = click.prompt(msg_prompt_dbt_choose_profile)
        log_message(msg_dbt_go_to_notebook, color="blue")
        context.add_datasource("dbt", "dbt", profile_name=dbt_profile)

    elif data_source_selection == "2": #sqlalchemy
        data_source_name = click.prompt(msg_prompt_datasource_name, default="mydb", show_default=True)

        log_message(msg_sqlalchemy_config_connection.format(data_source_name), color="blue")

        credentials = {
            "drivername": "postgres",
            "host": "localhost",
            "port": 5432,
            "username": "postgres",
            "password": "****",
            "database": "postgres"
        }
        context.add_profile_credentials(data_source_name, **credentials)
        context.add_datasource(data_source_name, "sqlalchemy", profile_name=data_source_name)


    elif data_source_selection == "1": # csv
        path = click.prompt(msg_prompt_filesys_enter_base_path, default='/data/', type=click.Path(exists=True, file_okay=False, dir_okay=True, readable=True), show_default=True)

        default_data_source_name = os.path.basename(path)
        data_source_name = click.prompt(msg_prompt_datasource_name, default=default_data_source_name, show_default=True)

        log_message(msg_filesys_go_to_notebook, color="blue")
        context.add_datasource(data_source_name, "pandas", base_directory=path)

    else:
        log_message(msg_unknown_data_source, color="blue")

    template_args = {}
    if dbt_profile:
        template_args["dbt_profile"] = dbt_profile
    if sql_alchemy_profile:
        template_args["sql_alchemy_profile"] = sql_alchemy_profile

    # with open(project_yml_filename, 'w') as ff:
    #     ff.write(_yml_template(**template_args))



    # path = prompt.query(str(clint_colored.yellow('Installation Path')), default='/usr/local/bin/', validators=[validators.PathValidator()])

    # slack_webhook = None
    # bucket = None
    #
    # if _does_user_want(input("Would you like to set up slack notifications? [Y/n] ")):
    #     slack_webhook = str(input("Please paste your Slack webhook url here: "))
    #
    # if _does_user_want(input("Would you like to set up an S3 bucket for validation results? [Y/n] ")):
    #     bucket = str(input("Which S3 bucket would you like validation results and data stored in? "))
    #
    # _save_append_line_to_gitignore("# These entries were added by Great Expectations")
    # for directory in ["validations", "snapshots", "samples"]:
    #     _save_append_line_to_gitignore(base_dir + "/" + directory)
    #
    # if slack_webhook or bucket:
    #     if _does_user_want(input("Would you to add {} to a .gitignore? [Y/n] ".format(project_yml_filename))):
    #         _save_append_line_to_gitignore(project_yml_filename)
    #     else:
    #         print("""⚠️   Warning! You have elected to skip adding entries to your .gitignore.
    # This is NOT recommended as it may contain secrets. Do not commit this to source control!""".format(project_yml_filename))
    #
    # # if slack_webhook or bucket:
    # #     # TODO fail if a project file already exists
    # with open(project_yml_filename, 'w') as ff:
    #     ff.write(_yml_template(bucket, slack_webhook))


def validate(parsed_args):
    """
    Read a dataset file and validate it using a config saved in another file. Uses parameters defined in the dispatch
    method.

    :param parsed_args: A Namespace object containing parsed arguments from the dispatch method.
    :return: The number of unsucessful expectations
    """
    parsed_args = vars(parsed_args)
    data_set = parsed_args['dataset']
    expectations_config_file = parsed_args['expectations_config_file']

    expectations_config = json.load(open(expectations_config_file))

    if parsed_args["evaluation_parameters"] is not None:
        evaluation_parameters = json.load(
            open(parsed_args["evaluation_parameters"]))
    else:
        evaluation_parameters = None

    # Use a custom dataasset module and class if provided. Otherwise infer from the config.
    if parsed_args["custom_dataset_module"]:
        sys.path.insert(0, os.path.dirname(
            parsed_args["custom_dataset_module"]))
        module_name = os.path.basename(
            parsed_args["custom_dataset_module"]).split('.')[0]
        custom_module = __import__(module_name)
        dataset_class = getattr(
            custom_module, parsed_args["custom_dataset_class"])
    elif "data_asset_type" in expectations_config:
        if expectations_config["data_asset_type"] == "Dataset" or expectations_config["data_asset_type"] == "PandasDataset":
            dataset_class = PandasDataset
        elif expectations_config["data_asset_type"].endswith("Dataset"):
            logger.info("Using PandasDataset to validate dataset of type %s." %
                        expectations_config["data_asset_type"])
            dataset_class = PandasDataset
        elif expectations_config["data_asset_type"] == "FileDataAsset":
            dataset_class = FileDataAsset
        else:
            logger.critical("Unrecognized data_asset_type %s. You may need to specifcy custom_dataset_module and custom_dataset_class." %
                            expectations_config["data_asset_type"])
            return -1
    else:
        dataset_class = PandasDataset

    if issubclass(dataset_class, Dataset):
        da = read_csv(data_set, expectations_config=expectations_config,
                      dataset_class=dataset_class)
    else:
        da = dataset_class(data_set, config=expectations_config)

    result = da.validate(
        evaluation_parameters=evaluation_parameters,
        result_format=parsed_args["result_format"],
        catch_exceptions=parsed_args["catch_exceptions"],
        only_return_failures=parsed_args["only_return_failures"],
    )

    print(json.dumps(result, indent=2))
    return result['statistics']['unsuccessful_expectations']


def version(parsed_args):
    """
    Print the currently-running version of great expectations
    """
    print(__version__)


def main():
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return_value = dispatch(sys.argv[1:])
    sys.exit(return_value)


if __name__ == '__main__':
    main()
