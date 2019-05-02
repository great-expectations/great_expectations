import glob
import json
import shutil
import sys
import os
import argparse
import logging

import yaml

from great_expectations import read_csv, script_relative_path
from great_expectations import __version__
from great_expectations.dataset import Dataset, PandasDataset
from great_expectations.data_asset import FileDataAsset

logger = logging.getLogger(__name__)

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
    scaffold_parser.set_defaults(func=janky_init)
    parsed_args = parser.parse_args(args)

    return parsed_args.func(parsed_args)


def safe_mmkdir(directory):
    try:
        os.mkdir(directory)
    except FileExistsError as fe:
        pass


def janky_init(parsed_args):
    project_yml_filename = ".great_expectations_project.yml"

    print('Welcome to Great Expectations! Always know what to expect from your data.')
    print('\tScaffolding project')

    _scaffold_directories_and_notebooks()

    slack_webhook = None
    access_key_id = None
    aws_secret_access_key = None

    wants_slack = input("Would you like to set up slack notifications? [Y/n] ")
    if wants_slack.lower() not in ["no", "n", "false", "f"]:
        slack_webhook = str(input("Please paste your Slack webhook url here: "))

    wants_s3 = input("Would you like to set up an S3 bucket for validation results? [Y/n]")
    if wants_s3.lower() not in ["no", "n", "false", "f"]:
        print("\nPlease note that credentials are only stored in {}): ".format(project_yml_filename))
        bucket = str(input("Which S3 bucket would you like validation results and data stored in? "))
        access_key_id = str(input("AWS access key id: "))
        aws_secret_access_key = str(input("AWS access key secret: "))

        wants_gitignore_fixed = input("Credentials stored in {}\n\nWould you like this added to your .gitignore? [Y/n] ".format(project_yml_filename))
        # TODO fix gitignore and strongly encourage them if they do it manually

    if slack_webhook or access_key_id or aws_secret_access_key:
        with open(project_yml_filename, 'w') as ff:
            ff.write(yaml.dump(
                {
                    "slack_webhook": slack_webhook,
                    "aws": {
                        "bucket": bucket,
                        "access_key_id": access_key_id,
                        "secret_access_key": aws_secret_access_key,
                    }
                }
            ))


def _scaffold_directories_and_notebooks():
    base_dir = "great_expecations"

    safe_mmkdir(base_dir)
    notebook_dir_name = "notebooks"
    notebook_directory = os.path.join(base_dir, notebook_dir_name)
    safe_mmkdir(notebook_directory)

    config_dir = os.path.join(base_dir, "dataset_expectations_configs")
    safe_mmkdir(config_dir)

    for notebook in glob.glob(script_relative_path("init_notebooks/*.ipynb")):
        notebook_name = os.path.basename(notebook)
        shutil.copyfile(notebook, os.path.join(notebook_directory, notebook_name))


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
            logger.info("Using PandasDataset to validate dataset of type %s." % expectations_config["data_asset_type"])
            dataset_class = PandasDataset
        elif expectations_config["data_asset_type"] == "FileDataAsset":
            dataset_class = FileDataAsset
        else:
            logger.critical("Unrecognized data_asset_type %s. You may need to specifcy custom_dataset_module and custom_dataset_class." % expectations_config["data_asset_type"])
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
    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return_value = dispatch(sys.argv[1:])
    sys.exit(return_value)


if __name__ == '__main__':
    main()
