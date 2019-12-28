import json
import os
import sys

import click

from great_expectations import read_csv
from great_expectations.cli.util import cli_message
from great_expectations.core import (
    expectationSuiteSchema,
    expectationSuiteValidationResultSchema,
)
from great_expectations.data_asset import FileDataAsset
from great_expectations.dataset import PandasDataset, Dataset
from great_expectations.cli.logging import logger


@click.group()
def validation():
    """validation operations"""
    pass


@validation.command(name="csv")
@click.argument("dataset")
@click.argument("expectation_suite_file")
@click.option(
    "--evaluation_parameters",
    "-p",
    default=None,
    help="Path to a file containing JSON object used to evaluate parameters in expectations config.",
)
@click.option(
    "--result_format",
    "-o",
    default="SUMMARY",
    help="Result format to use when building evaluation responses.",
)
@click.option(
    "--catch_exceptions",
    "-e",
    default=True,
    type=bool,
    help="Specify whether to catch exceptions raised during evaluation of expectations (defaults to True).",
)
@click.option(
    "--only_return_failures",
    "-f",
    default=False,
    type=bool,
    help="Specify whether to only return expectations that are not met during evaluation "
    "(defaults to False).",
)
@click.option(
    "--custom_dataset_module",
    "-m",
    default=None,
    help="Path to a python module containing a custom dataset class.",
)
@click.option(
    "--custom_dataset_class",
    "-c",
    default=None,
    help="Name of the custom dataset class to use during evaluation.",
)
def validation_csv(
    dataset,
    expectation_suite_file,
    evaluation_parameters,
    result_format,
    catch_exceptions,
    only_return_failures,
    custom_dataset_module,
    custom_dataset_class,
):
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
    with open(expectation_suite_file, "r") as infile:
        expectation_suite = expectationSuiteSchema.load(json.load(infile)).data

    if evaluation_parameters is not None:
        evaluation_parameters = json.load(open(evaluation_parameters, "r"))

    # Use a custom data_asset module and class if provided. Otherwise infer from the expectation suite
    if custom_dataset_module:
        sys.path.insert(0, os.path.dirname(custom_dataset_module))
        module_name = os.path.basename(custom_dataset_module).split(".")[0]
        custom_module = __import__(str(module_name))
        dataset_class = getattr(custom_module, custom_dataset_class)
    elif expectation_suite.data_asset_type is not None:
        if (
            expectation_suite.data_asset_type == "Dataset"
            or expectation_suite.data_asset_type == "PandasDataset"
        ):
            dataset_class = PandasDataset
        elif expectation_suite.data_asset_type.endswith("Dataset"):
            logger.info(
                "Using PandasDataset to validate dataset of type %s."
                % expectation_suite.data_asset_type
            )
            dataset_class = PandasDataset
        elif expectation_suite.data_asset_type == "FileDataAsset":
            dataset_class = FileDataAsset
        else:
            cli_message(
                "Unrecognized data_asset_type %s. You may need to specify "
                "custom_dataset_module and custom_dataset_class."
                % expectation_suite.data_asset_type
            )
            return -1
    else:
        dataset_class = PandasDataset

    if issubclass(dataset_class, Dataset):
        da = read_csv(
            dataset, expectation_suite=expectation_suite, dataset_class=dataset_class
        )
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
    print(
        json.dumps(expectationSuiteValidationResultSchema.dump(result).data, indent=2)
    )
    sys.exit(result["statistics"]["unsuccessful_expectations"])
