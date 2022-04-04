import json
import os

import pytest
from nbformat.notebooknode import NotebookNode

from great_expectations import DataContext

# noinspection PyProtectedMember
from great_expectations.cli.suite import _suite_edit_workflow
from great_expectations.core import ExpectationSuiteValidationResult
from great_expectations.core.batch import BatchRequest
from great_expectations.core.expectation_suite import (
    ExpectationSuite,
    ExpectationSuiteSchema,
)
from great_expectations.core.usage_statistics.anonymizers.types.base import (
    CLISuiteInteractiveFlagCombinations,
)
from great_expectations.exceptions import (
    InvalidExpectationConfigurationError,
    SuiteEditNotebookCustomTemplateModuleNotFoundError,
)
from great_expectations.render.renderer.v3.suite_edit_notebook_renderer import (
    SuiteEditNotebookRenderer,
)
from great_expectations.validator.validator import Validator
from tests.render.test_util import run_notebook


@pytest.fixture
def critical_suite_with_citations(empty_data_context) -> ExpectationSuite:
    """
    This hand made fixture has a wide range of expectations, and has a mix of
    metadata including an BasicSuiteBuilderProfiler entry, and citations.
    """
    context: DataContext = empty_data_context
    schema: ExpectationSuiteSchema = ExpectationSuiteSchema()
    critical_suite: dict = {
        "expectation_suite_name": "critical",
        "meta": {
            "great_expectations_version": "0.13.15+7252.g32fa98e2a.dirty",
            "columns": {
                "npi": {"description": ""},
                "nppes_provider_last_org_name": {"description": ""},
                "nppes_provider_first_name": {"description": ""},
                "nppes_provider_mi": {"description": ""},
                "nppes_credentials": {"description": ""},
                "nppes_provider_gender": {"description": ""},
                "nppes_entity_code": {"description": ""},
                "nppes_provider_street1": {"description": ""},
                "nppes_provider_street2": {"description": ""},
                "nppes_provider_city": {"description": ""},
            },
            "citations": [
                {
                    "citation_date": "2020-02-28T17:34:31.307271",
                    "batch_request": {
                        "datasource_name": "files_datasource",
                        "data_connector_name": "files_data_connector",
                        "data_asset_name": "10k",
                    },
                    "batch_markers": {
                        "ge_load_time": "20200229T013430.655026Z",
                        "pandas_data_fingerprint": "f6037d92eb4c01f976513bc0aec2420d",
                    },
                    "batch_parameters": None,
                    "comment": "BasicSuiteBuilderProfiler added a citation based on the current batch.",
                }
            ],
            "notes": {
                "format": "markdown",
                "content": [
                    "#### This is an _example_ suite\n\n- This suite was made by quickly glancing at 1000 rows of your data.\n- This is **not a production suite**. It is meant to show examples of expectations.\n- Because this suite was auto-generated using a very basic profiler that does not know your data like you do, many of the expectations may not be meaningful.\n"
                ],
            },
            "BasicSuiteBuilderProfiler": {
                "created_by": "BasicSuiteBuilderProfiler",
                "created_at": 1582838223.843476,
                "batch_request": {
                    "datasource_name": "files_datasource",
                    "data_connector_name": "files_data_connector",
                    "data_asset_name": "10k",
                },
            },
        },
        "expectations": [
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "npi"},
                "meta": {
                    "question": True,
                    "Notes": "There are empty strings that should probably be nulls",
                    "BasicSuiteBuilderProfiler": {"confidence": "very low"},
                },
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "provider_type"},
            },
        ],
        "data_asset_type": "Dataset",
    }
    expectation_suite_dict: dict = schema.loads(json.dumps(critical_suite))
    return ExpectationSuite(**expectation_suite_dict, data_context=context)


@pytest.fixture
def suite_with_multiple_citations(empty_data_context) -> ExpectationSuite:
    """
    A handmade suite with multiple citations each with different batch_request.

    The most recent citation does not have batch_request
    """
    context: DataContext = empty_data_context
    schema: ExpectationSuiteSchema = ExpectationSuiteSchema()
    critical_suite: dict = {
        "expectation_suite_name": "critical",
        "meta": {
            "great_expectations_version": "0.13.15+7252.g32fa98e2a.dirty",
            "citations": [
                {
                    "citation_date": "2000-01-01T00:00:01.000001",
                    "batch_request": {
                        "datasource_name": "files_datasource",
                        "data_connector_name": "files_data_connector",
                        "data_asset_name": "1k",
                    },
                },
                # This citation is the most recent and has no batch_request
                {
                    "citation_date": "2020-01-01T00:00:01.000001",
                },
                {
                    "citation_date": "1999-01-01T00:00:01.000001",
                    "batch_request": {
                        "datasource_name": "files_datasource",
                        "data_connector_name": "files_data_connector",
                        "data_asset_name": "2k",
                    },
                },
            ],
        },
        "expectations": [
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "npi"},
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "provider_type"},
            },
        ],
        "data_asset_type": "Dataset",
    }
    expectation_suite_dict: dict = schema.loads(json.dumps(critical_suite))
    return ExpectationSuite(**expectation_suite_dict, data_context=context)


@pytest.fixture
def warning_suite(empty_data_context) -> ExpectationSuite:
    """
    This hand made fixture has a wide range of expectations, and has a mix of
    metadata including BasicSuiteBuilderProfiler entries.
    """
    context: DataContext = empty_data_context
    schema: ExpectationSuiteSchema = ExpectationSuiteSchema()
    warning_suite: dict = {
        "expectation_suite_name": "warning",
        "meta": {
            "great_expectations_version": "0.13.15+7252.g32fa98e2a.dirty",
            "citations": [
                {
                    "citation_date": "2020-02-28T17:34:31.307271",
                    "batch_request": {
                        "datasource_name": "files_datasource",
                        "data_connector_name": "files_data_connector",
                        "data_asset_name": "10k",
                    },
                    "batch_markers": {
                        "ge_load_time": "20200229T013430.655026Z",
                        "pandas_data_fingerprint": "f6037d92eb4c01f976513bc0aec2420d",
                    },
                    "batch_parameters": None,
                    "comment": "BasicSuiteBuilderProfiler added a citation based on the current batch.",
                }
            ],
        },
        "expectations": [
            {
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": {"min_value": 800000, "max_value": 1200000},
            },
            {
                "expectation_type": "expect_table_column_count_to_equal",
                "kwargs": {"value": 71},
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "npi"},
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "provider_type"},
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "nppes_provider_last_org_name"},
            },
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {
                    "column": "nppes_provider_gender",
                    "value_set": ["M", "F", ""],
                },
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "nppes_entity_code"},
            },
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {"column": "nppes_entity_code", "value_set": ["I", "O"]},
            },
            {
                "expectation_type": "expect_column_kl_divergence_to_be_less_than",
                "kwargs": {
                    "column": "nppes_entity_code",
                    "partition_object": {
                        "values": ["I", "O"],
                        "weights": [0.9431769750233306, 0.056823024976669335],
                    },
                    "threshold": 0.1,
                },
            },
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {
                    "column": "nppes_provider_state",
                    "value_set": [
                        "AL",
                        "AK",
                        "AZ",
                        "AR",
                        "CA",
                        "CO",
                        "CT",
                        "DE",
                        "FL",
                        "GA",
                        "HI",
                        "ID",
                        "IL",
                        "IN",
                        "IA",
                        "KS",
                        "KY",
                        "LA",
                        "ME",
                        "MD",
                        "MA",
                        "MI",
                        "MN",
                        "MS",
                        "MO",
                        "MT",
                        "NE",
                        "NV",
                        "NH",
                        "NJ",
                        "NM",
                        "NY",
                        "NC",
                        "ND",
                        "OH",
                        "OK",
                        "OR",
                        "PA",
                        "RI",
                        "SC",
                        "SD",
                        "TN",
                        "TX",
                        "UT",
                        "VT",
                        "VA",
                        "WA",
                        "WV",
                        "WI",
                        "WY",
                        "DC",
                        "PR",
                        "AE",
                        "VI",
                    ],
                    "mostly": 0.999,
                },
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "medicare_participation_indicator"},
            },
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {
                    "column": "medicare_participation_indicator",
                    "value_set": ["Y", "N"],
                },
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "number_of_hcpcs"},
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "number_of_hcpcs",
                    "min_value": 0,
                    "max_value": 500,
                    "mostly": 0.999,
                },
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "total_unique_benes"},
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "total_unique_benes",
                    "min_value": 0,
                    "max_value": 2000,
                    "mostly": 0.95,
                },
            },
            {
                "expectation_type": "expect_column_values_to_be_null",
                "kwargs": {"column": "med_suppress_indicator", "mostly": 0.85},
            },
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {"column": "med_suppress_indicator", "value_set": ["#", "*"]},
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "beneficiary_average_age",
                    "min_value": 40,
                    "max_value": 90,
                    "mostly": 0.995,
                },
            },
            {
                "expectation_type": "expect_column_kl_divergence_to_be_less_than",
                "kwargs": {
                    "column": "beneficiary_average_age",
                    "partition_object": {
                        "bins": [8, 16.5, 25, 33.5, 42, 50.5, 59, 67.5, 76, 84.5, 93],
                        "weights": [
                            0.00025259576594384474,
                            0.00013318685840675451,
                            0.0009653750909344757,
                            0.0012363414580378728,
                            0.01081660996274442,
                            0.030813927854975127,
                            0.13495227317818748,
                            0.6919590041664524,
                            0.1244213260634741,
                            0.004449359600843578,
                        ],
                    },
                    "threshold": 0.9,
                },
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "total_submitted_chrg_amt",
                    "min_value": 2000,
                    "max_value": 5000000,
                    "mostly": 0.98,
                },
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "nppes_provider_first_name", "mostly": 0.9},
            },
            {
                "expectation_type": "expect_column_values_to_match_regex",
                "kwargs": {
                    "column": "nppes_provider_zip",
                    "regex": "^\\d*$",
                    "mostly": 0.999,
                },
            },
        ],
        "data_asset_type": "Dataset",
    }
    expectation_suite_dict: dict = schema.loads(json.dumps(warning_suite))
    return ExpectationSuite(**expectation_suite_dict, data_context=context)


def test_render_with_no_column_cells_without_batch_request(
    critical_suite_with_citations, empty_data_context
):
    critical_suite_with_citations.expectations = []
    obs: dict = SuiteEditNotebookRenderer.from_data_context(
        data_context=empty_data_context
    ).render(suite=critical_suite_with_citations)
    assert isinstance(obs, dict)
    expected = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {},
        "cells": [
            {
                "id": "smooth-texture",
                "cell_type": "markdown",
                "source": "# Edit Your Expectation Suite\nUse this notebook to recreate and modify your expectation suite:\n\n**Expectation Suite Name**: `critical`\n",
                "metadata": {},
            },
            {
                "id": "genetic-canada",
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'import datetime\n\nimport pandas as pd\n\nimport great_expectations as ge\nimport great_expectations.jupyter_ux\nfrom great_expectations.core.expectation_configuration import ExpectationConfiguration\nfrom great_expectations.data_context.types.resource_identifiers import (\n    ExpectationSuiteIdentifier,\n)\nfrom great_expectations.exceptions import DataContextError\n\ncontext = ge.data_context.DataContext()\n\n\n# Feel free to change the name of your suite here. Renaming this will not remove the other one.\nexpectation_suite_name = "critical"\ntry:\n    suite = context.get_expectation_suite(expectation_suite_name=expectation_suite_name)\n    print(\n        f\'Loaded ExpectationSuite "{suite.expectation_suite_name}" containing {len(suite.expectations)} expectations.\'\n    )\nexcept DataContextError:\n    suite = context.create_expectation_suite(\n        expectation_suite_name=expectation_suite_name\n    )\n    print(f\'Created ExpectationSuite "{suite.expectation_suite_name}".\')',
                "outputs": [],
            },
            {
                "id": "strategic-exhibit",
                "cell_type": "markdown",
                "source": "## Create & Edit Expectations\n\n\nYou are adding Expectation configurations to the suite. Since you selected manual mode, there is no sample batch of data and no validation happens during this process. See our documentation for more info and examples: **[How to create a new Expectation Suite without a sample batch](https://docs.greatexpectations.io/docs/guides/expectations/how_to_create_and_edit_expectations_based_on_domain_knowledge_without_inspecting_data_directly)**.\n\nNote that if you do use interactive mode you may specify a sample batch of data to use when creating your Expectation Suite. You can then use a `validator` to get immediate feedback on your Expectations against your specified sample batch.\n\n\nYou can see all the available expectations in the **[expectation gallery](https://greatexpectations.io/expectations)**.",
                "metadata": {},
            },
            {
                "id": "interracial-integration",
                "cell_type": "markdown",
                "source": "### Table Expectation(s)",
                "metadata": {},
            },
            {
                "id": "original-punch",
                "cell_type": "markdown",
                "source": "No table level expectations are in this suite. Feel free to add some here.\n",
                "metadata": {},
            },
            {
                "id": "occasional-honolulu",
                "cell_type": "markdown",
                "source": "### Column Expectation(s)",
                "metadata": {},
            },
            {
                "id": "removed-failing",
                "cell_type": "markdown",
                "source": "No column level expectations are in this suite. Feel free to add some here.\n",
                "metadata": {},
            },
            {
                "id": "bigger-clone",
                "cell_type": "markdown",
                "source": "## Review & Save Your Expectations\n\nLet's save the expectation suite as a JSON file in the `great_expectations/expectations` directory of your project.\n\nLet's now rebuild your Data Docs, which helps you communicate about your data with both machines and humans.",
                "metadata": {},
            },
            {
                "id": "broad-weekend",
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "print(context.get_expectation_suite(expectation_suite_name=expectation_suite_name))\ncontext.save_expectation_suite(expectation_suite=suite, expectation_suite_name=expectation_suite_name)\n\nsuite_identifier = ExpectationSuiteIdentifier(expectation_suite_name=expectation_suite_name)\ncontext.build_data_docs(resource_identifiers=[suite_identifier])\ncontext.open_data_docs(resource_identifier=suite_identifier)",
                "outputs": [],
            },
        ],
    }

    del expected["nbformat_minor"]
    del obs["nbformat_minor"]
    for obs_cell, expected_cell in zip(obs["cells"], expected["cells"]):
        obs_cell.pop("id", None)
        expected_cell.pop("id", None)
        assert obs_cell == expected_cell
    assert obs == expected


def test_complex_suite_with_batch_request(warning_suite, empty_data_context):
    batch_request: dict = {
        "datasource_name": "files_datasource",
        "data_connector_name": "files_data_connector",
        "data_asset_name": "1k",
    }
    obs: NotebookNode = SuiteEditNotebookRenderer.from_data_context(
        data_context=empty_data_context
    ).render(suite=warning_suite, batch_request=batch_request)
    assert isinstance(obs, dict)
    expected: dict = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {},
        "cells": [
            {
                "id": "configured-swing",
                "cell_type": "markdown",
                "source": "# Edit Your Expectation Suite\nUse this notebook to recreate and modify your expectation suite:\n\n**Expectation Suite Name**: `warning`\n",
                "metadata": {},
            },
            {
                "id": "proof-employee",
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'import datetime\n\nimport pandas as pd\n\nimport great_expectations as ge\nimport great_expectations.jupyter_ux\nfrom great_expectations.core.batch import BatchRequest\nfrom great_expectations.checkpoint import SimpleCheckpoint\nfrom great_expectations.exceptions import DataContextError\n\ncontext = ge.data_context.DataContext()\n\n# Note that if you modify this batch request, you may save the new version as a .json file\n#  to pass in later via the --batch-request option\n# "limit" will return the first n batches if n is positive, or the most recent n batches if n is negative\nbatch_request = {\n    "datasource_name": "files_datasource",\n    "data_connector_name": "files_data_connector",\n    "data_asset_name": "1k",\n}\n\n\n# Feel free to change the name of your suite here. Renaming this will not remove the other one.\nexpectation_suite_name = "warning"\ntry:\n    suite = context.get_expectation_suite(expectation_suite_name=expectation_suite_name)\n    print(\n        f\'Loaded ExpectationSuite "{suite.expectation_suite_name}" containing {len(suite.expectations)} expectations.\'\n    )\nexcept DataContextError:\n    suite = context.create_expectation_suite(\n        expectation_suite_name=expectation_suite_name\n    )\n    print(f\'Created ExpectationSuite "{suite.expectation_suite_name}".\')\n\n\nvalidator = context.get_validator(\n    batch_request=BatchRequest(**batch_request),\n    expectation_suite_name=expectation_suite_name,\n)\ncolumn_names = [f\'"{column_name}"\' for column_name in validator.columns()]\nprint(f"Columns: {\', \'.join(column_names)}.")\nvalidator.head(n_rows=5, fetch_all=False)',
                "outputs": [],
            },
            {
                "id": "banned-television",
                "cell_type": "markdown",
                "source": "## Create & Edit Expectations\n\n\nAdd expectations by calling specific expectation methods on the `validator` object. They all begin with `.expect_` which makes autocompleting easy using tab.\n\nBecause you selected interactive mode, you are now creating or editing an Expectation Suite with validator feedback from the sample batch of data that you specified (see `batch_request`).\n\nNote that if you select manual mode you may still create or edit an Expectation Suite directly, without feedback from the `validator`. See our documentation for more info and examples: [How to create a new Expectation Suite without a sample batch](https://docs.greatexpectations.io/docs/guides/expectations/how_to_create_and_edit_expectations_based_on_domain_knowledge_without_inspecting_data_directly).\n\n\n\nYou can see all the available expectations in the **[expectation gallery](https://greatexpectations.io/expectations)**.",
                "metadata": {},
            },
            {
                "id": "offensive-rings",
                "cell_type": "markdown",
                "source": "### Table Expectation(s)",
                "metadata": {},
            },
            {
                "id": "stopped-roulette",
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "validator.expect_table_row_count_to_be_between(min_value=800000, max_value=1200000)",
                "outputs": [],
            },
            {
                "id": "extra-decision",
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "validator.expect_table_column_count_to_equal(value=71)",
                "outputs": [],
            },
            {
                "id": "offshore-crown",
                "cell_type": "markdown",
                "source": "### Column Expectation(s)",
                "metadata": {},
            },
            {
                "id": "hollywood-harrison",
                "cell_type": "markdown",
                "source": "#### `npi`",
                "metadata": {},
            },
            {
                "id": "sufficient-address",
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'validator.expect_column_values_to_not_be_null(column="npi")',
                "outputs": [],
            },
            {
                "id": "lightweight-tomato",
                "cell_type": "markdown",
                "source": "#### `provider_type`",
                "metadata": {},
            },
            {
                "id": "advanced-chorus",
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'validator.expect_column_values_to_not_be_null(column="provider_type")',
                "outputs": [],
            },
            {
                "id": "otherwise-creation",
                "cell_type": "markdown",
                "source": "#### `nppes_provider_last_org_name`",
                "metadata": {},
            },
            {
                "id": "infrared-exception",
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'validator.expect_column_values_to_not_be_null(column="nppes_provider_last_org_name")',
                "outputs": [],
            },
            {
                "id": "judicial-highland",
                "cell_type": "markdown",
                "source": "#### `nppes_provider_gender`",
                "metadata": {},
            },
            {
                "id": "linear-portrait",
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'validator.expect_column_values_to_be_in_set(\n    column="nppes_provider_gender", value_set=["M", "F", ""]\n)',
                "outputs": [],
            },
            {
                "id": "senior-beatles",
                "cell_type": "markdown",
                "source": "#### `nppes_entity_code`",
                "metadata": {},
            },
            {
                "id": "genetic-vaccine",
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'validator.expect_column_values_to_not_be_null(column="nppes_entity_code")',
                "outputs": [],
            },
            {
                "id": "stable-budget",
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'validator.expect_column_values_to_be_in_set(\n    column="nppes_entity_code", value_set=["I", "O"]\n)',
                "outputs": [],
            },
            {
                "id": "prescribed-discretion",
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'validator.expect_column_kl_divergence_to_be_less_than(\n    column="nppes_entity_code",\n    partition_object={\n        "values": ["I", "O"],\n        "weights": [0.9431769750233306, 0.056823024976669335],\n    },\n    threshold=0.1,\n)',
                "outputs": [],
            },
            {
                "id": "moral-poison",
                "cell_type": "markdown",
                "source": "#### `nppes_provider_state`",
                "metadata": {},
            },
            {
                "id": "solid-duplicate",
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'validator.expect_column_values_to_be_in_set(\n    column="nppes_provider_state",\n    value_set=[\n        "AL",\n        "AK",\n        "AZ",\n        "AR",\n        "CA",\n        "CO",\n        "CT",\n        "DE",\n        "FL",\n        "GA",\n        "HI",\n        "ID",\n        "IL",\n        "IN",\n        "IA",\n        "KS",\n        "KY",\n        "LA",\n        "ME",\n        "MD",\n        "MA",\n        "MI",\n        "MN",\n        "MS",\n        "MO",\n        "MT",\n        "NE",\n        "NV",\n        "NH",\n        "NJ",\n        "NM",\n        "NY",\n        "NC",\n        "ND",\n        "OH",\n        "OK",\n        "OR",\n        "PA",\n        "RI",\n        "SC",\n        "SD",\n        "TN",\n        "TX",\n        "UT",\n        "VT",\n        "VA",\n        "WA",\n        "WV",\n        "WI",\n        "WY",\n        "DC",\n        "PR",\n        "AE",\n        "VI",\n    ],\n    mostly=0.999,\n)',
                "outputs": [],
            },
            {
                "id": "dried-module",
                "cell_type": "markdown",
                "source": "#### `medicare_participation_indicator`",
                "metadata": {},
            },
            {
                "id": "magnetic-inquiry",
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'validator.expect_column_values_to_not_be_null(column="medicare_participation_indicator")',
                "outputs": [],
            },
            {
                "id": "rough-composer",
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'validator.expect_column_values_to_be_in_set(\n    column="medicare_participation_indicator", value_set=["Y", "N"]\n)',
                "outputs": [],
            },
            {
                "id": "external-virginia",
                "cell_type": "markdown",
                "source": "#### `number_of_hcpcs`",
                "metadata": {},
            },
            {
                "id": "improving-interim",
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'validator.expect_column_values_to_not_be_null(column="number_of_hcpcs")',
                "outputs": [],
            },
            {
                "id": "spiritual-victor",
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'validator.expect_column_values_to_be_between(\n    column="number_of_hcpcs", min_value=0, max_value=500, mostly=0.999\n)',
                "outputs": [],
            },
            {
                "id": "elegant-gates",
                "cell_type": "markdown",
                "source": "#### `total_unique_benes`",
                "metadata": {},
            },
            {
                "id": "demonstrated-variety",
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'validator.expect_column_values_to_not_be_null(column="total_unique_benes")',
                "outputs": [],
            },
            {
                "id": "increasing-newton",
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'validator.expect_column_values_to_be_between(\n    column="total_unique_benes", min_value=0, max_value=2000, mostly=0.95\n)',
                "outputs": [],
            },
            {
                "id": "waiting-graham",
                "cell_type": "markdown",
                "source": "#### `med_suppress_indicator`",
                "metadata": {},
            },
            {
                "id": "willing-adrian",
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'validator.expect_column_values_to_be_null(column="med_suppress_indicator", mostly=0.85)',
                "outputs": [],
            },
            {
                "id": "framed-above",
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'validator.expect_column_values_to_be_in_set(\n    column="med_suppress_indicator", value_set=["#", "*"]\n)',
                "outputs": [],
            },
            {
                "id": "fancy-voltage",
                "cell_type": "markdown",
                "source": "#### `beneficiary_average_age`",
                "metadata": {},
            },
            {
                "id": "accomplished-league",
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'validator.expect_column_values_to_be_between(\n    column="beneficiary_average_age", min_value=40, max_value=90, mostly=0.995\n)',
                "outputs": [],
            },
            {
                "id": "bronze-march",
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'validator.expect_column_kl_divergence_to_be_less_than(\n    column="beneficiary_average_age",\n    partition_object={\n        "bins": [8, 16.5, 25, 33.5, 42, 50.5, 59, 67.5, 76, 84.5, 93],\n        "weights": [\n            0.00025259576594384474,\n            0.00013318685840675451,\n            0.0009653750909344757,\n            0.0012363414580378728,\n            0.01081660996274442,\n            0.030813927854975127,\n            0.13495227317818748,\n            0.6919590041664524,\n            0.1244213260634741,\n            0.004449359600843578,\n        ],\n    },\n    threshold=0.9,\n)',
                "outputs": [],
            },
            {
                "id": "interim-liquid",
                "cell_type": "markdown",
                "source": "#### `total_submitted_chrg_amt`",
                "metadata": {},
            },
            {
                "id": "assured-cannon",
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'validator.expect_column_values_to_be_between(\n    column="total_submitted_chrg_amt", min_value=2000, max_value=5000000, mostly=0.98\n)',
                "outputs": [],
            },
            {
                "id": "regular-corrections",
                "cell_type": "markdown",
                "source": "#### `nppes_provider_first_name`",
                "metadata": {},
            },
            {
                "id": "sensitive-landing",
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'validator.expect_column_values_to_not_be_null(\n    column="nppes_provider_first_name", mostly=0.9\n)',
                "outputs": [],
            },
            {
                "id": "inappropriate-shift",
                "cell_type": "markdown",
                "source": "#### `nppes_provider_zip`",
                "metadata": {},
            },
            {
                "id": "worthy-rainbow",
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'validator.expect_column_values_to_match_regex(\n    column="nppes_provider_zip", regex="^\\d*$", mostly=0.999\n)',
                "outputs": [],
            },
            {
                "id": "provincial-termination",
                "cell_type": "markdown",
                "source": "## Review & Save Your Expectations\n\nLet's save the expectation suite as a JSON file in the `great_expectations/expectations` directory of your project.\n\nLet's now rebuild your Data Docs, which helps you communicate about your data with both machines and humans.",
                "metadata": {},
            },
            {
                "id": "exposed-cement",
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'print(validator.get_expectation_suite(discard_failed_expectations=False))\nvalidator.save_expectation_suite(discard_failed_expectations=False)\n\ncheckpoint_config = {\n    "class_name": "SimpleCheckpoint",\n    "validations": [\n        {\n            "batch_request": batch_request,\n            "expectation_suite_name": expectation_suite_name\n        }\n    ]\n}\ncheckpoint = SimpleCheckpoint(\n    f"_tmp_checkpoint_{expectation_suite_name}",\n    context,\n    **checkpoint_config\n)\ncheckpoint_result = checkpoint.run()\n\ncontext.build_data_docs()\n\nvalidation_result_identifier = checkpoint_result.list_validation_result_identifiers()[0]\ncontext.open_data_docs(resource_identifier=validation_result_identifier)',
                "outputs": [],
            },
        ],
    }

    del expected["nbformat_minor"]
    del obs["nbformat_minor"]
    for obs_cell, expected_cell in zip(obs["cells"], expected["cells"]):
        obs_cell.pop("id", None)
        expected_cell.pop("id", None)
        assert obs_cell == expected_cell
    assert obs == expected


def test_notebook_execution_with_pandas_backend(
    titanic_v013_multi_datasource_pandas_data_context_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    """
    To set this test up we:

    - create a suite
    - add a few expectations (both table and column level)
    - verify that no validations have happened
    - create the suite edit notebook by hijacking the private cli method

    We then:
    - execute that notebook (Note this will raise various errors like
    CellExecutionError if any cell in the notebook fails
    - create a new context from disk
    - verify that a validation has been run with our expectation suite
    """
    # Since we'll run the notebook, we use a context with no data docs to avoid the renderer's default
    # behavior of building and opening docs, which is not part of this test.
    context: DataContext = titanic_v013_multi_datasource_pandas_data_context_with_checkpoints_v1_with_empty_store_stats_enabled
    root_dir: str = context.root_directory
    uncommitted_dir: str = os.path.join(root_dir, "uncommitted")
    expectation_suite_name: str = "warning"

    context.create_expectation_suite(expectation_suite_name=expectation_suite_name)
    batch_request: dict = {
        "datasource_name": "my_datasource",
        "data_connector_name": "my_basic_data_connector",
        "data_asset_name": "Titanic_1912",
    }
    validator: Validator = context.get_validator(
        batch_request=BatchRequest(**batch_request),
        expectation_suite_name=expectation_suite_name,
    )
    validator.expect_table_column_count_to_equal(1)
    validator.expect_table_row_count_to_equal(1313)
    validator.expect_column_values_to_be_in_set("Sex", ["female", "male"])
    validator.save_expectation_suite(discard_failed_expectations=False)

    # Sanity check test setup
    original_suite: ExpectationSuite = context.get_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    assert len(original_suite.expectations) == 3
    assert context.list_expectation_suite_names() == [expectation_suite_name]
    assert context.list_datasources() == [
        {
            "name": "my_datasource",
            "class_name": "Datasource",
            "module_name": "great_expectations.datasource",
            "execution_engine": {
                "class_name": "PandasExecutionEngine",
                "module_name": "great_expectations.execution_engine",
            },
            "data_connectors": {
                "my_basic_data_connector": {
                    "module_name": "great_expectations.datasource.data_connector",
                    "base_directory": f"{root_dir}/../data/titanic",
                    "default_regex": {
                        "pattern": "(.*)\\.csv",
                        "group_names": ["data_asset_name"],
                    },
                    "class_name": "InferredAssetFilesystemDataConnector",
                },
                "my_special_data_connector": {
                    "glob_directive": "*.csv",
                    "assets": {
                        "users": {
                            "pattern": "(.+)_(\\d+)_(\\d+)\\.csv",
                            "group_names": ["name", "timestamp", "size"],
                            "class_name": "Asset",
                            "base_directory": f"{root_dir}/../data/titanic",
                            "module_name": "great_expectations.datasource.data_connector.asset",
                        }
                    },
                    "module_name": "great_expectations.datasource.data_connector",
                    "base_directory": f"{root_dir}/../data/titanic",
                    "default_regex": {"pattern": "(.+)\\.csv", "group_names": ["name"]},
                    "class_name": "ConfiguredAssetFilesystemDataConnector",
                },
                "my_other_data_connector": {
                    "glob_directive": "*.csv",
                    "assets": {
                        "users": {
                            "class_name": "Asset",
                            "module_name": "great_expectations.datasource.data_connector.asset",
                        }
                    },
                    "module_name": "great_expectations.datasource.data_connector",
                    "base_directory": f"{root_dir}/../data/titanic",
                    "default_regex": {"pattern": "(.+)\\.csv", "group_names": ["name"]},
                    "class_name": "ConfiguredAssetFilesystemDataConnector",
                },
                "my_runtime_data_connector": {
                    "module_name": "great_expectations.datasource.data_connector",
                    "batch_identifiers": ["pipeline_stage_name", "airflow_run_id"],
                    "class_name": "RuntimeDataConnector",
                },
            },
        },
        {
            "name": "my_additional_datasource",
            "class_name": "Datasource",
            "module_name": "great_expectations.datasource",
            "execution_engine": {
                "module_name": "great_expectations.execution_engine",
                "class_name": "PandasExecutionEngine",
            },
            "data_connectors": {
                "my_additional_data_connector": {
                    "module_name": "great_expectations.datasource.data_connector",
                    "default_regex": {
                        "pattern": "(.*)\\.csv",
                        "group_names": ["data_asset_name"],
                    },
                    "base_directory": f"{root_dir}/../data/titanic",
                    "class_name": "InferredAssetFilesystemDataConnector",
                }
            },
        },
    ]

    assert context.get_validation_result(expectation_suite_name="warning") == {}

    # Create notebook
    # do not want to actually send usage_message, since the function call is not the result of actual usage
    _suite_edit_workflow(
        context=context,
        expectation_suite_name=expectation_suite_name,
        profile=False,
        profiler_name=None,
        usage_event="test_notebook_execution",
        interactive_mode=CLISuiteInteractiveFlagCombinations.UNPROMPTED_INTERACTIVE_FALSE_MANUAL_TRUE,
        no_jupyter=True,
        create_if_not_exist=False,
        datasource_name=None,
        batch_request=batch_request,
        additional_batch_request_args=None,
        suppress_usage_message=True,
    )
    edit_notebook_path: str = os.path.join(uncommitted_dir, "edit_warning.ipynb")
    assert os.path.isfile(edit_notebook_path)

    run_notebook(
        notebook_path=edit_notebook_path,
        notebook_dir=uncommitted_dir,
        string_to_be_replaced="context.open_data_docs(resource_identifier=validation_result_identifier)",
        replacement_string="",
    )

    # Assertions about output
    context = DataContext(context_root_dir=root_dir)
    obs_validation_result: ExpectationSuiteValidationResult = (
        context.get_validation_result(expectation_suite_name="warning")
    )
    assert obs_validation_result.statistics == {
        "evaluated_expectations": 3,
        "successful_expectations": 2,
        "unsuccessful_expectations": 1,
        "success_percent": 66.66666666666666,
    }
    suite: ExpectationSuite = context.get_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    assert suite == original_suite


def test_notebook_execution_with_custom_notebooks_wrong_module(
    suite_with_multiple_citations, data_context_v3_custom_bad_notebooks
):
    """
    Test the error message in case of "bad" custom module is clear
    """
    with pytest.raises(
        SuiteEditNotebookCustomTemplateModuleNotFoundError, match=r"invalid\.module"
    ):
        SuiteEditNotebookRenderer.from_data_context(
            data_context=data_context_v3_custom_bad_notebooks
        ).render(suite=suite_with_multiple_citations)


def test_notebook_execution_with_custom_notebooks(
    suite_with_multiple_citations, data_context_v3_custom_notebooks
):
    """
    Test the different parts of the notebooks can be modified
    """
    batch_request: dict = {
        "datasource_name": "files_datasource",
        "data_connector_name": "files_data_connector",
        "data_asset_name": "1k",
    }
    obs: NotebookNode = SuiteEditNotebookRenderer.from_data_context(
        data_context=data_context_v3_custom_notebooks
    ).render(suite=suite_with_multiple_citations, batch_request=batch_request)
    expected: dict = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {},
        "cells": [
            {
                "id": "intellectual-inspection",
                "cell_type": "markdown",
                "source": "# Edit Your Expectation Suite\nUse this notebook to recreate and modify your expectation suite:\n\n**Expectation Suite Name**: `critical`",
                "metadata": {},
            },
            {
                "id": "collaborative-transfer",
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'import datetime\n\nimport pandas as pd\n\nimport great_expectations as ge\nimport great_expectations.jupyter_ux\nfrom great_expectations.core.batch import BatchRequest\nfrom great_expectations.checkpoint import SimpleCheckpoint\nfrom great_expectations.exceptions import DataContextError\n\ncontext = ge.data_context.DataContext()\n\nbatch_request = {\n    "datasource_name": "files_datasource",\n    "data_connector_name": "files_data_connector",\n    "data_asset_name": "1k",\n}\n\n\n# Feel free to change the name of your suite here. Renaming this will not remove the other one.\nexpectation_suite_name = "critical"\ntry:\n    suite = context.get_expectation_suite(expectation_suite_name=expectation_suite_name)\n    print(\n        f\'Loaded ExpectationSuite "{suite.expectation_suite_name}" containing {len(suite.expectations)} expectations.\'\n    )\nexcept DataContextError:\n    suite = context.create_expectation_suite(\n        expectation_suite_name=expectation_suite_name\n    )\n    print(f\'Created ExpectationSuite "{suite.expectation_suite_name}".\')\n\n\nvalidator = context.get_validator(\n    batch_request=BatchRequest(**batch_request),\n    expectation_suite_name=expectation_suite_name,\n)\ncolumn_names = [f\'"{column_name}"\' for column_name in validator.columns()]\nprint(f"Columns: {\', \'.join(column_names)}.")\nvalidator.head(n_rows=5, fetch_all=False)',
                "outputs": [],
            },
            {
                "id": "legal-beauty",
                "cell_type": "markdown",
                "source": "## Create & Edit Expectations\n\n\nAdd expectations by calling specific expectation methods on the `validator` object. They all begin with `.expect_` which makes autocompleting easy using tab.\n\nBecause you selected interactive mode, you are now creating or editing an Expectation Suite with validator feedback from the sample batch of data that you specified (see `batch_request`).\n\nNote that if you select manual mode you may still create or edit an Expectation Suite directly, without feedback from the `validator`. See our documentation for more info and examples: [How to create a new Expectation Suite without a sample batch](https://docs.greatexpectations.io/en/latest/guides/how_to_guides/creating_and_editing_expectations/how_to_create_a_new_expectation_suite_without_a_sample_batch.html).\n\n\n\nYou can see all the available expectations in the **[expectation gallery](https://greatexpectations.io/expectations)**.",
                "metadata": {},
            },
            {
                "id": "smoking-bangladesh",
                "cell_type": "markdown",
                "source": "### Table Expectation(s)",
                "metadata": {},
            },
            {
                "id": "irish-storm",
                "cell_type": "markdown",
                "source": "No table level expectations are in this suite. Feel free to add some here.\n\nThey all begin with `validator.expect_table_...`.\n",
                "metadata": {},
            },
            {
                "id": "injured-differential",
                "cell_type": "markdown",
                "source": "### Column Expectation(s)",
                "metadata": {},
            },
            {
                "id": "eleven-football",
                "cell_type": "markdown",
                "source": "#### `npi`",
                "metadata": {},
            },
            {
                "id": "eleven-solid",
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'validator.expect_column_values_to_not_be_null(column="npi")',
                "outputs": [],
            },
            {
                "id": "responsible-coverage",
                "cell_type": "markdown",
                "source": "#### `provider_type`",
                "metadata": {},
            },
            {
                "id": "compressed-indiana",
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'validator.expect_column_values_to_not_be_null(column="provider_type")',
                "outputs": [],
            },
            {
                "id": "scheduled-freeware",
                "cell_type": "markdown",
                "source": "## Review & Save Your Expectations\n\nLet's save the expectation suite as a JSON file in the `great_expectations/expectations` directory of your project.\n\nLet's now rebuild your Data Docs, which helps you communicate about your data with both machines and humans.",
                "metadata": {},
            },
            {
                "id": "useful-hearts",
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'print(validator.get_expectation_suite(discard_failed_expectations=False))\nvalidator.save_expectation_suite(discard_failed_expectations=False)\n\ncheckpoint_config = {\n    "class_name": "SimpleCheckpoint",\n    "validations": [\n        {\n            "batch_request": batch_request,\n            "expectation_suite_name": expectation_suite_name\n        }\n    ]\n}\ncheckpoint = SimpleCheckpoint(\n    f"_tmp_checkpoint_{expectation_suite_name}",\n    context,\n    **checkpoint_config\n)\ncheckpoint_result = checkpoint.run()\n\ncontext.build_data_docs()\n\nvalidation_result_identifier = checkpoint_result.list_validation_result_identifiers()[0]\ncontext.open_data_docs(resource_identifier=validation_result_identifier)',
                "outputs": [],
            },
        ],
    }

    del expected["nbformat_minor"]
    del obs["nbformat_minor"]
    for obs_cell, expected_cell in zip(obs["cells"], expected["cells"]):
        obs_cell.pop("id", None)
        expected_cell.pop("id", None)
        assert obs_cell == expected_cell
    assert obs == expected


@pytest.mark.parametrize(
    "row_condition",
    [
        "Sex == 'female'",
        """
        Sex == "female"
        """,
    ],
)
def test_raise_exception_quotes_or_space_with_row_condition(
    row_condition,
    titanic_v013_multi_datasource_pandas_data_context_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    """
    To set this test up we:

    - create a suite
    - add an expectations with a row_condition
    - create the suite edit notebook by hijacking the private cli method
    - chack if the exception is raised
    """
    # Since we'll run the notebook, we use a context with no data docs to avoid the renderer's default
    # behavior of building and opening docs, which is not part of this test.
    context: DataContext = titanic_v013_multi_datasource_pandas_data_context_with_checkpoints_v1_with_empty_store_stats_enabled
    root_dir: str = context.root_directory
    uncommitted_dir: str = os.path.join(root_dir, "uncommitted")
    expectation_suite_name: str = "warning"

    context.create_expectation_suite(expectation_suite_name=expectation_suite_name)
    batch_request: dict = {
        "datasource_name": "my_datasource",
        "data_connector_name": "my_basic_data_connector",
        "data_asset_name": "Titanic_1912",
    }
    validator: Validator = context.get_validator(
        batch_request=BatchRequest(**batch_request),
        expectation_suite_name=expectation_suite_name,
    )
    with pytest.raises(
        InvalidExpectationConfigurationError,
        match=r".*Do not introduce (?:simple quotes|\\n) in configuration.*",
    ):
        validator.expect_column_values_to_be_in_set(
            column="Sex",
            row_condition=row_condition,
            condition_parser="pandas",
            value_set=["female", "male"],
        )
