import json
import os
from unittest import mock

import nbformat
import pytest
from nbconvert.preprocessors import ExecutePreprocessor

from great_expectations import DataContext
from great_expectations.cli.suite import _suite_edit
from great_expectations.core.expectation_suite import ExpectationSuiteSchema
from great_expectations.exceptions import (
    SuiteEditNotebookCustomTemplateModuleNotFoundError,
)
from great_expectations.render.renderer.suite_edit_notebook_renderer import (
    SuiteEditNotebookRenderer,
)


@pytest.fixture
def critical_suite_with_citations():
    """
    This hand made fixture has a wide range of expectations, and has a mix of
    metadata including an BasicSuiteBuilderProfiler entry, and citations.
    """
    schema = ExpectationSuiteSchema()
    critical_suite = {
        "expectation_suite_name": "critical",
        "meta": {
            "great_expectations_version": "0.9.1+9.gf17eff1f.dirty",
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
                    "batch_kwargs": {
                        "path": "/home/foo/data/10k.csv",
                        "datasource": "files_datasource",
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
                "batch_kwargs": {
                    "path": "/Users/foo/data/10k.csv",
                    "datasource": "files_datasource",
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
    return schema.loads(json.dumps(critical_suite))


@pytest.fixture
def suite_with_multiple_citations():
    """
    A handmade suite with multiple citations each with different batch_kwargs.

    The most recent citation does not have batch_kwargs
    """
    schema = ExpectationSuiteSchema()
    critical_suite = {
        "expectation_suite_name": "critical",
        "meta": {
            "great_expectations_version": "0.9.1+9.gf17eff1f.dirty",
            "citations": [
                {
                    "citation_date": "2001-01-01T00:00:01.000001",
                    "batch_kwargs": {
                        "path": "3.csv",
                        "datasource": "3",
                    },
                },
                {
                    "citation_date": "2000-01-01T00:00:01.000001",
                    "batch_kwargs": {
                        "path": "2.csv",
                        "datasource": "2",
                    },
                },
                # This citation is the most recent and has no batch_kwargs
                {
                    "citation_date": "2020-01-01T00:00:01.000001",
                },
                {
                    "citation_date": "1999-01-01T00:00:01.000001",
                    "batch_kwargs": {
                        "path": "1.csv",
                        "datasource": "1",
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
    return schema.loads(json.dumps(critical_suite))


@pytest.fixture
def warning_suite():
    """
    This hand made fixture has a wide range of expectations, and has a mix of
    metadata including BasicSuiteBuilderProfiler entries.
    """
    schema = ExpectationSuiteSchema()
    warning_suite = {
        "expectation_suite_name": "warning",
        "meta": {
            "great_expectations_version": "0.8.4.post0",
            "citations": [
                {
                    "citation_date": "2020-02-28T17:34:31.307271",
                    "batch_kwargs": {
                        "path": "/home/foo/data/10k.csv",
                        "datasource": "files_datasource",
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
    return schema.loads(json.dumps(warning_suite))


def test_render_without_batch_kwargs_uses_batch_kwargs_in_citations(
    critical_suite_with_citations, empty_data_context
):
    obs = SuiteEditNotebookRenderer.from_data_context(empty_data_context).render(
        critical_suite_with_citations
    )
    print(obs)
    assert isinstance(obs, dict)
    expected = {
        "nbformat": 4,
        "nbformat_minor": 4,
        "metadata": {},
        "cells": [
            {
                "cell_type": "markdown",
                "source": "# Edit Your Expectation Suite\nUse this notebook to recreate and modify your expectation suite:\n\n**Expectation Suite Name**: `critical`\n\nWe'd love it if you **reach out to us on** the [**Great Expectations Slack Channel**](https://greatexpectations.io/slack)",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'import datetime\nimport great_expectations as ge\nimport great_expectations.jupyter_ux\nfrom great_expectations.checkpoint import LegacyCheckpoint\nfrom great_expectations.data_context.types.resource_identifiers import (\n    ValidationResultIdentifier,\n)\n\ncontext = ge.data_context.DataContext()\n\n# Feel free to change the name of your suite here. Renaming this will not\n# remove the other one.\nexpectation_suite_name = "critical"\nsuite = context.get_expectation_suite(expectation_suite_name)\nsuite.expectations = []\n\nbatch_kwargs = {"path": "/home/foo/data/10k.csv", "datasource": "files_datasource"}\nbatch = context.get_batch(batch_kwargs, suite)\nbatch.head()',
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "## Create & Edit Expectations\n\nAdd expectations by calling specific expectation methods on the `batch` object. They all begin with `.expect_` which makes autocompleting easy using tab.\n\nYou can see all the available expectations in the **[expectation glossary](https://docs.greatexpectations.io/en/latest/reference/glossary_of_expectations.html?utm_source=notebook&utm_medium=create_expectations)**.",
                "metadata": {},
            },
            {
                "cell_type": "markdown",
                "source": "### Table Expectation(s)",
                "metadata": {},
            },
            {
                "cell_type": "markdown",
                "source": "No table level expectations are in this suite. Feel free to add some here. They all begin with `batch.expect_table_...`.",
                "metadata": {},
            },
            {
                "cell_type": "markdown",
                "source": "### Column Expectation(s)",
                "metadata": {},
            },
            {"cell_type": "markdown", "source": "#### `npi`", "metadata": {}},
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.expect_column_values_to_not_be_null(\n    column="npi",\n    meta={\n        "question": True,\n        "Notes": "There are empty strings that should probably be nulls",\n    },\n)',
                "outputs": [],
            },
            {"cell_type": "markdown", "source": "#### `provider_type`", "metadata": {}},
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.expect_column_values_to_not_be_null(column="provider_type")',
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "## Save & Review Your Expectations\n\nLet's save the expectation suite as a JSON file in the `great_expectations/expectations` directory of your project.\nIf you decide not to save some expectations that you created, use [remove_expectation method](https://docs.greatexpectations.io/en/latest/autoapi/great_expectations/data_asset/index.html?highlight=remove_expectation&utm_source=notebook&utm_medium=edit_expectations#great_expectations.data_asset.DataAsset.remove_expectation).\n\nLet's now rebuild your Data Docs, which helps you communicate about your data with both machines and humans.",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.save_expectation_suite(discard_failed_expectations=False)\n\nresults = LegacyCheckpoint(\n    name="_temp_checkpoint",\n    data_context=context,\n    batches=[\n        {\n          "batch_kwargs": batch_kwargs,\n          "expectation_suite_names": [expectation_suite_name]\n        }\n    ]\n).run()\nvalidation_result_identifier = results.list_validation_result_identifiers()[0]\ncontext.build_data_docs()\ncontext.open_data_docs(validation_result_identifier)',
                "outputs": [],
            },
        ],
    }
    del expected["nbformat_minor"]
    del obs["nbformat_minor"]
    for obs_cell, expected_cell in zip(obs["cells"], expected["cells"]):
        obs_cell.pop("id", None)
        assert obs_cell == expected_cell
    assert obs == expected


def test_render_with_no_column_cells(critical_suite_with_citations, empty_data_context):
    critical_suite_with_citations.expectations = []
    obs = SuiteEditNotebookRenderer.from_data_context(empty_data_context).render(
        critical_suite_with_citations
    )
    assert isinstance(obs, dict)
    expected = {
        "nbformat": 4,
        "nbformat_minor": 4,
        "metadata": {},
        "cells": [
            {
                "cell_type": "markdown",
                "source": "# Edit Your Expectation Suite\nUse this notebook to recreate and modify your expectation suite:\n\n**Expectation Suite Name**: `critical`\n\nWe'd love it if you **reach out to us on** the [**Great Expectations Slack Channel**](https://greatexpectations.io/slack)",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'import datetime\nimport great_expectations as ge\nimport great_expectations.jupyter_ux\nfrom great_expectations.checkpoint import LegacyCheckpoint\nfrom great_expectations.data_context.types.resource_identifiers import (\n    ValidationResultIdentifier,\n)\n\ncontext = ge.data_context.DataContext()\n\n# Feel free to change the name of your suite here. Renaming this will not\n# remove the other one.\nexpectation_suite_name = "critical"\nsuite = context.get_expectation_suite(expectation_suite_name)\nsuite.expectations = []\n\nbatch_kwargs = {"path": "/home/foo/data/10k.csv", "datasource": "files_datasource"}\nbatch = context.get_batch(batch_kwargs, suite)\nbatch.head()',
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "## Create & Edit Expectations\n\nAdd expectations by calling specific expectation methods on the `batch` object. They all begin with `.expect_` which makes autocompleting easy using tab.\n\nYou can see all the available expectations in the **[expectation glossary](https://docs.greatexpectations.io/en/latest/reference/glossary_of_expectations.html?utm_source=notebook&utm_medium=create_expectations)**.",
                "metadata": {},
            },
            {
                "cell_type": "markdown",
                "source": "### Table Expectation(s)",
                "metadata": {},
            },
            {
                "cell_type": "markdown",
                "source": "No table level expectations are in this suite. Feel free to add some here. They all begin with `batch.expect_table_...`.",
                "metadata": {},
            },
            {
                "cell_type": "markdown",
                "source": "### Column Expectation(s)",
                "metadata": {},
            },
            {
                "cell_type": "markdown",
                "source": "No column level expectations are in this suite. Feel free to add some here. They all begin with `batch.expect_column_...`.",
                "metadata": {},
            },
            {
                "cell_type": "markdown",
                "source": "## Save & Review Your Expectations\n\nLet's save the expectation suite as a JSON file in the `great_expectations/expectations` directory of your project.\nIf you decide not to save some expectations that you created, use [remove_expectation method](https://docs.greatexpectations.io/en/latest/autoapi/great_expectations/data_asset/index.html?highlight=remove_expectation&utm_source=notebook&utm_medium=edit_expectations#great_expectations.data_asset.DataAsset.remove_expectation).\n\nLet's now rebuild your Data Docs, which helps you communicate about your data with both machines and humans.",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.save_expectation_suite(discard_failed_expectations=False)\n\nresults = LegacyCheckpoint(\n    name="_temp_checkpoint",\n    data_context=context,\n    batches=[\n        {\n          "batch_kwargs": batch_kwargs,\n          "expectation_suite_names": [expectation_suite_name]\n        }\n    ]\n).run()\nvalidation_result_identifier = results.list_validation_result_identifiers()[0]\ncontext.build_data_docs()\ncontext.open_data_docs(validation_result_identifier)',
                "outputs": [],
            },
        ],
    }
    del expected["nbformat_minor"]
    del obs["nbformat_minor"]
    for obs_cell, expected_cell in zip(obs["cells"], expected["cells"]):
        obs_cell.pop("id", None)
        assert obs_cell == expected_cell
    assert obs == expected


def test_render_without_batch_kwargs_and_no_batch_kwargs_in_citations_uses_blank_batch_kwargs(
    critical_suite_with_citations, empty_data_context
):
    suite_with_no_kwargs_in_citations = critical_suite_with_citations
    suite_with_no_kwargs_in_citations.meta["citations"][0].pop("batch_kwargs")
    obs = SuiteEditNotebookRenderer.from_data_context(empty_data_context).render(
        suite_with_no_kwargs_in_citations
    )
    assert isinstance(obs, dict)
    expected = {
        "nbformat": 4,
        "nbformat_minor": 4,
        "metadata": {},
        "cells": [
            {
                "cell_type": "markdown",
                "source": "# Edit Your Expectation Suite\nUse this notebook to recreate and modify your expectation suite:\n\n**Expectation Suite Name**: `critical`\n\nWe'd love it if you **reach out to us on** the [**Great Expectations Slack Channel**](https://greatexpectations.io/slack)",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'import datetime\nimport great_expectations as ge\nimport great_expectations.jupyter_ux\nfrom great_expectations.checkpoint import LegacyCheckpoint\nfrom great_expectations.data_context.types.resource_identifiers import (\n    ValidationResultIdentifier,\n)\n\ncontext = ge.data_context.DataContext()\n\n# Feel free to change the name of your suite here. Renaming this will not\n# remove the other one.\nexpectation_suite_name = "critical"\nsuite = context.get_expectation_suite(expectation_suite_name)\nsuite.expectations = []\n\nbatch_kwargs = {}\nbatch = context.get_batch(batch_kwargs, suite)\nbatch.head()',
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "## Create & Edit Expectations\n\nAdd expectations by calling specific expectation methods on the `batch` object. They all begin with `.expect_` which makes autocompleting easy using tab.\n\nYou can see all the available expectations in the **[expectation glossary](https://docs.greatexpectations.io/en/latest/reference/glossary_of_expectations.html?utm_source=notebook&utm_medium=create_expectations)**.",
                "metadata": {},
            },
            {
                "cell_type": "markdown",
                "source": "### Table Expectation(s)",
                "metadata": {},
            },
            {
                "cell_type": "markdown",
                "source": "No table level expectations are in this suite. Feel free to add some here. They all begin with `batch.expect_table_...`.",
                "metadata": {},
            },
            {
                "cell_type": "markdown",
                "source": "### Column Expectation(s)",
                "metadata": {},
            },
            {"cell_type": "markdown", "source": "#### `npi`", "metadata": {}},
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.expect_column_values_to_not_be_null(\n    column="npi",\n    meta={\n        "question": True,\n        "Notes": "There are empty strings that should probably be nulls",\n    },\n)',
                "outputs": [],
            },
            {"cell_type": "markdown", "source": "#### `provider_type`", "metadata": {}},
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.expect_column_values_to_not_be_null(column="provider_type")',
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "## Save & Review Your Expectations\n\nLet's save the expectation suite as a JSON file in the `great_expectations/expectations` directory of your project.\nIf you decide not to save some expectations that you created, use [remove_expectation method](https://docs.greatexpectations.io/en/latest/autoapi/great_expectations/data_asset/index.html?highlight=remove_expectation&utm_source=notebook&utm_medium=edit_expectations#great_expectations.data_asset.DataAsset.remove_expectation).\n\nLet's now rebuild your Data Docs, which helps you communicate about your data with both machines and humans.",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.save_expectation_suite(discard_failed_expectations=False)\n\nresults = LegacyCheckpoint(\n    name="_temp_checkpoint",\n    data_context=context,\n    batches=[\n        {\n          "batch_kwargs": batch_kwargs,\n          "expectation_suite_names": [expectation_suite_name]\n        }\n    ]\n).run()\nvalidation_result_identifier = results.list_validation_result_identifiers()[0]\ncontext.build_data_docs()\ncontext.open_data_docs(validation_result_identifier)',
                "outputs": [],
            },
        ],
    }
    del expected["nbformat_minor"]
    del obs["nbformat_minor"]
    for obs_cell, expected_cell in zip(obs["cells"], expected["cells"]):
        obs_cell.pop("id", None)
        assert obs_cell == expected_cell
    assert obs == expected


def test_render_with_batch_kwargs_and_no_batch_kwargs_in_citations(
    critical_suite_with_citations, empty_data_context
):
    suite_with_no_kwargs_in_citations = critical_suite_with_citations
    suite_with_no_kwargs_in_citations.meta["citations"][0].pop("batch_kwargs")
    batch_kwargs = {"foo": "bar", "datasource": "things"}
    obs = SuiteEditNotebookRenderer.from_data_context(empty_data_context).render(
        suite_with_no_kwargs_in_citations, batch_kwargs
    )
    assert isinstance(obs, dict)
    expected = {
        "nbformat": 4,
        "nbformat_minor": 4,
        "metadata": {},
        "cells": [
            {
                "cell_type": "markdown",
                "source": "# Edit Your Expectation Suite\nUse this notebook to recreate and modify your expectation suite:\n\n**Expectation Suite Name**: `critical`\n\nWe'd love it if you **reach out to us on** the [**Great Expectations Slack Channel**](https://greatexpectations.io/slack)",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'import datetime\nimport great_expectations as ge\nimport great_expectations.jupyter_ux\nfrom great_expectations.checkpoint import LegacyCheckpoint\nfrom great_expectations.data_context.types.resource_identifiers import (\n    ValidationResultIdentifier,\n)\n\ncontext = ge.data_context.DataContext()\n\n# Feel free to change the name of your suite here. Renaming this will not\n# remove the other one.\nexpectation_suite_name = "critical"\nsuite = context.get_expectation_suite(expectation_suite_name)\nsuite.expectations = []\n\nbatch_kwargs = {"foo": "bar", "datasource": "things"}\nbatch = context.get_batch(batch_kwargs, suite)\nbatch.head()',
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "## Create & Edit Expectations\n\nAdd expectations by calling specific expectation methods on the `batch` object. They all begin with `.expect_` which makes autocompleting easy using tab.\n\nYou can see all the available expectations in the **[expectation glossary](https://docs.greatexpectations.io/en/latest/reference/glossary_of_expectations.html?utm_source=notebook&utm_medium=create_expectations)**.",
                "metadata": {},
            },
            {
                "cell_type": "markdown",
                "source": "### Table Expectation(s)",
                "metadata": {},
            },
            {
                "cell_type": "markdown",
                "source": "No table level expectations are in this suite. Feel free to add some here. They all begin with `batch.expect_table_...`.",
                "metadata": {},
            },
            {
                "cell_type": "markdown",
                "source": "### Column Expectation(s)",
                "metadata": {},
            },
            {"cell_type": "markdown", "source": "#### `npi`", "metadata": {}},
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.expect_column_values_to_not_be_null(\n    column="npi",\n    meta={\n        "question": True,\n        "Notes": "There are empty strings that should probably be nulls",\n    },\n)',
                "outputs": [],
            },
            {"cell_type": "markdown", "source": "#### `provider_type`", "metadata": {}},
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.expect_column_values_to_not_be_null(column="provider_type")',
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "## Save & Review Your Expectations\n\nLet's save the expectation suite as a JSON file in the `great_expectations/expectations` directory of your project.\nIf you decide not to save some expectations that you created, use [remove_expectation method](https://docs.greatexpectations.io/en/latest/autoapi/great_expectations/data_asset/index.html?highlight=remove_expectation&utm_source=notebook&utm_medium=edit_expectations#great_expectations.data_asset.DataAsset.remove_expectation).\n\nLet's now rebuild your Data Docs, which helps you communicate about your data with both machines and humans.",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.save_expectation_suite(discard_failed_expectations=False)\n\nresults = LegacyCheckpoint(\n    name="_temp_checkpoint",\n    data_context=context,\n    batches=[\n        {\n          "batch_kwargs": batch_kwargs,\n          "expectation_suite_names": [expectation_suite_name]\n        }\n    ]\n).run()\nvalidation_result_identifier = results.list_validation_result_identifiers()[0]\ncontext.build_data_docs()\ncontext.open_data_docs(validation_result_identifier)',
                "outputs": [],
            },
        ],
    }
    del expected["nbformat_minor"]
    del obs["nbformat_minor"]
    for obs_cell, expected_cell in zip(obs["cells"], expected["cells"]):
        obs_cell.pop("id", None)
        assert obs_cell == expected_cell
    assert obs == expected


def test_render_with_no_batch_kwargs_and_no_citations(
    critical_suite_with_citations, empty_data_context
):
    suite_with_no_citations = critical_suite_with_citations
    suite_with_no_citations.meta.pop("citations")
    obs = SuiteEditNotebookRenderer.from_data_context(empty_data_context).render(
        suite_with_no_citations
    )
    assert isinstance(obs, dict)
    expected = {
        "nbformat": 4,
        "nbformat_minor": 4,
        "metadata": {},
        "cells": [
            {
                "cell_type": "markdown",
                "source": "# Edit Your Expectation Suite\nUse this notebook to recreate and modify your expectation suite:\n\n**Expectation Suite Name**: `critical`\n\nWe'd love it if you **reach out to us on** the [**Great Expectations Slack Channel**](https://greatexpectations.io/slack)",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'import datetime\nimport great_expectations as ge\nimport great_expectations.jupyter_ux\nfrom great_expectations.checkpoint import LegacyCheckpoint\nfrom great_expectations.data_context.types.resource_identifiers import (\n    ValidationResultIdentifier,\n)\n\ncontext = ge.data_context.DataContext()\n\n# Feel free to change the name of your suite here. Renaming this will not\n# remove the other one.\nexpectation_suite_name = "critical"\nsuite = context.get_expectation_suite(expectation_suite_name)\nsuite.expectations = []\n\nbatch_kwargs = {}\nbatch = context.get_batch(batch_kwargs, suite)\nbatch.head()',
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "## Create & Edit Expectations\n\nAdd expectations by calling specific expectation methods on the `batch` object. They all begin with `.expect_` which makes autocompleting easy using tab.\n\nYou can see all the available expectations in the **[expectation glossary](https://docs.greatexpectations.io/en/latest/reference/glossary_of_expectations.html?utm_source=notebook&utm_medium=create_expectations)**.",
                "metadata": {},
            },
            {
                "cell_type": "markdown",
                "source": "### Table Expectation(s)",
                "metadata": {},
            },
            {
                "cell_type": "markdown",
                "source": "No table level expectations are in this suite. Feel free to add some here. They all begin with `batch.expect_table_...`.",
                "metadata": {},
            },
            {
                "cell_type": "markdown",
                "source": "### Column Expectation(s)",
                "metadata": {},
            },
            {"cell_type": "markdown", "source": "#### `npi`", "metadata": {}},
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.expect_column_values_to_not_be_null(\n    column="npi",\n    meta={\n        "question": True,\n        "Notes": "There are empty strings that should probably be nulls",\n    },\n)',
                "outputs": [],
            },
            {"cell_type": "markdown", "source": "#### `provider_type`", "metadata": {}},
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.expect_column_values_to_not_be_null(column="provider_type")',
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "## Save & Review Your Expectations\n\nLet's save the expectation suite as a JSON file in the `great_expectations/expectations` directory of your project.\nIf you decide not to save some expectations that you created, use [remove_expectation method](https://docs.greatexpectations.io/en/latest/autoapi/great_expectations/data_asset/index.html?highlight=remove_expectation&utm_source=notebook&utm_medium=edit_expectations#great_expectations.data_asset.DataAsset.remove_expectation).\n\nLet's now rebuild your Data Docs, which helps you communicate about your data with both machines and humans.",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.save_expectation_suite(discard_failed_expectations=False)\n\nresults = LegacyCheckpoint(\n    name="_temp_checkpoint",\n    data_context=context,\n    batches=[\n        {\n          "batch_kwargs": batch_kwargs,\n          "expectation_suite_names": [expectation_suite_name]\n        }\n    ]\n).run()\nvalidation_result_identifier = results.list_validation_result_identifiers()[0]\ncontext.build_data_docs()\ncontext.open_data_docs(validation_result_identifier)',
                "outputs": [],
            },
        ],
    }
    del expected["nbformat_minor"]
    del obs["nbformat_minor"]
    for obs_cell, expected_cell in zip(obs["cells"], expected["cells"]):
        obs_cell.pop("id", None)
        assert obs_cell == expected_cell
    assert obs == expected


def test_render_with_batch_kwargs_overrides_batch_kwargs_in_citations(
    critical_suite_with_citations, empty_data_context
):
    batch_kwargs = {"foo": "bar", "datasource": "things"}
    obs = SuiteEditNotebookRenderer.from_data_context(empty_data_context).render(
        critical_suite_with_citations, batch_kwargs
    )
    assert isinstance(obs, dict)
    expected = {
        "nbformat": 4,
        "nbformat_minor": 4,
        "metadata": {},
        "cells": [
            {
                "cell_type": "markdown",
                "source": "# Edit Your Expectation Suite\nUse this notebook to recreate and modify your expectation suite:\n\n**Expectation Suite Name**: `critical`\n\nWe'd love it if you **reach out to us on** the [**Great Expectations Slack Channel**](https://greatexpectations.io/slack)",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'import datetime\nimport great_expectations as ge\nimport great_expectations.jupyter_ux\nfrom great_expectations.checkpoint import LegacyCheckpoint\nfrom great_expectations.data_context.types.resource_identifiers import (\n    ValidationResultIdentifier,\n)\n\ncontext = ge.data_context.DataContext()\n\n# Feel free to change the name of your suite here. Renaming this will not\n# remove the other one.\nexpectation_suite_name = "critical"\nsuite = context.get_expectation_suite(expectation_suite_name)\nsuite.expectations = []\n\nbatch_kwargs = {"foo": "bar", "datasource": "things"}\nbatch = context.get_batch(batch_kwargs, suite)\nbatch.head()',
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "## Create & Edit Expectations\n\nAdd expectations by calling specific expectation methods on the `batch` object. They all begin with `.expect_` which makes autocompleting easy using tab.\n\nYou can see all the available expectations in the **[expectation glossary](https://docs.greatexpectations.io/en/latest/reference/glossary_of_expectations.html?utm_source=notebook&utm_medium=create_expectations)**.",
                "metadata": {},
            },
            {
                "cell_type": "markdown",
                "source": "### Table Expectation(s)",
                "metadata": {},
            },
            {
                "cell_type": "markdown",
                "source": "No table level expectations are in this suite. Feel free to add some here. They all begin with `batch.expect_table_...`.",
                "metadata": {},
            },
            {
                "cell_type": "markdown",
                "source": "### Column Expectation(s)",
                "metadata": {},
            },
            {"cell_type": "markdown", "source": "#### `npi`", "metadata": {}},
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.expect_column_values_to_not_be_null(\n    column="npi",\n    meta={\n        "question": True,\n        "Notes": "There are empty strings that should probably be nulls",\n    },\n)',
                "outputs": [],
            },
            {"cell_type": "markdown", "source": "#### `provider_type`", "metadata": {}},
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.expect_column_values_to_not_be_null(column="provider_type")',
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "## Save & Review Your Expectations\n\nLet's save the expectation suite as a JSON file in the `great_expectations/expectations` directory of your project.\nIf you decide not to save some expectations that you created, use [remove_expectation method](https://docs.greatexpectations.io/en/latest/autoapi/great_expectations/data_asset/index.html?highlight=remove_expectation&utm_source=notebook&utm_medium=edit_expectations#great_expectations.data_asset.DataAsset.remove_expectation).\n\nLet's now rebuild your Data Docs, which helps you communicate about your data with both machines and humans.",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.save_expectation_suite(discard_failed_expectations=False)\n\nresults = LegacyCheckpoint(\n    name="_temp_checkpoint",\n    data_context=context,\n    batches=[\n        {\n          "batch_kwargs": batch_kwargs,\n          "expectation_suite_names": [expectation_suite_name]\n        }\n    ]\n).run()\nvalidation_result_identifier = results.list_validation_result_identifiers()[0]\ncontext.build_data_docs()\ncontext.open_data_docs(validation_result_identifier)',
                "outputs": [],
            },
        ],
    }
    del expected["nbformat_minor"]
    del obs["nbformat_minor"]
    for obs_cell, expected_cell in zip(obs["cells"], expected["cells"]):
        obs_cell.pop("id", None)
        assert obs_cell == expected_cell
    assert obs == expected


def test_render_path_fixes_relative_paths():
    batch_kwargs = {"path": "data.csv"}
    notebook_renderer = SuiteEditNotebookRenderer()
    expected = {"path": "../../data.csv"}
    assert notebook_renderer._fix_path_in_batch_kwargs(batch_kwargs) == expected


def test_render_path_ignores_gcs_urls():
    batch_kwargs = {"path": "gs://a-bucket/data.csv"}
    notebook_renderer = SuiteEditNotebookRenderer()
    expected = {"path": "gs://a-bucket/data.csv"}
    assert notebook_renderer._fix_path_in_batch_kwargs(batch_kwargs) == expected


def test_render_path_ignores_s3_urls():
    batch_kwargs = {"path": "s3://a-bucket/data.csv"}
    notebook_renderer = SuiteEditNotebookRenderer()
    expected = {"path": "s3://a-bucket/data.csv"}
    assert notebook_renderer._fix_path_in_batch_kwargs(batch_kwargs) == expected


def test_render_with_no_batch_kwargs_multiple_batch_kwarg_citations(
    suite_with_multiple_citations, empty_data_context
):
    obs = SuiteEditNotebookRenderer.from_data_context(empty_data_context).render(
        suite_with_multiple_citations
    )
    assert isinstance(obs, dict)
    expected = {
        "nbformat": 4,
        "nbformat_minor": 4,
        "metadata": {},
        "cells": [
            {
                "cell_type": "markdown",
                "source": "# Edit Your Expectation Suite\nUse this notebook to recreate and modify your expectation suite:\n\n**Expectation Suite Name**: `critical`\n\nWe'd love it if you **reach out to us on** the [**Great Expectations Slack Channel**](https://greatexpectations.io/slack)",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'import datetime\nimport great_expectations as ge\nimport great_expectations.jupyter_ux\nfrom great_expectations.checkpoint import LegacyCheckpoint\nfrom great_expectations.data_context.types.resource_identifiers import (\n    ValidationResultIdentifier,\n)\n\ncontext = ge.data_context.DataContext()\n\n# Feel free to change the name of your suite here. Renaming this will not\n# remove the other one.\nexpectation_suite_name = "critical"\nsuite = context.get_expectation_suite(expectation_suite_name)\nsuite.expectations = []\n\nbatch_kwargs = {"path": "../../3.csv", "datasource": "3"}\nbatch = context.get_batch(batch_kwargs, suite)\nbatch.head()',
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "## Create & Edit Expectations\n\nAdd expectations by calling specific expectation methods on the `batch` object. They all begin with `.expect_` which makes autocompleting easy using tab.\n\nYou can see all the available expectations in the **[expectation glossary](https://docs.greatexpectations.io/en/latest/reference/glossary_of_expectations.html?utm_source=notebook&utm_medium=create_expectations)**.",
                "metadata": {},
            },
            {
                "cell_type": "markdown",
                "source": "### Table Expectation(s)",
                "metadata": {},
            },
            {
                "cell_type": "markdown",
                "source": "No table level expectations are in this suite. Feel free to add some here. They all begin with `batch.expect_table_...`.",
                "metadata": {},
            },
            {
                "cell_type": "markdown",
                "source": "### Column Expectation(s)",
                "metadata": {},
            },
            {"cell_type": "markdown", "source": "#### `npi`", "metadata": {}},
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.expect_column_values_to_not_be_null(column="npi")',
                "outputs": [],
            },
            {"cell_type": "markdown", "source": "#### `provider_type`", "metadata": {}},
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.expect_column_values_to_not_be_null(column="provider_type")',
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "## Save & Review Your Expectations\n\nLet's save the expectation suite as a JSON file in the `great_expectations/expectations` directory of your project.\nIf you decide not to save some expectations that you created, use [remove_expectation method](https://docs.greatexpectations.io/en/latest/autoapi/great_expectations/data_asset/index.html?highlight=remove_expectation&utm_source=notebook&utm_medium=edit_expectations#great_expectations.data_asset.DataAsset.remove_expectation).\n\nLet's now rebuild your Data Docs, which helps you communicate about your data with both machines and humans.",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.save_expectation_suite(discard_failed_expectations=False)\n\nresults = LegacyCheckpoint(\n    name="_temp_checkpoint",\n    data_context=context,\n    batches=[\n        {\n          "batch_kwargs": batch_kwargs,\n          "expectation_suite_names": [expectation_suite_name]\n        }\n    ]\n).run()\nvalidation_result_identifier = results.list_validation_result_identifiers()[0]\ncontext.build_data_docs()\ncontext.open_data_docs(validation_result_identifier)',
                "outputs": [],
            },
        ],
    }
    del expected["nbformat_minor"]
    del obs["nbformat_minor"]
    for obs_cell, expected_cell in zip(obs["cells"], expected["cells"]):
        obs_cell.pop("id", None)
        assert obs_cell == expected_cell
    assert obs == expected


def test_batch_kwarg_path_relative_is_modified_and_found_in_a_code_cell(
    critical_suite_with_citations, empty_data_context
):
    obs = SuiteEditNotebookRenderer.from_data_context(empty_data_context).render(
        critical_suite_with_citations, {"path": "foo/data"}
    )
    assert isinstance(obs, dict)
    found_expected = False
    for cell in obs["cells"]:
        if cell["cell_type"] == "code":
            source_code = cell["source"]
            if 'batch_kwargs = {"path": "../../foo/data"}' in source_code:
                found_expected = True
                break

    assert found_expected


def test_batch_kwarg_path_relative_dot_slash_is_modified_and_found_in_a_code_cell(
    critical_suite_with_citations, empty_data_context
):
    obs = SuiteEditNotebookRenderer.from_data_context(empty_data_context).render(
        critical_suite_with_citations, {"path": "./foo/data"}
    )
    assert isinstance(obs, dict)
    found_expected = False
    for cell in obs["cells"]:
        if cell["cell_type"] == "code":
            source_code = cell["source"]
            if 'batch_kwargs = {"path": "../.././foo/data"}' in source_code:
                found_expected = True
                break

    assert found_expected


def test_batch_kwarg_path_absolute_is_not_modified_and_is_found_in_a_code_cell(
    critical_suite_with_citations, empty_data_context
):
    obs = SuiteEditNotebookRenderer.from_data_context(empty_data_context).render(
        critical_suite_with_citations, {"path": "/home/user/foo/data"}
    )
    assert isinstance(obs, dict)
    found_expected = False
    for cell in obs["cells"]:
        if cell["cell_type"] == "code":
            source_code = cell["source"]
            if 'batch_kwargs = {"path": "/home/user/foo/data"}' in source_code:
                found_expected = True
                break

    assert found_expected


def test_complex_suite(warning_suite, empty_data_context):
    obs = SuiteEditNotebookRenderer.from_data_context(empty_data_context).render(
        warning_suite, {"path": "foo/data"}
    )
    assert isinstance(obs, dict)
    expected = {
        "nbformat": 4,
        "nbformat_minor": 4,
        "metadata": {},
        "cells": [
            {
                "cell_type": "markdown",
                "source": "# Edit Your Expectation Suite\nUse this notebook to recreate and modify your expectation suite:\n\n**Expectation Suite Name**: `warning`\n\nWe'd love it if you **reach out to us on** the [**Great Expectations Slack Channel**](https://greatexpectations.io/slack)",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'import datetime\nimport great_expectations as ge\nimport great_expectations.jupyter_ux\nfrom great_expectations.checkpoint import LegacyCheckpoint\nfrom great_expectations.data_context.types.resource_identifiers import (\n    ValidationResultIdentifier,\n)\n\ncontext = ge.data_context.DataContext()\n\n# Feel free to change the name of your suite here. Renaming this will not\n# remove the other one.\nexpectation_suite_name = "warning"\nsuite = context.get_expectation_suite(expectation_suite_name)\nsuite.expectations = []\n\nbatch_kwargs = {"path": "../../foo/data"}\nbatch = context.get_batch(batch_kwargs, suite)\nbatch.head()',
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "## Create & Edit Expectations\n\nAdd expectations by calling specific expectation methods on the `batch` object. They all begin with `.expect_` which makes autocompleting easy using tab.\n\nYou can see all the available expectations in the **[expectation glossary](https://docs.greatexpectations.io/en/latest/reference/glossary_of_expectations.html?utm_source=notebook&utm_medium=create_expectations)**.",
                "metadata": {},
            },
            {
                "cell_type": "markdown",
                "source": "### Table Expectation(s)",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "batch.expect_table_row_count_to_be_between(min_value=800000, max_value=1200000)",
                "outputs": [],
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "batch.expect_table_column_count_to_equal(value=71)",
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "### Column Expectation(s)",
                "metadata": {},
            },
            {"cell_type": "markdown", "source": "#### `npi`", "metadata": {}},
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.expect_column_values_to_not_be_null(column="npi")',
                "outputs": [],
            },
            {"cell_type": "markdown", "source": "#### `provider_type`", "metadata": {}},
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.expect_column_values_to_not_be_null(column="provider_type")',
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "#### `nppes_provider_last_org_name`",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.expect_column_values_to_not_be_null(column="nppes_provider_last_org_name")',
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "#### `nppes_provider_gender`",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.expect_column_values_to_be_in_set(\n    column="nppes_provider_gender", value_set=["M", "F", ""]\n)',
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "#### `nppes_entity_code`",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.expect_column_values_to_not_be_null(column="nppes_entity_code")',
                "outputs": [],
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.expect_column_values_to_be_in_set(\n    column="nppes_entity_code", value_set=["I", "O"]\n)',
                "outputs": [],
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.expect_column_kl_divergence_to_be_less_than(\n    column="nppes_entity_code",\n    partition_object={\n        "values": ["I", "O"],\n        "weights": [0.9431769750233306, 0.056823024976669335],\n    },\n    threshold=0.1,\n)',
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "#### `nppes_provider_state`",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.expect_column_values_to_be_in_set(\n    column="nppes_provider_state",\n    value_set=[\n        "AL",\n        "AK",\n        "AZ",\n        "AR",\n        "CA",\n        "CO",\n        "CT",\n        "DE",\n        "FL",\n        "GA",\n        "HI",\n        "ID",\n        "IL",\n        "IN",\n        "IA",\n        "KS",\n        "KY",\n        "LA",\n        "ME",\n        "MD",\n        "MA",\n        "MI",\n        "MN",\n        "MS",\n        "MO",\n        "MT",\n        "NE",\n        "NV",\n        "NH",\n        "NJ",\n        "NM",\n        "NY",\n        "NC",\n        "ND",\n        "OH",\n        "OK",\n        "OR",\n        "PA",\n        "RI",\n        "SC",\n        "SD",\n        "TN",\n        "TX",\n        "UT",\n        "VT",\n        "VA",\n        "WA",\n        "WV",\n        "WI",\n        "WY",\n        "DC",\n        "PR",\n        "AE",\n        "VI",\n    ],\n    mostly=0.999,\n)',
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "#### `medicare_participation_indicator`",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.expect_column_values_to_not_be_null(column="medicare_participation_indicator")',
                "outputs": [],
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.expect_column_values_to_be_in_set(\n    column="medicare_participation_indicator", value_set=["Y", "N"]\n)',
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "#### `number_of_hcpcs`",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.expect_column_values_to_not_be_null(column="number_of_hcpcs")',
                "outputs": [],
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.expect_column_values_to_be_between(\n    column="number_of_hcpcs", min_value=0, max_value=500, mostly=0.999\n)',
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "#### `total_unique_benes`",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.expect_column_values_to_not_be_null(column="total_unique_benes")',
                "outputs": [],
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.expect_column_values_to_be_between(\n    column="total_unique_benes", min_value=0, max_value=2000, mostly=0.95\n)',
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "#### `med_suppress_indicator`",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.expect_column_values_to_be_null(column="med_suppress_indicator", mostly=0.85)',
                "outputs": [],
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.expect_column_values_to_be_in_set(\n    column="med_suppress_indicator", value_set=["#", "*"]\n)',
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "#### `beneficiary_average_age`",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.expect_column_values_to_be_between(\n    column="beneficiary_average_age", min_value=40, max_value=90, mostly=0.995\n)',
                "outputs": [],
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.expect_column_kl_divergence_to_be_less_than(\n    column="beneficiary_average_age",\n    partition_object={\n        "bins": [8, 16.5, 25, 33.5, 42, 50.5, 59, 67.5, 76, 84.5, 93],\n        "weights": [\n            0.00025259576594384474,\n            0.00013318685840675451,\n            0.0009653750909344757,\n            0.0012363414580378728,\n            0.01081660996274442,\n            0.030813927854975127,\n            0.13495227317818748,\n            0.6919590041664524,\n            0.1244213260634741,\n            0.004449359600843578,\n        ],\n    },\n    threshold=0.9,\n)',
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "#### `total_submitted_chrg_amt`",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.expect_column_values_to_be_between(\n    column="total_submitted_chrg_amt", min_value=2000, max_value=5000000, mostly=0.98\n)',
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "#### `nppes_provider_first_name`",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.expect_column_values_to_not_be_null(\n    column="nppes_provider_first_name", mostly=0.9\n)',
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "#### `nppes_provider_zip`",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.expect_column_values_to_match_regex(\n    column="nppes_provider_zip", regex="^\\d*$", mostly=0.999\n)',
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "## Save & Review Your Expectations\n\nLet's save the expectation suite as a JSON file in the `great_expectations/expectations` directory of your project.\nIf you decide not to save some expectations that you created, use [remove_expectation method](https://docs.greatexpectations.io/en/latest/autoapi/great_expectations/data_asset/index.html?highlight=remove_expectation&utm_source=notebook&utm_medium=edit_expectations#great_expectations.data_asset.DataAsset.remove_expectation).\n\nLet's now rebuild your Data Docs, which helps you communicate about your data with both machines and humans.",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.save_expectation_suite(discard_failed_expectations=False)\n\nresults = LegacyCheckpoint(\n    name="_temp_checkpoint",\n    data_context=context,\n    batches=[\n        {\n          "batch_kwargs": batch_kwargs,\n          "expectation_suite_names": [expectation_suite_name]\n        }\n    ]\n).run()\nvalidation_result_identifier = results.list_validation_result_identifiers()[0]\ncontext.build_data_docs()\ncontext.open_data_docs(validation_result_identifier)',
                "outputs": [],
            },
        ],
    }
    del expected["nbformat_minor"]
    del obs["nbformat_minor"]
    for obs_cell, expected_cell in zip(obs["cells"], expected["cells"]):
        obs_cell.pop("id", None)
        assert obs_cell == expected_cell
    assert obs == expected


def test_notebook_execution_with_pandas_backend(titanic_data_context_no_data_docs):
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
    # Since we'll run the notebook, we use a context with no data docs to avoid
    # the renderer's default behavior of building and opening docs, which is not
    # part of this test.
    context = titanic_data_context_no_data_docs
    root_dir = context.root_directory
    uncommitted_dir = os.path.join(root_dir, "uncommitted")
    suite_name = "warning"

    context.create_expectation_suite(suite_name)
    csv_path = os.path.join(root_dir, "..", "data", "Titanic.csv")
    batch_kwargs = {"datasource": "mydatasource", "path": csv_path}
    batch = context.get_batch(batch_kwargs, suite_name)
    batch.expect_table_column_count_to_equal(1)
    batch.expect_table_row_count_to_equal(1313)
    batch.expect_column_values_to_be_in_set("Sex", ["female", "male"])
    batch.save_expectation_suite(discard_failed_expectations=False)

    # Sanity check test setup
    suite = context.get_expectation_suite(suite_name)
    original_suite = suite
    assert len(suite.expectations) == 3
    assert context.list_expectation_suite_names() == [suite_name]
    assert context.list_datasources() == [
        {
            "module_name": "great_expectations.datasource",
            "class_name": "PandasDatasource",
            "data_asset_type": {
                "module_name": "great_expectations.dataset",
                "class_name": "PandasDataset",
            },
            "batch_kwargs_generators": {
                "mygenerator": {
                    "class_name": "SubdirReaderBatchKwargsGenerator",
                    "base_directory": "../data",
                }
            },
            "name": "mydatasource",
        }
    ]
    assert context.get_validation_result("warning") == {}

    # Create notebook
    json_batch_kwargs = json.dumps(batch_kwargs)
    _suite_edit(
        suite_name,
        "mydatasource",
        directory=root_dir,
        jupyter=False,
        batch_kwargs=json_batch_kwargs,
        usage_event="test_notebook_execution",
    )
    edit_notebook_path = os.path.join(uncommitted_dir, "edit_warning.ipynb")
    assert os.path.isfile(edit_notebook_path)

    with open(edit_notebook_path) as f:
        nb = nbformat.read(f, as_version=4)

    # Run notebook
    ep = ExecutePreprocessor(timeout=600, kernel_name="python3")
    ep.preprocess(nb, {"metadata": {"path": uncommitted_dir}})

    # Assertions about output
    context = DataContext(root_dir)
    obs_validation_result = context.get_validation_result("warning")
    assert obs_validation_result.statistics == {
        "evaluated_expectations": 3,
        "successful_expectations": 2,
        "unsuccessful_expectations": 1,
        "success_percent": 66.66666666666666,
    }
    suite = context.get_expectation_suite(suite_name)
    assert suite == original_suite


def test_notebook_execution_with_custom_notebooks_wrong_module(
    suite_with_multiple_citations, data_context_with_bad_notebooks
):
    """
    Test the error message in case of "bad" custom module is clear
    """
    with pytest.raises(
        SuiteEditNotebookCustomTemplateModuleNotFoundError, match=r"invalid\.module"
    ):
        SuiteEditNotebookRenderer.from_data_context(
            data_context_with_bad_notebooks
        ).render(suite_with_multiple_citations)


def test_notebook_execution_with_custom_notebooks(
    suite_with_multiple_citations, data_context_custom_notebooks
):
    """
    Test the different parts of the notebooks can be modified
    """
    obs = SuiteEditNotebookRenderer.from_data_context(
        data_context_custom_notebooks
    ).render(suite_with_multiple_citations)
    assert isinstance(obs, dict)
    expected = {
        "nbformat": 4,
        "nbformat_minor": 4,
        "metadata": {},
        "cells": [
            {
                "cell_type": "markdown",
                "source": "# Custom header for MyCompany",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'import datetime\nimport great_expectations as ge\nimport great_expectations.jupyter_ux\nfrom great_expectations.checkpoint import LegacyCheckpoint\nfrom great_expectations.data_context.types.resource_identifiers import (\n    ValidationResultIdentifier,\n)\n\ncontext = ge.data_context.DataContext()\n\n# Feel free to change the name of your suite here. Renaming this will not\n# remove the other one.\nexpectation_suite_name = "critical"\nsuite = context.get_expectation_suite(expectation_suite_name)\nsuite.expectations = []\n\nbatch_kwargs = {"path": "../../3.csv", "datasource": "3"}\nbatch = context.get_batch(batch_kwargs, suite)\nbatch.head()',
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "## Create & Edit Expectations\n\nAdd expectations by calling specific expectation methods on the `batch` object. They all begin with `.expect_` which makes autocompleting easy using tab.\n\nYou can see all the available expectations in the **[expectation glossary](https://docs.greatexpectations.io/en/latest/reference/glossary_of_expectations.html?utm_source=notebook&utm_medium=create_expectations)**.",
                "metadata": {},
            },
            {
                "cell_type": "markdown",
                "source": "### Table Expectation(s)",
                "metadata": {},
            },
            {
                "cell_type": "markdown",
                "source": "No table level expectations are in this suite. Feel free to add some here. They all begin with `batch.expect_table_...`.",
                "metadata": {},
            },
            {
                "cell_type": "markdown",
                "source": "### Column Expectation(s)\nwrite your column expectations here",
                "metadata": {},
            },
            {"cell_type": "markdown", "source": "#### `npi`", "metadata": {}},
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.expect_column_values_to_not_be_null(column="npi")',
                "outputs": [],
            },
            {"cell_type": "markdown", "source": "#### `provider_type`", "metadata": {}},
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.expect_column_values_to_not_be_null(column="provider_type")',
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "## Save & Review Your Expectations\n\nLet's save the expectation suite as a JSON file in the `great_expectations/expectations` directory of your project.\nIf you decide not to save some expectations that you created, use [remove_expectation method](https://docs.greatexpectations.io/en/latest/autoapi/great_expectations/data_asset/index.html?highlight=remove_expectation&utm_source=notebook&utm_medium=edit_expectations#great_expectations.data_asset.DataAsset.remove_expectation).\n\nLet's now rebuild your Data Docs, which helps you communicate about your data with both machines and humans.",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.save_expectation_suite(discard_failed_expectations=False)\nrun_id = {\n    "run_name": "some_string_that_uniquely_identifies_this_run",  # insert your own run_name here\n    "run_time": datetime.datetime.now(datetime.timezone.utc),\n}\nresults = context.run_validation_operator(\n    "local", assets_to_validate=[batch], run_id=run_id\n)\nvalidation_result_identifier = results.list_validation_result_identifiers()[0]\ncontext.build_data_docs(site_names=["site_local"])\ncontext.open_data_docs(validation_result_identifier, site_name="site_local")',
                "outputs": [],
            },
        ],
    }
    del expected["nbformat_minor"]
    del obs["nbformat_minor"]
    for obs_cell, expected_cell in zip(obs["cells"], expected["cells"]):
        obs_cell.pop("id", None)
        assert obs_cell == expected_cell
    assert obs == expected
