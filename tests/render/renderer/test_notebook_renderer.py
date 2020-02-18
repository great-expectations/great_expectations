import json
import pytest
from six import PY2

from great_expectations.core import ExpectationSuiteSchema
from great_expectations.render.renderer.notebook_renderer import NotebookRenderer


@pytest.fixture
def critical_suite():
    """
    This hand made fixture has a wide range of expectations, and has a mix of
    metadata including an SampleExpectationsDatasetProfiler entry.
    """
    schema = ExpectationSuiteSchema(strict=True)
    critical_suite = {
        "expectation_suite_name": "critical",
        "meta": {"great_expectations.__version__": "0.7.10"},
        "expectations": [
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "npi"},
                "meta": {
                    "question": True,
                    "Notes": "There are empty strings that should probably be nulls",
                    'SampleExpectationsDatasetProfiler': {
                        'confidence': 'very low'
                    }
                },
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "provider_type"},
            },
        ],
        "data_asset_type": "Dataset",
    }
    return schema.loads(json.dumps(critical_suite)).data


@pytest.fixture
def warning_suite():
    """
    This hand made fixture has a wide range of expectations, and has a mix of
    metadata including SampleExpectationsDatasetProfiler entries.
    """
    schema = ExpectationSuiteSchema(strict=True)
    warning_suite = {
        "expectation_suite_name": "warning",
        "meta": {"great_expectations.__version__": "0.8.4.post0"},
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
                "meta": {
                    'SampleExpectationsDatasetProfiler': {
                        'confidence': 'very low'
                    }
                }
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "provider_type"},
                "meta": {
                    'SampleExpectationsDatasetProfiler': {
                        'confidence': 'very low'
                    }
                }
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
    return schema.loads(json.dumps(warning_suite)).data


@pytest.mark.xfail(condition=PY2, reason="legacy python")
def test_simple_suite(critical_suite):
    obs = NotebookRenderer().render(critical_suite, {"path": "foo/data"})
    assert isinstance(obs, dict)
    expected = {
        "nbformat": 4,
        "nbformat_minor": 4,
        "metadata": {},
        "cells": [
            {
                "cell_type": "markdown",
                "source": "# Edit Your Expectation Suite\nUse this notebook to recreate and modify your expectation suite for:\n\n**Expectation Suite Name**: `critical`\n\nWe'd love it if you **reach out to us on** the [**Great Expectations Slack Channel**](https://greatexpectations.io/slack)",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'from datetime import datetime\nimport great_expectations as ge\nimport great_expectations.jupyter_ux\nfrom great_expectations.data_context.types.resource_identifiers import ValidationResultIdentifier\n\ncontext = ge.data_context.DataContext()\n\n# Feel free to change the name of your suite here. Renaming this will not\n# remove the other one.\nexpectation_suite_name = "critical"\ncontext.create_expectation_suite(\n    expectation_suite_name,\n    overwrite_existing=True)\n\nbatch_kwargs = {\'path\': \'../../foo/data\'}\nbatch = context.get_batch(batch_kwargs, expectation_suite_name)\nbatch.head()',
                "outputs": []
            },
            {
                "cell_type": "markdown",
                "source": "## Create & Edit Expectations\n\nAdd expectations by calling specific expectation methods on the `batch` object. They all begin with `.expect_` which makes autocompleting easy using tab.\n\nYou can see all the available expectations in the **[expectation glossary](https://docs.greatexpectations.io/en/latest/expectation_glossary.html?utm_source=notebook&utm_medium=create_expectations)**.",
                "metadata": {},
            },
            {
                "cell_type": "markdown",
                "source": "### Table Expectation(s)",
                "metadata": {},
            },
            {
                "cell_type": "markdown",
                "source": "No table level expectations are in this suite. Feel free to add some here. The all begin with `batch.expect_table_...`.",
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
                "source": "batch.expect_column_values_to_not_be_null(\n    'npi',\n    meta={\n        'question': True,\n        'Notes': 'There are empty strings that should probably be nulls'})",
                "outputs": [],
            },
            {"cell_type": "markdown", "source": "#### `provider_type`", "metadata": {}},
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "batch.expect_column_values_to_not_be_null('provider_type')",
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "## Save & Review Your Expectations\n\nLet's save the expectation suite as a JSON file in the `great_expectations/expectations` directory of your project.\nIf you decide not to save some expectations that you created, use [remove_expectaton method](https://docs.greatexpectations.io/en/latest/module_docs/data_asset_module.html?highlight=remove_expectation&utm_source=notebook&utm_medium=edit_expectations#great_expectations.data_asset.data_asset.DataAsset.remove_expectation).\n\nLet's now rebuild your Data Docs, which helps you communicate about your data with both machines and humans.",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": """\
batch.save_expectation_suite(discard_failed_expectations=False)

# Let's make a simple sortable timestamp. Note this could come from your pipeline runner.
run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%S.%fZ")

results = context.run_validation_operator("action_list_operator", assets_to_validate=[batch], run_id=run_id)
expectation_suite_identifier = list(results["details"].keys())[0]
validation_result_identifier = ValidationResultIdentifier(
    expectation_suite_identifier=expectation_suite_identifier,
    batch_identifier=batch.batch_kwargs.to_id(),
    run_id=run_id
)
context.build_data_docs()
context.open_data_docs(validation_result_identifier)""",
                "outputs": [],
            },
        ],
    }
    del expected["nbformat_minor"]
    del obs["nbformat_minor"]
    for obs_cell, expected_cell in zip(obs["cells"], expected["cells"]):
        assert obs_cell == expected_cell
    assert obs == expected


def test_batch_kwarg_path_relative_is_modified_and_found_in_a_code_cell(critical_suite):
    obs = NotebookRenderer().render(critical_suite, {"path": "foo/data"})
    assert isinstance(obs, dict)
    found_expected = False
    for cell in obs["cells"]:
        if cell["cell_type"] == "code":
            source_code = cell["source"]
            print(source_code)
            if "batch_kwargs = {'path': '../../foo/data'}" in source_code:
                found_expected = True
                break

    assert found_expected


def test_batch_kwarg_path_relative_dot_slash_is_modified_and_found_in_a_code_cell(
    critical_suite,
):
    obs = NotebookRenderer().render(critical_suite, {"path": "./foo/data"})
    assert isinstance(obs, dict)
    found_expected = False
    for cell in obs["cells"]:
        if cell["cell_type"] == "code":
            source_code = cell["source"]
            print(source_code)
            if "batch_kwargs = {'path': '../.././foo/data'}" in source_code:
                found_expected = True
                break

    assert found_expected


def test_batch_kwarg_path_absolute_is_not_modified_and_is_found_in_a_code_cell(
    critical_suite,
):
    obs = NotebookRenderer().render(critical_suite, {"path": "/home/user/foo/data"})
    assert isinstance(obs, dict)
    found_expected = False
    for cell in obs["cells"]:
        if cell["cell_type"] == "code":
            source_code = cell["source"]
            print(source_code)
            if "batch_kwargs = {'path': '/home/user/foo/data'}" in source_code:
                found_expected = True
                break

    assert found_expected


@pytest.mark.xfail(condition=PY2, reason="legacy python")
def test_complex_suite(warning_suite):
    obs = NotebookRenderer().render(warning_suite, {"path": "foo/data"})
    assert isinstance(obs, dict)
    expected = {
        "nbformat": 4,
        "nbformat_minor": 4,
        "metadata": {},
        "cells": [
            {
                "cell_type": "markdown",
                "source": "# Edit Your Expectation Suite\nUse this notebook to recreate and modify your expectation suite for:\n\n**Expectation Suite Name**: `warning`\n\nWe'd love it if you **reach out to us on** the [**Great Expectations Slack Channel**](https://greatexpectations.io/slack)",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'from datetime import datetime\nimport great_expectations as ge\nimport great_expectations.jupyter_ux\nfrom great_expectations.data_context.types.resource_identifiers import ValidationResultIdentifier\n\ncontext = ge.data_context.DataContext()\n\n# Feel free to change the name of your suite here. Renaming this will not\n# remove the other one.\nexpectation_suite_name = "warning"\ncontext.create_expectation_suite(\n    expectation_suite_name,\n    overwrite_existing=True)\n\nbatch_kwargs = {\'path\': \'../../foo/data\'}\nbatch = context.get_batch(batch_kwargs, expectation_suite_name)\nbatch.head()',
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "## Create & Edit Expectations\n\nAdd expectations by calling specific expectation methods on the `batch` object. They all begin with `.expect_` which makes autocompleting easy using tab.\n\nYou can see all the available expectations in the **[expectation glossary](https://docs.greatexpectations.io/en/latest/expectation_glossary.html?utm_source=notebook&utm_medium=create_expectations)**.",
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
                "source": "batch.expect_column_values_to_not_be_null('npi')",
                "outputs": [],
            },
            {"cell_type": "markdown", "source": "#### `provider_type`", "metadata": {}},
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "batch.expect_column_values_to_not_be_null('provider_type')",
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
                "source": "batch.expect_column_values_to_not_be_null('nppes_provider_last_org_name')",
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
                "source": "batch.expect_column_values_to_be_in_set(\n    'nppes_provider_gender', value_set=['M', 'F', ''])",
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
                "source": "batch.expect_column_values_to_not_be_null('nppes_entity_code')",
                "outputs": [],
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "batch.expect_column_values_to_be_in_set(\n    'nppes_entity_code', value_set=['I', 'O'])",
                "outputs": [],
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "batch.expect_column_kl_divergence_to_be_less_than(\n    'nppes_entity_code', partition_object={\n        'values': [\n            'I', 'O'], 'weights': [\n                0.9431769750233306, 0.056823024976669335]}, threshold=0.1)",
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
                "source": "batch.expect_column_values_to_be_in_set(\n    'nppes_provider_state',\n    value_set=[\n        'AL',\n        'AK',\n        'AZ',\n        'AR',\n        'CA',\n        'CO',\n        'CT',\n        'DE',\n        'FL',\n        'GA',\n        'HI',\n        'ID',\n        'IL',\n        'IN',\n        'IA',\n        'KS',\n        'KY',\n        'LA',\n        'ME',\n        'MD',\n        'MA',\n        'MI',\n        'MN',\n        'MS',\n        'MO',\n        'MT',\n        'NE',\n        'NV',\n        'NH',\n        'NJ',\n        'NM',\n        'NY',\n        'NC',\n        'ND',\n        'OH',\n        'OK',\n        'OR',\n        'PA',\n        'RI',\n        'SC',\n        'SD',\n        'TN',\n        'TX',\n        'UT',\n        'VT',\n        'VA',\n        'WA',\n        'WV',\n        'WI',\n        'WY',\n        'DC',\n        'PR',\n        'AE',\n        'VI'],\n    mostly=0.999)",
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
                "source": "batch.expect_column_values_to_not_be_null('medicare_participation_indicator')",
                "outputs": [],
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "batch.expect_column_values_to_be_in_set(\n    'medicare_participation_indicator', value_set=['Y', 'N'])",
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
                "source": "batch.expect_column_values_to_not_be_null('number_of_hcpcs')",
                "outputs": [],
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "batch.expect_column_values_to_be_between(\n    'number_of_hcpcs',\n    min_value=0,\n    max_value=500,\n    mostly=0.999)",
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
                "source": "batch.expect_column_values_to_not_be_null('total_unique_benes')",
                "outputs": [],
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "batch.expect_column_values_to_be_between(\n    'total_unique_benes',\n    min_value=0,\n    max_value=2000,\n    mostly=0.95)",
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
                "source": "batch.expect_column_values_to_be_null('med_suppress_indicator', mostly=0.85)",
                "outputs": [],
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "batch.expect_column_values_to_be_in_set(\n    'med_suppress_indicator', value_set=['#', '*'])",
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
                "source": "batch.expect_column_values_to_be_between(\n    'beneficiary_average_age',\n    min_value=40,\n    max_value=90,\n    mostly=0.995)",
                "outputs": [],
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "batch.expect_column_kl_divergence_to_be_less_than(\n    'beneficiary_average_age',\n    partition_object={\n        'bins': [\n            8,\n            16.5,\n            25,\n            33.5,\n            42,\n            50.5,\n            59,\n            67.5,\n            76,\n            84.5,\n            93],\n        'weights': [\n            0.00025259576594384474,\n            0.00013318685840675451,\n            0.0009653750909344757,\n            0.0012363414580378728,\n            0.01081660996274442,\n            0.030813927854975127,\n            0.13495227317818748,\n            0.6919590041664524,\n            0.1244213260634741,\n            0.004449359600843578]},\n    threshold=0.9)",
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
                "source": "batch.expect_column_values_to_be_between(\n    'total_submitted_chrg_amt',\n    min_value=2000,\n    max_value=5000000,\n    mostly=0.98)",
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
                "source": "batch.expect_column_values_to_not_be_null(\n    'nppes_provider_first_name', mostly=0.9)",
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
                "source": "batch.expect_column_values_to_match_regex(\n    'nppes_provider_zip', regex=r'^\\d*$', mostly=0.999)",
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "## Save & Review Your Expectations\n\nLet's save the expectation suite as a JSON file in the `great_expectations/expectations` directory of your project.\nIf you decide not to save some expectations that you created, use [remove_expectaton method](https://docs.greatexpectations.io/en/latest/module_docs/data_asset_module.html?highlight=remove_expectation&utm_source=notebook&utm_medium=edit_expectations#great_expectations.data_asset.data_asset.DataAsset.remove_expectation).\n\nLet's now rebuild your Data Docs, which helps you communicate about your data with both machines and humans.",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": """\
batch.save_expectation_suite(discard_failed_expectations=False)

# Let's make a simple sortable timestamp. Note this could come from your pipeline runner.
run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%S.%fZ")

results = context.run_validation_operator("action_list_operator", assets_to_validate=[batch], run_id=run_id)
expectation_suite_identifier = list(results["details"].keys())[0]
validation_result_identifier = ValidationResultIdentifier(
    expectation_suite_identifier=expectation_suite_identifier,
    batch_identifier=batch.batch_kwargs.to_id(),
    run_id=run_id
)
context.build_data_docs()
context.open_data_docs(validation_result_identifier)""",
                "outputs": [],
            },
        ],
    }
    del expected["nbformat_minor"]
    del obs["nbformat_minor"]
    for obs_cell, expected_cell in zip(obs["cells"], expected["cells"]):
        assert obs_cell == expected_cell
    assert obs == expected
