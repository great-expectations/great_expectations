import pytest
from great_expectations.render.renderer.notebook_renderer import NotebookRenderer


@pytest.fixture
def critical_suite():
    critical_suite = {
        "data_asset_name": "edw/default/pre_prod_staging.staging_npi",
        "expectation_suite_name": "critical",
        "meta": {"great_expectations.__version__": "0.7.10"},
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
    return critical_suite


@pytest.fixture
def warning_suite():
    return {
        "data_asset_name": "edw/default/pre_prod_staging.staging_npi",
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
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "provider_type"},
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


def test_simple_suite(critical_suite):
    obs = NotebookRenderer().render(critical_suite)
    assert isinstance(obs, dict)
    expected = {
        "nbformat": 4,
        "nbformat_minor": 2,
        "metadata": {},
        "cells": [
            {
                "cell_type": "markdown",
                "source": "# Create & Edit Expectation Suite\n### Data Asset: `edw/default/pre_prod_staging.staging_npi`\n### Expecation Suite: `critical`\n\nUse this notebook to recreate and modify your expectation suite.\n",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'import great_expectations as ge\n\nproject_root = "/Users/taylor/repos/demo_public_data_test/great_expectations"\ncontext = ge.data_context.DataContext(project_root)\n# context.get_available_data_asset_names()',
                "outputs": [],
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": '# If you would like to validate an entire table or view in your database\'s default schema:\nbatch_kwargs = {\'table\': "YOUR_TABLE"}\n\n# If you would like to validate an entire table or view from a non-default schema in your database:\nbatch_kwargs = {\'table\': "staging_npi", "schema": "pre_prod_staging"}\n\nbatch = context.get_batch("edw/default/pre_prod_staging.staging_npi", "critical", batch_kwargs)',
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "## Table Expectation(s)",
                "metadata": {},
            },
            {
                "cell_type": "markdown",
                "source": "No table level expectations are in this suite. Feel free to add some.",
                "metadata": {},
            },
            {
                "cell_type": "markdown",
                "source": "## Column Expectation(s)",
                "metadata": {},
            },
            {"cell_type": "markdown", "source": "### `npi`", "metadata": {}},
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "batch.expect_column_values_to_not_be_null('npi')",
                "outputs": [],
            },
            {"cell_type": "markdown", "source": "### `provider_type`", "metadata": {}},
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "batch.expect_column_values_to_not_be_null('provider_type')",
                "outputs": [],
            },
        ],
    }
    assert obs == expected


def test_complex_suite(warning_suite):
    obs = NotebookRenderer().render(warning_suite)
    assert isinstance(obs, dict)
    expected = {
        "nbformat": 4,
        "nbformat_minor": 2,
        "metadata": {},
        "cells": [
            {
                "cell_type": "markdown",
                "source": "# Create & Edit Expectation Suite\n### Data Asset: `edw/default/pre_prod_staging.staging_npi`\n### Expecation Suite: `warning`\n\nUse this notebook to recreate and modify your expectation suite.\n",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'import great_expectations as ge\n\nproject_root = "/Users/taylor/repos/demo_public_data_test/great_expectations"\ncontext = ge.data_context.DataContext(project_root)\n# context.get_available_data_asset_names()',
                "outputs": [],
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": '# If you would like to validate an entire table or view in your database\'s default schema:\nbatch_kwargs = {\'table\': "YOUR_TABLE"}\n\n# If you would like to validate an entire table or view from a non-default schema in your database:\nbatch_kwargs = {\'table\': "staging_npi", "schema": "pre_prod_staging"}\n\nbatch = context.get_batch("edw/default/pre_prod_staging.staging_npi", "warning", batch_kwargs)',
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "## Table Expectation(s)",
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
                "source": "## Column Expectation(s)",
                "metadata": {},
            },
            {"cell_type": "markdown", "source": "### `npi`", "metadata": {}},
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "batch.expect_column_values_to_not_be_null('npi')",
                "outputs": [],
            },
            {"cell_type": "markdown", "source": "### `provider_type`", "metadata": {}},
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "batch.expect_column_values_to_not_be_null('provider_type')",
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "### `nppes_provider_last_org_name`",
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
                "source": "### `nppes_provider_gender`",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "batch.expect_column_values_to_be_in_set('nppes_provider_gender', value_set=['M', 'F', ''])",
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "### `nppes_entity_code`",
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
                "source": "batch.expect_column_values_to_be_in_set('nppes_entity_code', value_set=['I', 'O'])",
                "outputs": [],
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "batch.expect_column_kl_divergence_to_be_less_than('nppes_entity_code', partition_object={'values': ['I', 'O'], 'weights': [0.9431769750233306, 0.056823024976669335]}, threshold=0.1)",
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "### `nppes_provider_state`",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "batch.expect_column_values_to_be_in_set('nppes_provider_state', value_set=['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC', 'PR', 'AE', 'VI'], mostly=0.999)",
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "### `medicare_participation_indicator`",
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
                "source": "batch.expect_column_values_to_be_in_set('medicare_participation_indicator', value_set=['Y', 'N'])",
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "### `number_of_hcpcs`",
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
                "source": "batch.expect_column_values_to_be_between('number_of_hcpcs', min_value=0, max_value=500, mostly=0.999)",
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "### `total_unique_benes`",
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
                "source": "batch.expect_column_values_to_be_between('total_unique_benes', min_value=0, max_value=2000, mostly=0.95)",
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "### `med_suppress_indicator`",
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
                "source": "batch.expect_column_values_to_be_in_set('med_suppress_indicator', value_set=['#', '*'])",
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "### `beneficiary_average_age`",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "batch.expect_column_values_to_be_between('beneficiary_average_age', min_value=40, max_value=90, mostly=0.995)",
                "outputs": [],
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "batch.expect_column_kl_divergence_to_be_less_than('beneficiary_average_age', partition_object={'bins': [8, 16.5, 25, 33.5, 42, 50.5, 59, 67.5, 76, 84.5, 93], 'weights': [0.00025259576594384474, 0.00013318685840675451, 0.0009653750909344757, 0.0012363414580378728, 0.01081660996274442, 0.030813927854975127, 0.13495227317818748, 0.6919590041664524, 0.1244213260634741, 0.004449359600843578]}, threshold=0.9)",
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "### `total_submitted_chrg_amt`",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "batch.expect_column_values_to_be_between('total_submitted_chrg_amt', min_value=2000, max_value=5000000, mostly=0.98)",
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "### `nppes_provider_first_name`",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "batch.expect_column_values_to_not_be_null('nppes_provider_first_name', mostly=0.9)",
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "### `nppes_provider_zip`",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "batch.expect_column_values_to_match_regex('nppes_provider_zip', regex='^\\d*$', mostly=0.999)",
                "outputs": [],
            },
        ],
    }
    assert obs == expected
