import json
import pytest

from great_expectations.core import NamespaceAwareExpectationSuiteSchema
from great_expectations.render.renderer.notebook_renderer import NotebookRenderer


@pytest.fixture
def critical_suite():
    schema = NamespaceAwareExpectationSuiteSchema(strict=True)
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
    return schema.loads(json.dumps(critical_suite)).data


@pytest.fixture
def warning_suite():
    schema = NamespaceAwareExpectationSuiteSchema(strict=True)
    warning_suite = {
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
    return schema.loads(json.dumps(warning_suite)).data


def test_simple_suite(critical_suite):
    obs = NotebookRenderer().render(critical_suite, {"path": "foo/data"})
    assert isinstance(obs, dict)
    expected = {
        "nbformat": 4,
        "nbformat_minor": 2,
        "metadata": {},
        "cells": [
            {
                "cell_type": "markdown",
                "source": "# Edit the Scaffolded Expectation Suite\nUse this notebook to recreate and modify your expectation suite for:\n- **Data Asset**: `pre_prod_staging.staging_npi`\n- **Expectation Suite Name**: `critical`\n\nWe'd love it if you **reach out for help on** the [**Great Expectations Slack Channel**](https://greatexpectations.io/slack)",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "import os\nimport json\nfrom datetime import datetime\nimport great_expectations as ge\nimport great_expectations.jupyter_ux",
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "## 1. Get a DataContext\nThis represents your **project** that you just created using `great_expectations init`. [Read more in the tutorial](https://docs.greatexpectations.io/en/latest/tutorials/create_expectations.html?utm_source=notebook&utm_medium=create_expectations#get-a-datacontext-object)",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "context = ge.data_context.DataContext()",
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "## 2. Optional: Rename your Expectation Suite\n\nWe recommend naming your first expectation suite for a table `warning`. Later, as you identify some of the expectations that you add to this suite as critical, you can move these expectations into another suite and call it `failure`.\n",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'expectation_suite_name = "critical" # TODO: replace with your value!\ncontext.create_expectation_suite(data_asset_name="pre_prod_staging.staging_npi", expectation_suite_name=expectation_suite_name, overwrite_existing=True);',
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "## 3. Load a batch of data you want to use to create `Expectations`\n\nTo learn more about batches and `get_batch`, see [this tutorial](https://docs.greatexpectations.io/en/latest/tutorials/create_expectations.html?utm_source=notebook&utm_medium=create_expectations#load-a-batch-of-data-to-create-expectations)\nLet's glance at a bit of your data.",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "batch_kwargs = {'path': '../../foo/data'}\nbatch = context.get_batch(\"pre_prod_staging.staging_npi\", expectation_suite_name, batch_kwargs)\nbatch.head()",
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "## 3. Create & Edit Expectations\n\nWith a batch, you can add expectations by calling specific expectation methods. They all begin with `.expect_` which makes autocompleting easy.\n\nSee available expectations in the [expectation glossary](https://docs.greatexpectations.io/en/latest/reference/expectation_glossary.html?utm_source=notebook&utm_medium=create_expectations).\nYou can also see available expectations by hovering over data elements in DataDocs generated by profiling your dataset. [Read more in the tutorial](https://docs.greatexpectations.io/en/latest/tutorials/create_expectations.html?utm_source=notebook&utm_medium=create_expectations#author-expectations)",
                "metadata": {},
            },
            {
                "cell_type": "markdown",
                "source": "### Table Expectation(s)",
                "metadata": {},
            },
            {
                "cell_type": "markdown",
                "source": "No table level expectations are in this suite. Feel free to add some.",
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
                "source": "## Save & Review Your Expectations\n\nRun the next cell to save the expectation suite as a JSON file in the `great_expectations/expectations` directory of your project.\nIf you decide not to save some expectations that you created, use [remove_expectaton method](https://docs.greatexpectations.io/en/latest/module_docs/data_asset_module.html?highlight=remove_expectation&utm_source=notebook&utm_medium=edit_expectations#great_expectations.data_asset.data_asset.DataAsset.remove_expectation).\n\nLet's now rebuild your Data Docs, which helps you communicate about your data with both machines and humans.",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.save_expectation_suite(discard_failed_expectations=False)\n\n# Let\'s make a simple sortable timestamp. Note this could come from your pipeline runner.\nrun_id = datetime.utcnow().isoformat().replace(":", "") + "Z"\n\nresults = context.run_validation_operator(\n    assets_to_validate=[batch],\n    run_id=run_id,\n    validation_operator_name="action_list_operator",\n)\ncontext.build_data_docs()\ncontext.open_data_docs()',
                "outputs": [],
            },
        ],
    }
    assert obs == expected


def test_complex_suite(warning_suite):
    obs = NotebookRenderer().render(warning_suite, {"path": "foo/data"})
    assert isinstance(obs, dict)
    expected = {
        "nbformat": 4,
        "nbformat_minor": 2,
        "metadata": {},
        "cells": [
            {
                "cell_type": "markdown",
                "source": "# Edit the Scaffolded Expectation Suite\nUse this notebook to recreate and modify your expectation suite for:\n- **Data Asset**: `pre_prod_staging.staging_npi`\n- **Expectation Suite Name**: `warning`\n\nWe'd love it if you **reach out for help on** the [**Great Expectations Slack Channel**](https://greatexpectations.io/slack)",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "import os\nimport json\nfrom datetime import datetime\nimport great_expectations as ge\nimport great_expectations.jupyter_ux",
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "## 1. Get a DataContext\nThis represents your **project** that you just created using `great_expectations init`. [Read more in the tutorial](https://docs.greatexpectations.io/en/latest/tutorials/create_expectations.html?utm_source=notebook&utm_medium=create_expectations#get-a-datacontext-object)",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "context = ge.data_context.DataContext()",
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "## 2. Optional: Rename your Expectation Suite\n\nWe recommend naming your first expectation suite for a table `warning`. Later, as you identify some of the expectations that you add to this suite as critical, you can move these expectations into another suite and call it `failure`.\n",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'expectation_suite_name = "warning" # TODO: replace with your value!\ncontext.create_expectation_suite(data_asset_name="pre_prod_staging.staging_npi", expectation_suite_name=expectation_suite_name, overwrite_existing=True);',
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "## 3. Load a batch of data you want to use to create `Expectations`\n\nTo learn more about batches and `get_batch`, see [this tutorial](https://docs.greatexpectations.io/en/latest/tutorials/create_expectations.html?utm_source=notebook&utm_medium=create_expectations#load-a-batch-of-data-to-create-expectations)\nLet's glance at a bit of your data.",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "batch_kwargs = {'path': '../../foo/data'}\nbatch = context.get_batch(\"pre_prod_staging.staging_npi\", expectation_suite_name, batch_kwargs)\nbatch.head()",
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "## 3. Create & Edit Expectations\n\nWith a batch, you can add expectations by calling specific expectation methods. They all begin with `.expect_` which makes autocompleting easy.\n\nSee available expectations in the [expectation glossary](https://docs.greatexpectations.io/en/latest/reference/expectation_glossary.html?utm_source=notebook&utm_medium=create_expectations).\nYou can also see available expectations by hovering over data elements in DataDocs generated by profiling your dataset. [Read more in the tutorial](https://docs.greatexpectations.io/en/latest/tutorials/create_expectations.html?utm_source=notebook&utm_medium=create_expectations#author-expectations)",
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
                "source": "batch.expect_column_values_to_be_in_set('nppes_provider_gender', value_set=['M', 'F', ''])",
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
                "source": "#### `nppes_provider_state`",
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
                "source": "batch.expect_column_values_to_be_in_set('medicare_participation_indicator', value_set=['Y', 'N'])",
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
                "source": "batch.expect_column_values_to_be_between('number_of_hcpcs', min_value=0, max_value=500, mostly=0.999)",
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
                "source": "batch.expect_column_values_to_be_between('total_unique_benes', min_value=0, max_value=2000, mostly=0.95)",
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
                "source": "batch.expect_column_values_to_be_in_set('med_suppress_indicator', value_set=['#', '*'])",
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
                "source": "#### `total_submitted_chrg_amt`",
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
                "source": "#### `nppes_provider_first_name`",
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
                "source": "#### `nppes_provider_zip`",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": "batch.expect_column_values_to_match_regex('nppes_provider_zip', regex='^\\d*$', mostly=0.999)",
                "outputs": [],
            },
            {
                "cell_type": "markdown",
                "source": "## Save & Review Your Expectations\n\nRun the next cell to save the expectation suite as a JSON file in the `great_expectations/expectations` directory of your project.\nIf you decide not to save some expectations that you created, use [remove_expectaton method](https://docs.greatexpectations.io/en/latest/module_docs/data_asset_module.html?highlight=remove_expectation&utm_source=notebook&utm_medium=edit_expectations#great_expectations.data_asset.data_asset.DataAsset.remove_expectation).\n\nLet's now rebuild your Data Docs, which helps you communicate about your data with both machines and humans.",
                "metadata": {},
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "source": 'batch.save_expectation_suite(discard_failed_expectations=False)\n\n# Let\'s make a simple sortable timestamp. Note this could come from your pipeline runner.\nrun_id = datetime.utcnow().isoformat().replace(":", "") + "Z"\n\nresults = context.run_validation_operator(\n    assets_to_validate=[batch],\n    run_id=run_id,\n    validation_operator_name="action_list_operator",\n)\ncontext.build_data_docs()\ncontext.open_data_docs()',
                "outputs": [],
            },
        ],
    }
    assert obs == expected
