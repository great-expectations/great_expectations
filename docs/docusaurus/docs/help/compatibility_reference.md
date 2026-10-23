---
title: Compatibility reference
hide_table_of_contents: true
---

import DataSourceSupportTable from './_data_source_support_table.md';

This page defines the integrations and tools supported by GX Core. It starts with how data sources are placed into support tiers, lists each data source with its tier, and ends with a summary of everything else.

## Data source support tiers

Each data source is placed in one of three tiers. A tier is decided by which criteria a data source meets, and each criterion is a test suite that runs in this repository's continuous integration. A criterion is shown on a data source's row only when that suite is declared for the data source and runs for it.

| Tier | What it requires |
|---|---|
| Fully supported | Both **Every shipped expectation** and **Datasource API contract**. |
| Tested | At least one expectation criterion, **Every shipped expectation** or **Expectation suite**, without the combination that makes a data source Fully supported. |
| Best effort | No published expectation suite claims the data source. GX ships a connection path for it, but nothing here shows expectations running against it. |

The three criteria are:

- **Every shipped expectation** runs a suite with one case for every expectation GX Core ships, each with a configuration that should pass and one that should fail. A case runs on a data source only where it applies to that data source's engine: some expectations have no implementation on an engine, and some cases check something only SQL engines have, such as SQL type names. Passing it proves each case that runs on that data source reaches the right verdict there.
- **Expectation suite** runs a shared or curated set of expectations against the data source with test data in place. Passing it proves that validation returns the right results on that data source for that set of expectations. It runs a chosen set of expectations rather than every shipped expectation.
- **Datasource API contract** runs the fluent datasource API's create, update and persist checks for the data source's connection type, using placeholder settings and with connection testing turned off, so no service is contacted. Passing it proves that a data source of that kind can be created, updated and persisted through the API. It does not show that expectations run against it, so a data source that meets this criterion alone stays in Best effort. The criterion is still shown on its row, so that such a row can be told apart from one that meets nothing.

When a row says "(with exceptions)" beside a criterion, the suite runs for that data source but skips some cases, and the row's Notes column says which ones and why.

A row with several variants, such as Pandas, meets a criterion only when every variant does. A row's Notes column names the variants that fall short.

The Best effort tier says nothing about who wrote or maintains a connection path. Every connection path in it ships in the GX Core package. The tier means that no published expectation suite claims the data source.

### Planned criterion

An additional criterion for the Fully supported tier, beyond the two it requires today, is planned: rendering a validation result that was produced against a live data source into Data Docs. No test suite for it exists yet, so it is not part of any tier today. It affects no data source's placement until it exists. When it does, the Fully supported tier will require it as well, and data sources that do not meet it will move to Tested.

### Moving between tiers

A data source moves between tiers by meeting criteria: its declaration changes and the suite behind the criterion runs in continuous integration. The tier follows from the criteria met, and nothing else decides it.

The declarations live in the public repository:

- The data-source records, under [`tests/integration/test_utils/data_source_config/`](https://github.com/fivetran/great_expectations/tree/develop/tests/integration/test_utils/data_source_config).
- The datasource API contract, in [`tests/datasource/fluent/crud_contract.py`](https://github.com/fivetran/great_expectations/blob/develop/tests/datasource/fluent/crud_contract.py).
- The suite behind **Every shipped expectation**, in [`tests/integration/data_sources_and_expectations/test_gallery_expectation_suite.py`](https://github.com/fivetran/great_expectations/blob/develop/tests/integration/data_sources_and_expectations/test_gallery_expectation_suite.py).
- The suites behind **Expectation suite**, in [`tests/integration/data_sources_and_expectations/expectations/`](https://github.com/fivetran/great_expectations/tree/develop/tests/integration/data_sources_and_expectations/expectations) and [`tests/integration/data_sources_and_expectations/test_curated_backend_suite.py`](https://github.com/fivetran/great_expectations/blob/develop/tests/integration/data_sources_and_expectations/test_curated_backend_suite.py).
- The maintainer guide that describes how to change them, in [`tests/integration/data_sources_and_expectations/README.md`](https://github.com/fivetran/great_expectations/blob/develop/tests/integration/data_sources_and_expectations/README.md).

## Data sources

Where an expectation lists the data sources it supports, it uses the same names as this page. If you know a data source by a different name, this is why the page differs.

{/* The table below is generated from the repository's declarations and is not edited by hand. */}

<DataSourceSupportTable />

{/* The sentence below is written by hand. The table generator neither reads nor writes it. */}

The following data sources have been seen to work with GX Core, but none of them is tested against a running instance in this repository's continuous integration: Athena, AWS Glue, Databricks (Spark), Dremio, EMR Spark, Teradata and Vertica. This is an observation, not a support claim, and no tier is implied. It is written by hand, and the generated table above neither reads nor writes it.

## Other integrations and tools

The tiers above apply to data sources only. The other rows are not produced by a test suite, so they carry no tier.

| Service | GX Core | Notes |
|---|---|---|
| Data sources | See [Data sources](#data-sources) above | Each data source's support tier, the criteria it meets, and notes. |
| Actions| Email<br/>Microsoft Teams<br/>Slack<br/>Custom | We support the general workflow for creating custom Actions, but cannot help troubleshoot the domain-specific logic within a custom Action. |
| Credential stores | Environment variables<br/>`config_variables.yml` |  |
| Orchestrators | Airflow version 2.9.0+ | Although only Airflow is supported, GX Core should work with any orchestrator that executes Python code. |
| Operating systems | Mac/Linux | Though GX does not currently support Windows, we've seen users successfully deploy on Windows. |
| Python versions | 3.10 to 3.14 | GX typically follows the [Python release cycle](https://devguide.python.org/versions/). GX currently supports Python 3.10 to 3.14. Python 3.15 and later are not currently supported. The `clickhouse` and `teradata` extras are not supported on Python 3.14: each installs SQLAlchemy 1.4, which does not support Python 3.14.|
| GX library versions | ≥1.0 | Support for 0.18 was deprecated October 25, 2024 and reached end of life on October 1, 2025. |
| Core dependencies | View [current core dependency support](https://github.com/fivetran/great_expectations/blob/develop/requirements.txt) in GitHub | GX typically supports core package dependencies for 2 years after initial release. Exceptions are made to support Amazon Managed Workflows for Apache Airflow (MWAA) - GX typically supports the versions of dependencies that are pinned in [MWAA constraints files](https://docs.aws.amazon.com/mwaa/latest/userguide/airflow-versions.html).  |
| Optional dependencies | View [current optional dependency support](https://github.com/fivetran/great_expectations/tree/develop/reqs) in GitHub | GX typically supports optional package dependencies for 1 year after initial release. Exceptions are made to support Amazon Managed Workflows for Apache Airflow (MWAA) - GX typically supports the versions of dependencies that are pinned in [MWAA constraints files](https://docs.aws.amazon.com/mwaa/latest/userguide/airflow-versions.html). |
| Web browsers | [Google Chrome](https://www.google.com/chrome/)<br/>[Mozilla Firefox](https://www.mozilla.org/en-US/firefox/)<br/>[Apple Safari](https://www.apple.com/safari/)<br/>[Microsoft Edge](https://www.microsoft.com/en-us/edge?ep=82&form=MA13KI&es=24) | Only the latest version of each browser is supported. |

