---
title: How to connect to an Athena database
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';
import ConnectionStringAthena from './components/_connection_string_athena.md'

This guide will help you add an Athena instance (or a database) as a <TechnicalTag tag="datasource" text="Datasource" />. This will allow you to <TechnicalTag tag="validation" text="Validate" /> tables and queries within this instance. When you use an Athena Datasource, the validation is done in Athena itself. Your data is not downloaded.

<Prerequisites>

  - [Set up a working deployment of Great Expectations](/docs/guides/setup/setup_overview)
  - Installed the pyathena package for the Athena SQLAlchemy dialect (``pip install "pyathena[SQLAlchemy]"``)

</Prerequisites>

## Steps

### 1. Run the following CLI command to begin the interactive Datasource creation process:

```bash
great_expectations datasource new
```

When prompted to choose from the list of database engines, chose `other`.

### 2. Identify your connection string

<ConnectionStringAthena />

After providing your connection string, you will then be presented with a Jupyter Notebook.

### 3. Follow the steps in the Jupyter Notebook

The Jupyter Notebook will guide you through the remaining steps of creating a Datasource.  Follow the steps in the presented notebook, including entering the connection string in the yaml configuration.

## Additional notes

Environment variables can be used to store the SQLAlchemy URL instead of the file, if preferred - search documentation for "Managing Environment and Secrets".
