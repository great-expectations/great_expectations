---
title: How to contribute a new SQLAlchemy-compatible Datasource
---

import Prerequisites from '../../guides/expectations/creating_custom_expectations/components/prerequisites.jsx';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

This guide will help you add a new SQLAlchemy-compatible <TechnicalTag tag="datasource" text="Datasource" />.

<Prerequisites>

- A Python package for the SQLAlchemy dialect you want to add as a Datasource

</Prerequisites>

## Code changes

### If there is a Docker image available for your dialect...

> See mssql, mysql, postgresql, & trino

- Create a new directory in `assets/docker/` and add a `docker-compose.yml` file
- Run `docker-compose up` from the directory
- Connect to the db
- Edit the `azure-pipelines-docs-integration.yml` file
    - Add to the `resources: -> containers:` section at the top
    - Add to `services:` section for the `test_docs` job
    - Add your new flag to the pytest command in the `test_docs` section (i.e. `--trino`)

### If your dialect is a cloud service...

> See athena, bigquery, redshift, snowflake

- Edit the `azure-pipelines-docs-integration.yml` file
    - Add your new flag to the pytest command in the `test_docs` section (i.e. `--snowflake`)
    - Add environment variables for the components of connection string that will be used for
      integration testing
        - Ask the Admin to add these variables to the azure pipelines

### TODO

> The various funcs in various files that need to be modified and how/why.

## Adding docs

### Copy/edit example files

```
% cd tests/integration/docusaurus/connecting_to_your_data/database
```

Copy a specific `xx_python_example.py` and corresponding `xx_yaml_example.py` and edit

> Going to copy from postgres for trino and adjust... then vimdiff postgres trino mysql

```
% cp postgres_python_example.py trino_python_example.py

% cp postgres_yaml_example.py trino_yaml_example.py

% vimdiff -R postgres_python_example.py trino_python_example.py mysql_python_example.py
```

- Set the correct `CONNECTION_STRING` near the top of the file (that can connect to the running docker container)
    - if your dialect is a cloud service, be sure to use `os.environ.get(...)` to get the appropriate
      parts of the connection string from environment variables before setting `CONNECTION_STRING`
- In the `datasource_config` dict, set the new `name` and `connection_string` (template for connection
  string, NOT the contents of the `CONNECTION_STRING` variable)
- In the RuntimeBatchRequest, update the `datasource_name` and the `runtime_parameters`
- In the BatchRequest, update the `datasource_name` (same) and the `data_asset_name` (table name
  in the `runtime_parameters` query)
- In the final assert statements, update the table name and datasource name strings

```
% vimdiff postgres_yaml_example.py trino_yaml_example.py mysql_yaml_example.py
```

- Set the correct `CONNECTION_STRING` near the top of the file (that can connect to the running docker container)
    - if your dialect is a cloud service, be sure to use `os.environ.get(...)` to get the appropriate
      parts of the connection string from environment variables before setting `CONNECTION_STRING`
- In the `datasource_yaml` string, set the new `name` and `connection_string` (template for connection
  string, NOT the contents of the `CONNECTION_STRING` variable)
- In the RuntimeBatchRequest, update the `datasource_name` and the `runtime_parameters`
- In the BatchRequest, update the `datasource_name` (same) and the `data_asset_name` (table name
  in the `runtime_parameters` query)
- In the final assert statements, update the table name and datasource name strings

### Copy/edit doc file

```
% cd -

% cd docs/guides/connecting_to_your_data/database
```

Copy a specific `xx.md` file and edit

> Going to copy from postgres for trino and adjust... then vimdiff postgres trino mysql

```
% cp postgres.md trino.md

% vimdiff -R postgres.md trino.md mysql.md
```

- Use correct db name in the title and the prerequisites section
- Update the `pip install` line(s) to use the correct packages
- Update the connection string template in the "Add credentials" section
- For each of the `python file=` lines that specify code snippets, use the appropriate example file names
- In the "Additional Notes" section near the end, update the file names and links

### Edit index file

```
% cd ..

% vim index.md
```

In the "Database" section, add a new line for your database

### Edit sidebar.js

```
% cd ..

% vim sidebar.js
```

- Search for `label: 'Database'`
    - In the `items:` section right below, add your database

### Build docs site locally and confirm that the correct lines are

```
% cd docs

% yarn install

% yarn start
```

Visit <http://localhost:3000/docs/guides/connecting_to_your_data/connect_to_data_overview> in your browser
and click on the "Database" section under "Step 2: Connect to data" in the side nav

- Make sure your newly added doc appears in that section
