Requirements
============

- **requirements.txt**: All requirements that are included when you `pip install great_expectations` or `pip install .`
- **reqs/requirements-dev-lite.txt**: The least amount of extra requirements needed to run all the basic pytest tests
    - the sqlalchemy version specified in here is included in the requirements for each sqlalchemy dialect's `extras_require` key, if the dialect is compatible with sqlalchemy 2.x
    - the boto3 version specified in this file is used in the `extras_require` keys `aws_secrets` and `s3` (`pip install ".[aws_secrets]"` or `pip install ".[s3]"`)
- **reqs/requirements-dev-sqlalchemy1.txt**: A sqlalchemy 1.x version to be included in the requirements for each sqlalchemy dialect's `extras_require` key, if the dialect is not yet compatible with 2.x
- **reqs/requirements-dev-contrib.txt**: Mostly linting tools like `black`, `mypy`, and `ruff`
- **reqs/requirements-dev-all-contrib-expectations.txt**: The requirements for all of the contrib Expectations; only used in CI when building the Expectation Gallery
- **reqs/requirements-dev-api-docs-test.txt**: Just `docstring-parser` package needed for generating API docs
- **reqs/requirements-dev-cloud.txt**: Just `pika` package; `pip install ".[cloud]"`
- **reqs/requirements-dev-spark.txt**: `pip install ".[spark]"`
- **reqs/requirements-dev-arrow.txt**: `pip install ".[arrow]"`
- **reqs/requirements-dev-azure.txt**: `pip install ".[azure]"` and `pip install ".[azure_secrets]"`
- **reqs/requirements-dev-excel.txt**: `pip install ".[excel]"`
- **reqs/requirements-dev-pagerduty.txt**: `pip install ".[pagerduty]"`
- **reqs/requirements-dev-tools.txt**: jupyter, matplotlib, and scikit-learn; only meant to be used in the Dockerfile.tests file

## Collections (of other requirements files)

- **reqs/requirements-dev-test.txt**: Requirements that are included when you `pip install ".[test]"`; the recommended starting point for contributors local development; many CI jobs have their environment setup with `pip install -c constraints-dev.txt "[test, some-backend]"`
    - reqs/requirements-dev-arrow.txt
    - reqs/requirements-dev-lite.txt
    - reqs/requirements-dev-contrib.txt
    - reqs/requirements-dev-api-docs-test.txt
    - reqs/requirements-dev-cloud.txt
- **reqs/requirements-dev-sqlalchemy.txt**: Includes every requirements file for every sqlalchemy dialect; should not do `pip install -r reqs/requirements-dev-sqlalchemy.txt`
    - reqs/requirements-dev-lite.txt
    - reqs/requirements-dev-sqlalchemy1.txt
    - reqs/requirements-dev-athena.txt: `pip install ".[athena]"`
    - reqs/requirements-dev-bigquery.txt: `pip install ".[bigquery]"` and `pip install ".[gcp]"`
    - reqs/requirements-dev-clickhouse.txt: `pip install ".[clickhouse]"`
    - reqs/requirements-dev-dremio.txt: `pip install ".[dremio]"`
    - reqs/requirements-dev-hive.txt: `pip install ".[hive]"`
    - reqs/requirements-dev-mssql.txt: `pip install ".[mssql]"`
    - reqs/requirements-dev-mysql.txt: `pip install ".[mysql]"`
    - reqs/requirements-dev-postgresql.txt: `pip install ".[postgresql]"`
    - reqs/requirements-dev-redshift.txt: `pip install ".[redshift]"`
    - reqs/requirements-dev-snowflake.txt: `pip install ".[snowflake]"`
    - reqs/requirements-dev-teradata.txt: `pip install ".[teradata]"`
    - reqs/requirements-dev-trino.txt: `pip install ".[trino]"`
    - reqs/requirements-dev-vertica.txt: `pip install ".[vertica]"`
- **requirements-types.txt**: Used in 2 static type checking CI jobs
    - requirements.txt
    - reqs/requirements-dev-sqlalchemy.txt
    - reqs/requirements-dev-spark.txt
    - reqs/requirements-dev-azure.txt
    - reqs/requirements-dev-contrib.txt
    - reqs/requirements-dev-cloud.txt
- **requirements-dev.txt**: Includes pretty much every single requirements file; only meant to be used in the Dockerfile.tests file
    - requirements.txt
    - reqs/requirements-dev-test.txt
    - reqs/requirements-dev-sqlalchemy.txt
    - reqs/requirements-dev-arrow.txt
    - reqs/requirements-dev-azure.txt
    - reqs/requirements-dev-excel.txt
    - reqs/requirements-dev-pagerduty.txt
    - reqs/requirements-dev-spark.txt

## Testing changes to requirements files

Any time you update any of the requirements files in this `reqs` directory or the root of the repo, your're going to want to run the tests in these 2 files and make adjustments.

```
pytest -vv tests/core/usage_statistics/test_package_dependencies.py tests/test_packaging.py
```

Might need to update `great_expectations/core/usage_statistics/package_dependencies.py`.
