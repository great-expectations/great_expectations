import re
from pathlib import Path

import pkg_resources
from setuptools import find_packages, setup

import versioneer


def get_extras_require():
    results = {}
    extra_key_mapping = {
        "aws_secrets": "boto",
        "azure_secrets": "azure",
        "gcp": "bigquery",
        "s3": "boto",
    }
    sqla1x_only_keys = (
        "bigquery",  # https://github.com/googleapis/python-bigquery-sqlalchemy/blob/main/setup.py
        "clickhouse",  # https://github.com/xzkostyan/clickhouse-sqlalchemy/blob/master/setup.py
        "redshift",  # https://github.com/sqlalchemy-redshift/sqlalchemy-redshift/blob/main/setup.py
        "snowflake",  # https://github.com/snowflakedb/snowflake-sqlalchemy/blob/main/setup.cfg
        "teradata",  # https://pypi.org/project/teradatasqlalchemy   https://support.teradata.com/knowledge?id=kb_article_view&sys_kb_id=a5a869149729251ced863fe3f153af27
    )
    sqla_keys = (
        "athena",  # https://github.com/laughingman7743/PyAthena/blob/master/pyproject.toml
        "dremio",  # https://github.com/narendrans/sqlalchemy_dremio/blob/master/setup.py
        "hive",  # https://github.com/dropbox/PyHive/blob/master/setup.py
        "mssql",  # https://github.com/mkleehammer/pyodbc/blob/master/setup.py
        "mysql",  # https://github.com/PyMySQL/PyMySQL/blob/main/pyproject.toml
        "postgresql",  # https://github.com/psycopg/psycopg2/blob/master/setup.py
        "trino",  # https://github.com/trinodb/trino-python-client/blob/master/setup.py
        "vertica",  # https://github.com/bluelabsio/sqlalchemy-vertica-python/blob/master/setup.py
    )
    ignore_keys = (
        "sqlalchemy",
        "test",
        "tools",
        "all-contrib-expectations",
    )

    requirements_dir = "reqs"
    rx_name_part = re.compile(r"requirements-dev-(.*).txt")

    # Use Path() from pathlib so we can make this section of the code OS agnostic.
    # Loop through each requirement file and verify they are named
    # correctly and are in the right location.
    for file_path in Path().glob(f"{requirements_dir}/*.txt"):
        match = rx_name_part.match(file_path.name)
        assert (
            match is not None
        ), f"The extras requirements dir ({requirements_dir}) contains files that do not adhere to the following format: requirements-dev-*.txt"
        key = match.group(1)
        if key in ignore_keys:
            continue
        with open(file_path) as f:
            parsed = [str(req) for req in pkg_resources.parse_requirements(f)]
            results[key] = parsed

    lite = results.pop("lite")
    contrib = results.pop("contrib")
    docs_test = results.pop("api-docs-test")
    cloud = results["cloud"]
    arrow = results["arrow"]
    sqlalchemy = results["sqlalchemy"]
    spark = results["spark"]

    results["boto"] = [req for req in lite if req.startswith("boto")]
    results["sqlalchemy2"] = [req for req in lite if req.startswith("sqlalchemy")]
    results["test"] = lite + contrib + docs_test + cloud + arrow + sqlalchemy + spark

    for new_key, existing_key in extra_key_mapping.items():
        results[new_key] = results[existing_key]
    for key in sqla1x_only_keys:
        results[key] += results["sqlalchemy1"]
    for key in sqla_keys:
        results[key] += results["sqlalchemy2"]

    results.pop("boto")
    results.pop("sqlalchemy1")
    results.pop("sqlalchemy2")
    # all_requirements_set = set()
    # [all_requirements_set.update(vals) for vals in results.values()]
    # results["dev"] = sorted(all_requirements_set)
    return results


# Parse requirements.txt
with open("requirements.txt") as f:
    required = f.read().splitlines()

long_description = "Always know what to expect from your data. (See https://github.com/great-expectations/great_expectations for full description)."

config = {
    "description": "Always know what to expect from your data.",
    "author": "The Great Expectations Team",
    "url": "https://greatexpectations.io",
    "download_url": "https://github.com/great-expectations/great_expectations",
    "author_email": "team@greatexpectations.io",
    "version": versioneer.get_version(),
    "cmdclass": versioneer.get_cmdclass(),
    "install_requires": required,
    "extras_require": get_extras_require(),
    "packages": find_packages(
        exclude=["contrib*", "docs*", "tests*", "examples*", "scripts*"]
    ),
    "entry_points": {
        "console_scripts": [
            "great_expectations=great_expectations.cli:main",
            "gx-agent=great_expectations.agent:run_agent",
        ]
    },
    "package_data": {"great_expectations": ["**/py.typed", "**/*.pyi"]},
    "name": "great_expectations",
    "long_description": long_description,
    "license": "Apache-2.0",
    "keywords": "data science testing pipeline data quality dataquality validation datavalidation",
    "include_package_data": True,
    "python_requires": ">=3.8",
    "classifiers": [
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Intended Audience :: Other Audience",
        "Topic :: Scientific/Engineering",
        "Topic :: Software Development",
        "Topic :: Software Development :: Testing",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
}

setup(**config)
