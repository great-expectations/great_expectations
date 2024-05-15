import re
from pathlib import Path
from typing import Any, Dict, List

# https://setuptools.pypa.io/en/latest/pkg_resources.html
import pkg_resources  # noqa: TID251: TODO: switch to poetry
from setuptools import find_packages, setup


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
        "databricks",  # https://github.com/databricks/databricks-sql-python/blob/main/pyproject.toml
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
        ), f"The extras requirements dir ({requirements_dir}) contains files that do not adhere to the following format: requirements-dev-*.txt"  # noqa: E501
        key = match.group(1)
        if key in ignore_keys:
            continue
        with open(file_path) as f:
            parsed = [str(req) for req in pkg_resources.parse_requirements(f)]
            results[key] = parsed

    lite = results.pop("lite")
    contrib = results.pop("contrib")
    docs_test = results.pop("api-docs-test")
    arrow = results["arrow"]
    results["boto"] = [req for req in lite if req.startswith("boto")]
    results["sqlalchemy2"] = [req for req in lite if req.startswith("sqlalchemy")]
    results["test"] = lite + contrib + docs_test + arrow

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


def get_config(
    packages: List[str],
    long_description: str,
) -> Dict[str, Any]:
    package_name: str = packages[0]
    assert package_name in (
        "great_expectations",
        "great_expectations_v1",
    ), f"Unexpected package name: {package_name}"

    if package_name == "great_expectations_v1":
        long_description = f"v1 mirror. {long_description}"

    return {
        "description": "Always know what to expect from your data.",
        "author": "The Great Expectations Team",
        "url": "https://greatexpectations.io",
        "download_url": "https://github.com/great-expectations/great_expectations",
        "author_email": "team@greatexpectations.io",
        "version": "1.0.0.a3",  # TODO: make this dynamic
        "install_requires": required,
        "extras_require": get_extras_require(),
        "packages": packages,
        "entry_points": {
            "console_scripts": [
                f"great_expectations={package_name}.cli:main",
            ]
        },
        "package_data": {package_name: ["**/py.typed", "**/*.pyi"]},
        "name": package_name,
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


if __name__ == "__main__":
    packages = find_packages(exclude=["contrib*", "docs*", "tests*", "examples*", "scripts*"])
    config = get_config(packages=packages, long_description=long_description)
    setup(**config)
