import os
import re
from pathlib import Path
from typing import List

from setuptools import find_packages, setup

import versioneer

SUPPORTED_PYTHON = ">=3.10,<3.14"


def parse_requirements(file_path: Path) -> List[str]:
    """
    Requirements parser that doesn't depend on pip internals.
    This avoids circular dependencies during package build.

    Args:
        file_path: Path to the requirements file to parse

    Returns:
        List of requirement strings with comments and empty lines removed
    """
    requirements = []
    with open(file_path) as f:
        for line in f:
            cleaned_line = line.strip()
            # Skip empty lines and comments
            if cleaned_line and not cleaned_line.startswith("#"):
                # Remove inline comments
                requirement_line = cleaned_line
                if "#" in cleaned_line:
                    requirement_line = cleaned_line.split("#")[0].strip()
                requirements.append(requirement_line)
    return requirements


def get_python_requires() -> str:
    """
    If the GX_PYTHON_EXPERIMENTAL environment variable is set,
    return a version with no upper-bound.
    """
    if os.getenv("GX_PYTHON_EXPERIMENTAL"):
        return ">=3.10"
    return SUPPORTED_PYTHON


def get_extras_require():
    results = {}
    extra_key_mapping = {
        "aws_secrets": "boto",
        "azure_secrets": "azure",
        "fabric": "sql-server",
        "gcp": "bigquery",
        "s3": "boto",
    }
    sqla1x_only_keys = (
        "clickhouse",  # https://github.com/xzkostyan/clickhouse-sqlalchemy/blob/master/setup.py
        "teradata",  # https://pypi.org/project/teradatasqlalchemy   https://support.teradata.com/knowledge?id=kb_article_view&sys_kb_id=a5a869149729251ced863fe3f153af27
    )
    sqla_keys = (
        "athena",  # https://github.com/laughingman7743/PyAthena/blob/master/pyproject.toml
        "bigquery",  # https://github.com/googleapis/python-bigquery-sqlalchemy/blob/main/setup.py
        "dremio",  # https://github.com/narendrans/sqlalchemy_dremio/blob/master/setup.py
        "hive",  # https://github.com/dropbox/PyHive/blob/master/setup.py
        "sql-server",  # https://github.com/mkleehammer/pyodbc/blob/master/setup.py
        "mysql",  # https://github.com/PyMySQL/PyMySQL/blob/main/pyproject.toml
        "oracle",  # https://github.com/oracle/python-oracledb/blob/main/pyproject.toml
        "postgresql",  # https://github.com/psycopg/psycopg2/blob/master/setup.py
        "trino",  # https://github.com/trinodb/trino-python-client/blob/master/setup.py
        "vertica",  # https://github.com/bluelabsio/sqlalchemy-vertica-python/blob/master/setup.py
        "databricks",  # https://github.com/databricks/databricks-sql-python/blob/main/pyproject.toml
        "snowflake",  # https://github.com/snowflakedb/snowflake-sqlalchemy/blob/main/setup.cfg
        "redshift",  # https://github.com/sqlalchemy-redshift/sqlalchemy-redshift/blob/main/setup.py
        "gx-redshift",  # deprecated alias for "redshift"
    )
    ignore_keys = (
        "sqlalchemy",
        "sqlalchemy2",
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
        assert match is not None, (
            f"The extras requirements dir ({requirements_dir}) contains "
            "files that do not adhere to the following format: requirements-dev-*.txt"
        )
        key = match.group(1)
        if key in ignore_keys:
            continue

        parsed = parse_requirements(file_path)
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

long_description = "Always know what to expect from your data. (See https://github.com/great-expectations/great_expectations for full description)."  # noqa: E501

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
    "packages": find_packages(exclude=["contrib*", "docs*", "tests*", "examples*", "scripts*"]),
    "package_data": {
        "great_expectations": [
            "**/py.typed",
            "**/*.pyi",
            # Machine-readable catalogs describing the package's built-in expectation and
            # datasource types. Tooling reads these from the installed package, so they
            # have to ship with it rather than exist only in the source tree.
            # Scoped to the two schema trees rather than a blanket JSON glob, so an
            # unrelated JSON file added elsewhere in the package does not silently
            # become part of the distribution.
            "expectations/core/schemas/*.json",
            "datasource/fluent/schemas/**/*.json",
            # Agent-facing guidance, read by a coding agent rather than imported by
            # Python. Matched by file rather than by a directory glob, because a
            # setuptools package_data pattern only ever selects files -- an empty
            # directory in the pattern's path is never itself a match, and wheels
            # cannot record an empty directory anyway. Scoped to the skills tree
            # rather than a blanket markdown glob, so an unrelated markdown file
            # added elsewhere in the package does not silently become part of the
            # distribution.
            ".agents/skills/**/*.md",
        ]
    },
    "name": "great_expectations",
    "long_description": long_description,
    "license": "Apache-2.0",
    "keywords": "data science testing pipeline data quality dataquality validation datavalidation",
    "include_package_data": True,
    "python_requires": get_python_requires(),
    "classifiers": [
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Intended Audience :: Other Audience",
        "Topic :: Scientific/Engineering",
        "Topic :: Software Development",
        "Topic :: Software Development :: Testing",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
    ],
}

if __name__ == "__main__":
    setup(**config)
