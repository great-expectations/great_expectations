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
    sqla_keys = (
        "athena",
        "bigquery",
        "clickhouse",
        "dremio",
        "mssql",
        "mysql",
        "postgresql",
        "redshift",
        "snowflake",
        "teradata",
        "trino",
        "vertica",
    )
    ignore_keys = (
        "sqlalchemy",
        "test",
        "tools",
        "sqlalchemy-alchemy-less-than-2" "all-contrib-expectations",
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
    arrow = results.pop("arrow")
    results["boto"] = [req for req in lite if req.startswith("boto")]
    results["sqlalchemy"] = [req for req in lite if req.startswith("sqlalchemy")]
    results["test"] = lite + contrib + docs_test + cloud + arrow

    for new_key, existing_key in extra_key_mapping.items():
        results[new_key] = results[existing_key]
    for key in sqla_keys:
        results[key] += results["sqlalchemy"]

    results.pop("boto")
    all_requirements_set = set()
    [all_requirements_set.update(vals) for vals in results.values()]
    results["dev"] = sorted(all_requirements_set)
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
