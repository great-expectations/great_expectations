from __future__ import annotations

import os.path
import pathlib
from typing import List

import pytest
import requirements as rp


def collect_requirements_files() -> List[pathlib.Path]:
    project_root = pathlib.Path(__file__).parents[1]
    assert project_root.exists()
    reqs_dir = project_root.joinpath("reqs")
    assert reqs_dir.exists()

    pattern = "requirements*.txt"
    return list(project_root.glob(pattern)) + list(reqs_dir.glob(pattern))


def parse_requirements_files_to_strings(files: list[pathlib.Path]) -> dict:
    """Parse requirements files to dict.

    dict of the form {"filename": {"package_name_with_specs_as_str"}}

    Args:
        files: Paths to files that should be parsed
    """

    req_set_dict = {}
    for req_file in files:
        abs_path = req_file.absolute().as_posix()
        key = abs_path.rsplit(os.path.sep, 1)[-1]
        with open(req_file) as f:
            req_set_dict[key] = {
                f'{line.name}{",".join(["".join(spec) for spec in line.specs])}'
                for line in rp.parse(f)
                if line.specs
            }

    return req_set_dict


def parse_requirements_files_to_specs(files: list[pathlib.Path]) -> dict:
    """Parse requirements files to dict.

    dict of the form {"filename": {"package_name_with_specs_as_str"}}

    Args:
        files: Paths to files that should be parsed
    """

    req_set_dict = {}
    for req_file in files:
        abs_path = req_file.absolute().as_posix()
        key = abs_path.rsplit(os.path.sep, 1)[-1]
        with open(req_file) as f:
            req_set_dict[key] = {
                line.name: line.specs for line in rp.parse(f) if line.specs
            }

    return req_set_dict


@pytest.mark.integration
def test_requirements_files():
    """requirements.txt should be a subset of requirements-dev.txt"""

    req_files = collect_requirements_files()
    req_set_dict = parse_requirements_files_to_strings(files=req_files)

    assert req_set_dict["requirements.txt"] <= req_set_dict["requirements-dev.txt"]

    assert (
        req_set_dict["requirements-dev-arrow.txt"]
        | req_set_dict["requirements-dev-contrib.txt"]
        | req_set_dict["requirements-dev-lite.txt"]
        | req_set_dict["requirements-dev-api-docs-test.txt"]
        | req_set_dict["requirements-dev-cloud.txt"]
        == req_set_dict["requirements-dev-test.txt"]
    )

    assert (
        req_set_dict["requirements-dev-lite.txt"]
        & req_set_dict["requirements-dev-spark.txt"]
        == set()
    )

    assert (
        req_set_dict["requirements-dev-spark.txt"]
        & req_set_dict["requirements-dev-sqlalchemy.txt"]
        & req_set_dict["requirements-dev-azure.txt"]
        == set()
    )

    assert (
        req_set_dict["requirements-dev-lite.txt"]
        & req_set_dict["requirements-dev-contrib.txt"]
        == set()
    )

    assert (
        req_set_dict["requirements-dev-lite.txt"]
        | req_set_dict["requirements-dev-athena.txt"]
        | req_set_dict["requirements-dev-bigquery.txt"]
        | req_set_dict["requirements-dev-dremio.txt"]
        | req_set_dict["requirements-dev-mssql.txt"]
        | req_set_dict["requirements-dev-mysql.txt"]
        | req_set_dict["requirements-dev-postgresql.txt"]
        | req_set_dict["requirements-dev-redshift.txt"]
        | req_set_dict["requirements-dev-snowflake.txt"]
        | req_set_dict["requirements-dev-teradata.txt"]
        | req_set_dict["requirements-dev-clickhouse.txt"]
        | req_set_dict["requirements-dev-trino.txt"]
        | req_set_dict["requirements-dev-hive.txt"]
        | req_set_dict["requirements-dev-vertica.txt"]
    ) == req_set_dict["requirements-dev-sqlalchemy.txt"]

    assert (
        req_set_dict["requirements.txt"]
        | req_set_dict["requirements-dev-contrib.txt"]
        | req_set_dict["requirements-dev-test.txt"]
        | req_set_dict["requirements-dev-sqlalchemy.txt"]
        | req_set_dict["requirements-dev-arrow.txt"]
        | req_set_dict["requirements-dev-azure.txt"]
        | req_set_dict["requirements-dev-excel.txt"]
        | req_set_dict["requirements-dev-pagerduty.txt"]
        | req_set_dict["requirements-dev-spark.txt"]
    ) == req_set_dict["requirements-dev.txt"]

    assert req_set_dict["requirements-dev.txt"] - (
        req_set_dict["requirements.txt"]
        | req_set_dict["requirements-dev-lite.txt"]
        | req_set_dict["requirements-dev-contrib.txt"]
        | req_set_dict["requirements-dev-test.txt"]
        | req_set_dict["requirements-dev-spark.txt"]
        | req_set_dict["requirements-dev-sqlalchemy.txt"]
        | req_set_dict["requirements-dev-arrow.txt"]
        | req_set_dict["requirements-dev-athena.txt"]
        | req_set_dict["requirements-dev-azure.txt"]
        | req_set_dict["requirements-dev-bigquery.txt"]
        | req_set_dict["requirements-dev-dremio.txt"]
        | req_set_dict["requirements-dev-excel.txt"]
        | req_set_dict["requirements-dev-mssql.txt"]
        | req_set_dict["requirements-dev-mysql.txt"]
        | req_set_dict["requirements-dev-pagerduty.txt"]
        | req_set_dict["requirements-dev-postgresql.txt"]
        | req_set_dict["requirements-dev-redshift.txt"]
        | req_set_dict["requirements-dev-snowflake.txt"]
        | req_set_dict["requirements-dev-teradata.txt"]
        | req_set_dict["requirements-dev-clickhouse.txt"]
        | req_set_dict["requirements-dev-trino.txt"]
        | req_set_dict["requirements-dev-vertica.txt"]
    ) <= {"numpy>=1.21.0", "scipy>=1.7.0"}


@pytest.mark.unit
def test_polish_and_ratchet_pins_and_upper_bounds():
    """What does this test do and why?

    We would like to reduce the number of pins and upper bounds on dependencies
    so that we can stay up to date with our dependencies where possible.
    """
    req_files = collect_requirements_files()
    req_set_dict = parse_requirements_files_to_specs(files=req_files)

    packages_with_pins_or_upper_bounds = set()

    for req_file, package_specs in req_set_dict.items():
        for package, specs in package_specs.items():
            for spec in specs:
                if spec[0] in ("<", "<=", "=="):
                    packages_with_pins_or_upper_bounds.add(
                        (
                            req_file,
                            package,
                            tuple(sorted(specs, key=lambda s: (s[0], s[1]))),
                        )
                    )

    sorted_packages_with_pins_or_upper_bounds = sorted(
        list(packages_with_pins_or_upper_bounds), key=lambda p: (p[0], p[1])
    )

    # Polish and ratchet this number down as low as possible
    assert len(sorted_packages_with_pins_or_upper_bounds) == 80
    assert sorted_packages_with_pins_or_upper_bounds == [
        ("requirements-dev-api-docs-test.txt", "docstring-parser", (("==", "0.15"),)),
        ("requirements-dev-athena.txt", "pyathena", (("<", "3"), (">=", "2.0.0"))),
        ("requirements-dev-cloud.txt", "pika", (("==", "1.3.1"),)),
        ("requirements-dev-contrib.txt", "adr-tools-python", (("==", "1.0.3"),)),
        ("requirements-dev-contrib.txt", "black", (("==", "23.3.0"),)),
        ("requirements-dev-contrib.txt", "mypy", (("==", "1.3.0"),)),
        ("requirements-dev-contrib.txt", "ruff", (("==", "0.0.271"),)),
        ("requirements-dev-dremio.txt", "sqlalchemy-dremio", (("==", "1.2.1"),)),
        ("requirements-dev-excel.txt", "xlrd", (("<", "2.0.0"), (">=", "1.1.0"))),
        ("requirements-dev-lite.txt", "ipykernel", (("<=", "6.17.1"),)),
        ("requirements-dev-lite.txt", "moto", (("<", "3.0.0"), (">=", "2.0.0"))),
        ("requirements-dev-lite.txt", "snapshottest", (("==", "0.6.0"),)),
        ("requirements-dev-mysql.txt", "PyMySQL", (("<", "0.10"), (">=", "0.9.3"))),
        ("requirements-dev-pagerduty.txt", "pypd", (("==", "1.1.0"),)),
        (
            "requirements-dev-sqlalchemy-less-than-2.txt",
            "sqlalchemy",
            (("<", "2.0.0"),),
        ),
        (
            "requirements-dev-sqlalchemy.txt",
            "PyMySQL",
            (("<", "0.10"), (">=", "0.9.3")),
        ),
        ("requirements-dev-sqlalchemy.txt", "ipykernel", (("<=", "6.17.1"),)),
        ("requirements-dev-sqlalchemy.txt", "moto", (("<", "3.0.0"), (">=", "2.0.0"))),
        ("requirements-dev-sqlalchemy.txt", "pyathena", (("<", "3"), (">=", "2.0.0"))),
        ("requirements-dev-sqlalchemy.txt", "snapshottest", (("==", "0.6.0"),)),
        ("requirements-dev-sqlalchemy.txt", "sqlalchemy-dremio", (("==", "1.2.1"),)),
        (
            "requirements-dev-sqlalchemy.txt",
            "teradatasqlalchemy",
            (("==", "17.0.0.5"),),
        ),
        ("requirements-dev-teradata.txt", "teradatasqlalchemy", (("==", "17.0.0.5"),)),
        ("requirements-dev-test.txt", "adr-tools-python", (("==", "1.0.3"),)),
        ("requirements-dev-test.txt", "black", (("==", "23.3.0"),)),
        ("requirements-dev-test.txt", "docstring-parser", (("==", "0.15"),)),
        ("requirements-dev-test.txt", "ipykernel", (("<=", "6.17.1"),)),
        ("requirements-dev-test.txt", "moto", (("<", "3.0.0"), (">=", "2.0.0"))),
        ("requirements-dev-test.txt", "mypy", (("==", "1.3.0"),)),
        ("requirements-dev-test.txt", "pika", (("==", "1.3.1"),)),
        ("requirements-dev-test.txt", "ruff", (("==", "0.0.271"),)),
        ("requirements-dev-test.txt", "snapshottest", (("==", "0.6.0"),)),
        ("requirements-dev.txt", "Click", (("<=", "8.1.3"), (">=", "7.1.2"))),
        ("requirements-dev.txt", "PyMySQL", (("<", "0.10"), (">=", "0.9.3"))),
        ("requirements-dev.txt", "adr-tools-python", (("==", "1.0.3"),)),
        ("requirements-dev.txt", "altair", (("<", "5.0.0"), (">=", "4.2.1"))),
        ("requirements-dev.txt", "black", (("==", "23.3.0"),)),
        ("requirements-dev.txt", "docstring-parser", (("==", "0.15"),)),
        ("requirements-dev.txt", "ipykernel", (("<=", "6.17.1"),)),
        ("requirements-dev.txt", "jsonschema", (("<", "4.18.1"), (">=", "2.5.1"))),
        ("requirements-dev.txt", "makefun", (("<", "2"), (">=", "1.7.0"))),
        ("requirements-dev.txt", "marshmallow", (("<", "4.0.0"), (">=", "3.7.1"))),
        ("requirements-dev.txt", "moto", (("<", "3.0.0"), (">=", "2.0.0"))),
        ("requirements-dev.txt", "mypy", (("==", "1.3.0"),)),
        ("requirements-dev.txt", "pika", (("==", "1.3.1"),)),
        ("requirements-dev.txt", "pyathena", (("<", "3"), (">=", "2.0.0"))),
        ("requirements-dev.txt", "pydantic", (("<", "2.0"), (">=", "1.9.2"))),
        ("requirements-dev.txt", "pypd", (("==", "1.1.0"),)),
        ("requirements-dev.txt", "ruamel.yaml", (("<", "0.17.18"), (">=", "0.16"))),
        ("requirements-dev.txt", "ruff", (("==", "0.0.271"),)),
        ("requirements-dev.txt", "snapshottest", (("==", "0.6.0"),)),
        ("requirements-dev.txt", "sqlalchemy-dremio", (("==", "1.2.1"),)),
        ("requirements-dev.txt", "teradatasqlalchemy", (("==", "17.0.0.5"),)),
        ("requirements-dev.txt", "xlrd", (("<", "2.0.0"), (">=", "1.1.0"))),
        ("requirements-types.txt", "Click", (("<=", "8.1.3"), (">=", "7.1.2"))),
        ("requirements-types.txt", "PyMySQL", (("<", "0.10"), (">=", "0.9.3"))),
        ("requirements-types.txt", "adr-tools-python", (("==", "1.0.3"),)),
        ("requirements-types.txt", "altair", (("<", "5.0.0"), (">=", "4.2.1"))),
        ("requirements-types.txt", "black", (("==", "23.3.0"),)),
        ("requirements-types.txt", "ipykernel", (("<=", "6.17.1"),)),
        ("requirements-types.txt", "jsonschema", (("<", "4.18.1"), (">=", "2.5.1"))),
        ("requirements-types.txt", "makefun", (("<", "2"), (">=", "1.7.0"))),
        ("requirements-types.txt", "marshmallow", (("<", "4.0.0"), (">=", "3.7.1"))),
        ("requirements-types.txt", "moto", (("<", "3.0.0"), (">=", "2.0.0"))),
        ("requirements-types.txt", "mypy", (("==", "1.3.0"),)),
        ("requirements-types.txt", "pika", (("==", "1.3.1"),)),
        ("requirements-types.txt", "pyathena", (("<", "3"), (">=", "2.0.0"))),
        ("requirements-types.txt", "pydantic", (("<", "2.0"), (">=", "1.9.2"))),
        ("requirements-types.txt", "ruamel.yaml", (("<", "0.17.18"), (">=", "0.16"))),
        ("requirements-types.txt", "ruff", (("==", "0.0.271"),)),
        ("requirements-types.txt", "snapshottest", (("==", "0.6.0"),)),
        ("requirements-types.txt", "sqlalchemy-dremio", (("==", "1.2.1"),)),
        ("requirements-types.txt", "teradatasqlalchemy", (("==", "17.0.0.5"),)),
        ("requirements.txt", "Click", (("<=", "8.1.3"), (">=", "7.1.2"))),
        ("requirements.txt", "altair", (("<", "5.0.0"), (">=", "4.2.1"))),
        ("requirements.txt", "jsonschema", (("<", "4.18.1"), (">=", "2.5.1"))),
        ("requirements.txt", "makefun", (("<", "2"), (">=", "1.7.0"))),
        ("requirements.txt", "marshmallow", (("<", "4.0.0"), (">=", "3.7.1"))),
        ("requirements.txt", "pydantic", (("<", "2.0"), (">=", "1.9.2"))),
        ("requirements.txt", "ruamel.yaml", (("<", "0.17.18"), (">=", "0.16"))),
    ]
