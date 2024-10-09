from __future__ import annotations

import os.path
import pathlib
from pprint import pformat as pf
from typing import Final, List

import pytest
import requirements as rp

IGNORE_PINS: Final[set[str]] = {"mypy", "ruff", "pytest"}


def collect_requirements_files() -> List[pathlib.Path]:
    project_root = pathlib.Path(__file__).parents[1]
    assert project_root.exists()
    reqs_dir = project_root.joinpath("reqs")
    assert reqs_dir.exists()

    pattern = "requirements*.txt"
    requirement_files = list(project_root.glob(pattern)) + list(reqs_dir.glob(pattern))
    return requirement_files


def parse_requirements_files_to_strings(
    files: list[pathlib.Path],
) -> dict[str, set[str]]:
    """Parse requirements files to dict.

    dict of the form {"filename": {"package_name_with_specs_as_str"}}

    Args:
        files: Paths to files that should be parsed
    """

    req_set_dict: dict[str, set[str]] = {}
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


def parse_requirements_files_to_specs(
    files: list[pathlib.Path],
) -> dict[str, dict[str | None, list[str]]]:
    """Parse requirements files to dict.

    dict of the form {"filename": {"package_name_with_specs_as_str"}}

    Args:
        files: Paths to files that should be parsed
    """

    req_set_dict: dict[str, dict[str | None, list[str]]] = {}
    for req_file in files:
        abs_path = req_file.absolute().as_posix()
        key = abs_path.rsplit(os.path.sep, 1)[-1]
        with open(req_file) as f:
            req_set_dict[key] = {line.name: line.specs for line in rp.parse(f) if line.specs}  # type: ignore[misc]

    return req_set_dict


@pytest.mark.unit
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
        == req_set_dict["requirements-dev-test.txt"]
    )

    assert (
        req_set_dict["requirements-dev-lite.txt"] & req_set_dict["requirements-dev-spark.txt"]
        == set()
    )

    assert (
        req_set_dict["requirements-dev-spark.txt"]
        & req_set_dict["requirements-dev-sqlalchemy.txt"]
        & req_set_dict["requirements-dev-azure.txt"]
        == set()
    )

    assert (
        req_set_dict["requirements-dev-lite.txt"] & req_set_dict["requirements-dev-contrib.txt"]
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
        | req_set_dict["requirements-dev-sqlalchemy1.txt"]
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
        | req_set_dict["requirements-dev-sqlalchemy1.txt"]
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
    # don't care about type-checking requirements
    req_set_dict.pop("requirements-types.txt")
    print(f"Requirement Groups:\n{pf(req_set_dict, depth=1)}")

    packages_with_pins_or_upper_bounds = set()

    for req_file, package_specs in req_set_dict.items():
        for package, specs in package_specs.items():
            if package in IGNORE_PINS:
                continue
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
    assert len(sorted_packages_with_pins_or_upper_bounds) == 36
    assert set(sorted_packages_with_pins_or_upper_bounds) == {
        (
            "requirements-dev-api-docs-test.txt",
            "docstring-parser",
            (("==", "0.15"),),
        ),
        ("requirements-dev-athena.txt", "pyathena", (("<", "3"), (">=", "2.0.0"))),
        ("requirements-dev-contrib.txt", "adr-tools-python", (("==", "1.0.3"),)),
        ("requirements-dev-dremio.txt", "sqlalchemy-dremio", (("==", "1.2.1"),)),
        ("requirements-dev-excel.txt", "xlrd", (("<", "2.0.0"), (">=", "1.1.0"))),
        ("requirements-dev-lite.txt", "moto", (("<", "5.0"), (">=", "4.2.13"))),
        ("requirements-dev-pagerduty.txt", "pypd", (("==", "1.1.0"),)),
        ("requirements-dev-snowflake.txt", "pandas", (("<", "2.2.0"),)),
        ("requirements-dev-sqlalchemy.txt", "moto", (("<", "5.0"), (">=", "4.2.13"))),
        ("requirements-dev-sqlalchemy.txt", "pandas", (("<", "2.2.0"),)),
        (
            "requirements-dev-sqlalchemy.txt",
            "pyathena",
            (("<", "3"), (">=", "2.0.0")),
        ),
        ("requirements-dev-sqlalchemy.txt", "sqlalchemy", (("<", "2.0.0"),)),
        (
            "requirements-dev-sqlalchemy.txt",
            "sqlalchemy-dremio",
            (("==", "1.2.1"),),
        ),
        (
            "requirements-dev-sqlalchemy.txt",
            "teradatasqlalchemy",
            (("==", "17.0.0.5"),),
        ),
        ("requirements-dev-sqlalchemy1.txt", "sqlalchemy", (("<", "2.0.0"),)),
        (
            "requirements-dev-teradata.txt",
            "teradatasqlalchemy",
            (("==", "17.0.0.5"),),
        ),
        ("requirements-dev-test.txt", "adr-tools-python", (("==", "1.0.3"),)),
        ("requirements-dev-test.txt", "docstring-parser", (("==", "0.15"),)),
        ("requirements-dev-test.txt", "moto", (("<", "5.0"), (">=", "4.2.13"))),
        ("requirements-dev.txt", "adr-tools-python", (("==", "1.0.3"),)),
        ("requirements-dev.txt", "altair", (("<", "5.0.0"), (">=", "4.2.1"))),
        ("requirements-dev.txt", "docstring-parser", (("==", "0.15"),)),
        ("requirements-dev.txt", "marshmallow", (("<", "4.0.0"), (">=", "3.7.1"))),
        ("requirements-dev.txt", "moto", (("<", "5.0"), (">=", "4.2.13"))),
        ("requirements-dev.txt", "pandas", (("<", "2.2.0"),)),
        ("requirements-dev.txt", "posthog", (("<", "3"), (">=", "2.1.0"))),
        ("requirements-dev.txt", "pyathena", (("<", "3"), (">=", "2.0.0"))),
        ("requirements-dev.txt", "pypd", (("==", "1.1.0"),)),
        ("requirements-dev.txt", "sqlalchemy", (("<", "2.0.0"),)),
        ("requirements-dev.txt", "sqlalchemy-dremio", (("==", "1.2.1"),)),
        ("requirements-dev.txt", "teradatasqlalchemy", (("==", "17.0.0.5"),)),
        ("requirements-dev.txt", "xlrd", (("<", "2.0.0"), (">=", "1.1.0"))),
        ("requirements.txt", "altair", (("<", "5.0.0"), (">=", "4.2.1"))),
        ("requirements.txt", "marshmallow", (("<", "4.0.0"), (">=", "3.7.1"))),
        ("requirements.txt", "pandas", (("<", "2.2"),)),
        ("requirements.txt", "posthog", (("<", "3"), (">=", "2.1.0"))),
    }
