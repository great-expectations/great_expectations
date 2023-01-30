import os.path
import pathlib
from typing import List

import requirements as rp


def collect_requirements_files() -> List[pathlib.Path]:
    project_root = pathlib.Path(__file__).parents[1]
    assert project_root.exists()
    reqs_dir = project_root.joinpath("reqs")
    assert reqs_dir.exists()

    pattern = "requirements*.txt"
    return list(project_root.glob(pattern)) + list(reqs_dir.glob(pattern))


def test_requirements_files():
    """requirements.txt should be a subset of requirements-dev.txt"""

    req_set_dict = {}
    req_files = collect_requirements_files()
    for req_file in req_files:
        abs_path = req_file.absolute().as_posix()
        key = abs_path.rsplit(os.path.sep, 1)[-1]
        with open(req_file) as f:
            req_set_dict[key] = {
                f'{line.name}{"".join(line.specs[0])}'
                for line in rp.parse(f)
                if line.specs
            }

    assert req_set_dict["requirements.txt"] <= req_set_dict["requirements-dev.txt"]

    assert (
        req_set_dict["requirements-dev-contrib.txt"]
        | req_set_dict["requirements-dev-lite.txt"]
        | req_set_dict["requirements-dev-api-docs-test.txt"]
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
        | req_set_dict["requirements-dev-trino.txt"]
        | req_set_dict["requirements-dev-vertica.txt"]
    ) <= {"numpy>=1.21.0", "scipy>=1.7.0"}
