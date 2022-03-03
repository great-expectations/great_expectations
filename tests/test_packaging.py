import requirements as rp

from great_expectations.data_context.util import file_relative_path


def test_requirements_files():
    """requirements.txt should be a subset of requirements-dev.txt"""

    with open(file_relative_path(__file__, "../requirements.txt")) as req:
        requirements = {
            f'{line.name}{"".join(line.specs[0])}' for line in rp.parse(req)
        }

    with open(file_relative_path(__file__, "../requirements-dev.txt")) as req:
        requirements_dev = {
            f'{line.name}{"".join(line.specs[0])}' for line in rp.parse(req)
        }

    with open(file_relative_path(__file__, "../requirements-dev-lite.txt")) as req:
        requirements_dev_lite = {
            f'{line.name}{"".join(line.specs[0])}' for line in rp.parse(req)
        }

    with open(file_relative_path(__file__, "../requirements-dev-contrib.txt")) as req:
        requirements_dev_contrib = {
            f'{line.name}{"".join(line.specs[0])}' for line in rp.parse(req)
        }

    with open(file_relative_path(__file__, "../requirements-dev-test.txt")) as req:
        requirements_dev_test = {
            f'{line.name}{"".join(line.specs[0])}' for line in rp.parse(req)
        }

    with open(file_relative_path(__file__, "../requirements-dev-spark.txt")) as req:
        requirements_dev_spark = {
            f'{line.name}{"".join(line.specs[0])}' for line in rp.parse(req)
        }

    with open(
        file_relative_path(__file__, "../requirements-dev-sqlalchemy.txt")
    ) as req:
        requirements_dev_sqlalchemy = {
            f'{line.name}{"".join(line.specs[0])}' for line in rp.parse(req)
        }

    with open(file_relative_path(__file__, "../requirements-dev-airflow.txt")) as req:
        requirements_dev_airflow = {
            f'{line.name}{"".join(line.specs[0])}' for line in rp.parse(req)
        }

    with open(file_relative_path(__file__, "../requirements-dev-arrow.txt")) as req:
        requirements_dev_arrow = {
            f'{line.name}{"".join(line.specs[0])}' for line in rp.parse(req)
        }

    with open(file_relative_path(__file__, "../requirements-dev-athena.txt")) as req:
        requirements_dev_athena = {
            f'{line.name}{"".join(line.specs[0])}' for line in rp.parse(req)
        }

    with open(file_relative_path(__file__, "../requirements-dev-azure.txt")) as req:
        requirements_dev_azure = {
            f'{line.name}{"".join(line.specs[0])}' for line in rp.parse(req)
        }

    with open(file_relative_path(__file__, "../requirements-dev-bigquery.txt")) as req:
        requirements_dev_bigquery = {
            f'{line.name}{"".join(line.specs[0])}' for line in rp.parse(req)
        }

    with open(file_relative_path(__file__, "../requirements-dev-dremio.txt")) as req:
        requirements_dev_dremio = {
            f'{line.name}{"".join(line.specs[0])}' for line in rp.parse(req)
        }

    with open(file_relative_path(__file__, "../requirements-dev-excel.txt")) as req:
        requirements_dev_excel = {
            f'{line.name}{"".join(line.specs[0])}' for line in rp.parse(req)
        }

    with open(file_relative_path(__file__, "../requirements-dev-mssql.txt")) as req:
        requirements_dev_mssql = {
            f'{line.name}{"".join(line.specs[0])}' for line in rp.parse(req)
        }

    with open(file_relative_path(__file__, "../requirements-dev-mysql.txt")) as req:
        requirements_dev_mysql = {
            f'{line.name}{"".join(line.specs[0])}' for line in rp.parse(req)
        }

    with open(file_relative_path(__file__, "../requirements-dev-pagerduty.txt")) as req:
        requirements_dev_pagerduty = {
            f'{line.name}{"".join(line.specs[0])}' for line in rp.parse(req)
        }

    with open(
        file_relative_path(__file__, "../requirements-dev-postgresql.txt")
    ) as req:
        requirements_dev_postgresql = {
            f'{line.name}{"".join(line.specs[0])}' for line in rp.parse(req)
        }

    with open(file_relative_path(__file__, "../requirements-dev-redshift.txt")) as req:
        requirements_dev_redshift = {
            f'{line.name}{"".join(line.specs[0])}' for line in rp.parse(req)
        }

    with open(file_relative_path(__file__, "../requirements-dev-snowflake.txt")) as req:
        requirements_dev_snowflake = {
            f'{line.name}{"".join(line.specs[0])}' for line in rp.parse(req)
        }

    with open(file_relative_path(__file__, "../requirements-dev-teradata.txt")) as req:
        requirements_dev_teradata = {
            f'{line.name}{"".join(line.specs[0])}' for line in rp.parse(req)
        }

    assert requirements <= requirements_dev

    assert requirements_dev_contrib | requirements_dev_lite == requirements_dev_test

    assert requirements_dev_lite & requirements_dev_spark == set()

    assert requirements_dev_spark & requirements_dev_sqlalchemy == set()

    assert requirements_dev_lite & requirements_dev_contrib == set()

    assert (
        requirements_dev_lite
        | requirements_dev_athena
        | requirements_dev_azure
        | requirements_dev_bigquery
        | requirements_dev_dremio
        | requirements_dev_mssql
        | requirements_dev_mysql
        | requirements_dev_postgresql
        | requirements_dev_redshift
        | requirements_dev_snowflake
        | requirements_dev_teradata
    ) == requirements_dev_sqlalchemy

    assert (
        requirements
        | requirements_dev_contrib
        | requirements_dev_sqlalchemy
        | requirements_dev_airflow
        | requirements_dev_arrow
        | requirements_dev_excel
        | requirements_dev_pagerduty
        | requirements_dev_spark
    ) == requirements_dev

    assert requirements_dev - (
        requirements
        | requirements_dev_lite
        | requirements_dev_contrib
        | requirements_dev_spark
        | requirements_dev_sqlalchemy
        | requirements_dev_airflow
        | requirements_dev_arrow
        | requirements_dev_athena
        | requirements_dev_azure
        | requirements_dev_bigquery
        | requirements_dev_dremio
        | requirements_dev_excel
        | requirements_dev_mssql
        | requirements_dev_mysql
        | requirements_dev_pagerduty
        | requirements_dev_postgresql
        | requirements_dev_redshift
        | requirements_dev_snowflake
        | requirements_dev_teradata
    ) <= {"numpy>=1.21.0", "scipy>=1.7.0"}
