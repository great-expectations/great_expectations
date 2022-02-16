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

    with open(file_relative_path(__file__, "../requirements-dev-base.txt")) as req:
        requirements_dev_base = {
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

    assert requirements <= requirements_dev

    assert requirements_dev_base.intersection(requirements_dev_spark) == set()
    assert requirements_dev_base.intersection(requirements_dev_sqlalchemy) == set()

    assert requirements_dev_spark.intersection(requirements_dev_sqlalchemy) == set()

    assert requirements_dev - (
        requirements
        | requirements_dev_base
        | requirements_dev_sqlalchemy
        | requirements_dev_spark
    ) <= {"numpy>=1.21.0", "scipy>=1.7.0"}
