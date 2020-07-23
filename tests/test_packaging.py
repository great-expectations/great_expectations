import requirements as rp

from great_expectations.data_context.util import file_relative_path


def test_requirements_files():
    """requirements.txt should be a subset of requirements-dev.txt"""

    with open(file_relative_path(__file__, "../requirements.txt")) as req:
        requirements = set(
            [f'{line.name}{"".join(line.specs[0])}' for line in rp.parse(req)]
        )

    with open(file_relative_path(__file__, "../requirements-dev.txt")) as req:
        requirements_dev = set(
            [f'{line.name}{"".join(line.specs[0])}' for line in rp.parse(req)]
        )

    with open(
        file_relative_path(__file__, "../tests/requirements-dev-minimal.txt")
    ) as req:
        requirements_dev_min = set(
            [f'{line.name}{"".join(line.specs[0])}' for line in rp.parse(req)]
        )

    with open(
        file_relative_path(__file__, "../tests/requirements-dev-sqlalchemy.txt")
    ) as req:
        requirements_dev_sqlalchemy = set(
            [f'{line.name}{"".join(line.specs[0])}' for line in rp.parse(req)]
        )

    with open(
        file_relative_path(__file__, "../tests/requirements-dev-spark.txt")
    ) as req:
        requirements_dev_spark = set(
            [f'{line.name}{"".join(line.specs[0])}' for line in rp.parse(req)]
        )

    assert requirements <= requirements_dev

    assert requirements_dev_min.intersection(requirements_dev_sqlalchemy) == set()
    assert requirements_dev_min.intersection(requirements_dev_spark) == set()
    assert requirements_dev_spark.intersection(requirements_dev_sqlalchemy) == set()

    assert requirements_dev - (
        requirements
        | requirements_dev_min
        | requirements_dev_sqlalchemy
        | requirements_dev_spark
    ) == {"pre-commit>=2.3.0", "isort==4.3.21"}
