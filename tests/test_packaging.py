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

    with open(file_relative_path(__file__, "../requirements-dev-util.txt")) as req:
        requirements_dev_util = {
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

    with open(file_relative_path(__file__, "../requirements-dev-test.txt")) as req:
        requirements_dev_test = {
            f'{line.name}{"".join(line.specs[0])}' for line in rp.parse(req)
        }

    with open(file_relative_path(__file__, "../requirements-dev-build.txt")) as req:
        requirements_dev_build = {
            f'{line.name}{"".join(line.specs[0])}' for line in rp.parse(req)
        }

    with open(file_relative_path(__file__, "../requirements-dev-publish.txt")) as req:
        requirements_dev_publish = {
            f'{line.name}{"".join(line.specs[0])}' for line in rp.parse(req)
        }

    assert requirements <= requirements_dev

    assert requirements_dev_util.intersection(requirements_dev_spark) == set()
    assert requirements_dev_util.intersection(requirements_dev_sqlalchemy) == set()
    assert requirements_dev_util.intersection(requirements_dev_test) == set()
    assert requirements_dev_util.intersection(requirements_dev_build) == set()

    assert requirements_dev_spark.intersection(requirements_dev_sqlalchemy) == set()
    assert requirements_dev_spark.intersection(requirements_dev_test) == set()
    assert requirements_dev_spark.intersection(requirements_dev_build) == set()

    assert requirements_dev_sqlalchemy.intersection(requirements_dev_test) == set()
    assert requirements_dev_sqlalchemy.intersection(requirements_dev_build) == set()

    assert requirements_dev_test.intersection(requirements_dev_build) == set()

    assert requirements_dev_publish.intersection(requirements_dev_test) == set()
    assert requirements_dev_publish.intersection(requirements_dev_build) == set()

    assert (
        requirements_dev
        - (
            requirements
            | requirements_dev_util
            | requirements_dev_sqlalchemy
            | requirements_dev_spark
            | requirements_dev_test
            | requirements_dev_build
            | requirements_dev_publish
        )
        == set()
    )
