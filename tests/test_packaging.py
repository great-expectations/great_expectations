import os

from great_expectations.data_context.util import file_relative_path


def test_requirements_files():
    """requirements.txt should be a subset of requirements-dev.txt"""

    with open(file_relative_path(__file__, "../requirements.txt")) as req:
        requirements = set(
            [
                line.rstrip().split(" ")[0]
                for line in req.readlines()
                if line != os.linesep and not line.startswith("#")
            ]
        )

    with open(file_relative_path(__file__, "../requirements-dev.txt")) as req:
        requirements_dev = set(
            [
                line.rstrip().split(" ")[0]
                for line in req.readlines()
                if line != os.linesep and not line.startswith("#")
            ]
        )

    with open(
        file_relative_path(__file__, "../tests/requirements-dev-minimal.txt")
    ) as req:
        requirements_dev_min = set(
            [
                line.rstrip().split(" ")[0]
                for line in req.readlines()
                if line != os.linesep and not line.startswith("#")
            ]
        )

    with open(
        file_relative_path(__file__, "../tests/requirements-dev-sqlalchemy.txt")
    ) as req:
        requirements_dev_sqlalchemy = set(
            [
                line.rstrip().split(" ")[0]
                for line in req.readlines()
                if line != os.linesep and not line.startswith("#")
            ]
        )

    with open(
        file_relative_path(__file__, "../tests/requirements-dev-spark.txt")
    ) as req:
        requirements_dev_spark = set(
            [
                line.rstrip().split(" ")[0]
                for line in req.readlines()
                if line != os.linesep and not line.startswith("#")
            ]
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
    ) == {"pre-commit>=2.3.0", "isort[requirements]==4.3.21"}
