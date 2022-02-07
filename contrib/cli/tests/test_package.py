from typing import List
from unittest import mock

import py
import pytest
from great_expectations_contrib.package import (
    Dependency,
    GreatExpectationsContribPackageManifest,
    Maturity,
    RenderedExpectation,
)

from great_expectations.core.expectation_diagnostics.expectation_diagnostics import (
    ExpectationDiagnostics,
)
from great_expectations.expectations.core.expect_column_min_to_be_between import (
    ExpectColumnMinToBeBetween,
)
from great_expectations.expectations.core.expect_column_most_common_value_to_be_in_set import (
    ExpectColumnMostCommonValueToBeInSet,
)
from great_expectations.expectations.core.expect_column_stdev_to_be_between import (
    ExpectColumnStdevToBeBetween,
)


@pytest.fixture
def package() -> GreatExpectationsContribPackageManifest:
    package = GreatExpectationsContribPackageManifest()
    return package


@pytest.fixture
def diagnostics() -> List[ExpectationDiagnostics]:
    expectations = [
        ExpectColumnMinToBeBetween,
        ExpectColumnMostCommonValueToBeInSet,
        ExpectColumnStdevToBeBetween,
    ]
    diagnostics = list(map(lambda e: e().run_diagnostics(), expectations))
    return diagnostics


def test_update_expectations(
    package: GreatExpectationsContribPackageManifest,
    diagnostics: List[ExpectationDiagnostics],
):
    package._update_expectations(diagnostics)

    assert package.expectation_count == 3
    assert package.expectations and all(
        isinstance(expectation, RenderedExpectation)
        for expectation in package.expectations
    )
    assert (
        package.status and package.status.production == 3 and package.status.total == 3
    )
    assert package.maturity == Maturity.PRODUCTION


def test_update_dependencies(
    tmpdir: py.path.local, package: GreatExpectationsContribPackageManifest
):
    requirements_file = tmpdir.mkdir("tmp").join("requirements.txt")
    contents = """
altair>=4.0.0,<5  # package
Click>=7.1.2  # package
mistune>=0.8.4,<2.0.0  # package
numpy>=1.14.1  # package
ruamel.yaml>=0.16,<0.17.18  # package
    """
    requirements_file.write(contents)

    package._update_dependencies(str(requirements_file))
    assert package.dependencies == [
        Dependency(
            text="altair", link="https://pypi.org/project/altair", version="<5, >=4.0.0"
        ),
        Dependency(
            text="Click", link="https://pypi.org/project/Click", version=">=7.1.2"
        ),
        Dependency(
            text="mistune",
            link="https://pypi.org/project/mistune",
            version=">=0.8.4, <2.0.0",
        ),
        Dependency(
            text="numpy", link="https://pypi.org/project/numpy", version=">=1.14.1"
        ),
        Dependency(
            text="ruamel.yaml",
            link="https://pypi.org/project/ruamel.yaml",
            version="<0.17.18, >=0.16",
        ),
    ]
