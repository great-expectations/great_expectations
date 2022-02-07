from typing import List
from unittest import mock

import pytest
from great_expectations_contrib.package import GreatExpectationsContribPackageManifest

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
def diagnostics():
    expectations = [
        ExpectColumnMinToBeBetween,
        ExpectColumnMostCommonValueToBeInSet,
        ExpectColumnStdevToBeBetween,
    ]
    diagnostics = list(map(lambda e: e().run_diagnostics(), expectations))
    return diagnostics


def test_update_package_state(diagnostics: List[ExpectationDiagnostics]):
    breakpoint()
    package = GreatExpectationsContribPackageManifest()
    with mock.patch(
        "great_expectations_contrib.package.GreatExpectationsContribPackageManifest._retrieve_package_expectations_diagnostics",
        return_value=diagnostics,
    ):
        package.update_package_state()

    assert package.expectation_count == 3
