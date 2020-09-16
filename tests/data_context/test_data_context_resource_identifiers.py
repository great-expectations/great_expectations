import pytest
from freezegun import freeze_time

from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.exceptions import InvalidDataContextKeyError


def test_expectation_suite_identifier_to_tuple():
    identifier = ExpectationSuiteIdentifier("test.identifier.name")
    assert identifier.to_tuple() == ("test", "identifier", "name")
    assert identifier.to_fixed_length_tuple() == ("test.identifier.name",)

    with pytest.raises(InvalidDataContextKeyError) as exc:
        _ = ExpectationSuiteIdentifier(None)
    assert "must be a string, not NoneType" in str(exc.value)

    with pytest.raises(InvalidDataContextKeyError) as exc:
        _ = ExpectationSuiteIdentifier(1)
    assert "must be a string, not int" in str(exc.value)


@freeze_time("09/26/2019 13:42:41")
def test_ValidationResultIdentifier_to_tuple(expectation_suite_identifier):
    validation_result_identifier = ValidationResultIdentifier(
        expectation_suite_identifier, "my_run_id", "my_batch_identifier"
    )
    assert validation_result_identifier.to_tuple() == (
        "my",
        "expectation",
        "suite",
        "name",
        "my_run_id",
        "20190926T134241.000000Z",
        "my_batch_identifier",
    )
    assert validation_result_identifier.to_fixed_length_tuple() == (
        "my.expectation.suite.name",
        "my_run_id",
        "20190926T134241.000000Z",
        "my_batch_identifier",
    )

    validation_result_identifier_no_run_id = ValidationResultIdentifier(
        expectation_suite_identifier, None, "my_batch_identifier"
    )
    assert validation_result_identifier_no_run_id.to_tuple() == (
        "my",
        "expectation",
        "suite",
        "name",
        "__none__",
        "20190926T134241.000000Z",
        "my_batch_identifier",
    )
    assert validation_result_identifier_no_run_id.to_fixed_length_tuple() == (
        "my.expectation.suite.name",
        "__none__",
        "20190926T134241.000000Z",
        "my_batch_identifier",
    )

    validation_result_identifier_no_batch_identifier = ValidationResultIdentifier(
        expectation_suite_identifier, "my_run_id", None
    )
    assert validation_result_identifier_no_batch_identifier.to_tuple() == (
        "my",
        "expectation",
        "suite",
        "name",
        "my_run_id",
        "20190926T134241.000000Z",
        "__none__",
    )
    assert validation_result_identifier_no_batch_identifier.to_fixed_length_tuple() == (
        "my.expectation.suite.name",
        "my_run_id",
        "20190926T134241.000000Z",
        "__none__",
    )

    validation_result_identifier_no_run_id_no_batch_identifier = (
        ValidationResultIdentifier(expectation_suite_identifier, None, None)
    )
    assert validation_result_identifier_no_run_id_no_batch_identifier.to_tuple() == (
        "my",
        "expectation",
        "suite",
        "name",
        "__none__",
        "20190926T134241.000000Z",
        "__none__",
    )
    assert validation_result_identifier_no_run_id_no_batch_identifier.to_fixed_length_tuple() == (
        "my.expectation.suite.name",
        "__none__",
        "20190926T134241.000000Z",
        "__none__",
    )
