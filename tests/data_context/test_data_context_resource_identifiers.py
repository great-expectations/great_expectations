from freezegun import freeze_time

from great_expectations.data_context.types.resource_identifiers import (
    ValidationResultIdentifier
)


@freeze_time("09/26/2019 13:42:41")
def test_ValidationResultIdentifier_to_tuple(expectation_suite_identifier):
    validation_result_identifier = ValidationResultIdentifier(
        expectation_suite_identifier,
        "my_run_id",
        "my_batch_identifier"
    )
    assert validation_result_identifier.to_tuple() == (
        "my", "expectation", "suite", "name",
        "my_run_id",
        "2019-09-26T13:42:41+00:00",
        "my_batch_identifier"
    )
    assert validation_result_identifier.to_fixed_length_tuple() == (
        "my.expectation.suite.name",
        "my_run_id",
        "2019-09-26T13:42:41+00:00",
        "my_batch_identifier"
    )

    validation_result_identifier_no_run_id = ValidationResultIdentifier(
        expectation_suite_identifier,
        None,
        "my_batch_identifier"
    )
    assert validation_result_identifier_no_run_id.to_tuple() == (
        "my", "expectation", "suite", "name",
        "__none__",
        "2019-09-26T13:42:41+00:00",
        "my_batch_identifier"
    )
    assert validation_result_identifier_no_run_id.to_fixed_length_tuple() == (
        "my.expectation.suite.name",
        "__none__",
        "2019-09-26T13:42:41+00:00",
        "my_batch_identifier"
    )

    validation_result_identifier_no_batch_identifier = ValidationResultIdentifier(
        expectation_suite_identifier,
        "my_run_id",
        None
    )
    assert validation_result_identifier_no_batch_identifier.to_tuple() == (
        "my", "expectation", "suite", "name",
        "my_run_id",
        "2019-09-26T13:42:41+00:00",
        "__none__",
    )
    assert validation_result_identifier_no_batch_identifier.to_fixed_length_tuple() == (
        "my.expectation.suite.name",
        "my_run_id",
        "2019-09-26T13:42:41+00:00",
        "__none__",
    )

    validation_result_identifier_no_run_id_no_batch_identifier = ValidationResultIdentifier(
        expectation_suite_identifier,
        None,
        None
    )
    assert validation_result_identifier_no_run_id_no_batch_identifier.to_tuple() == (
        "my", "expectation", "suite", "name",
        "__none__",
        "2019-09-26T13:42:41+00:00",
        "__none__"
    )
    assert validation_result_identifier_no_run_id_no_batch_identifier.to_fixed_length_tuple() == (
        "my.expectation.suite.name",
        "__none__",
        "2019-09-26T13:42:41+00:00",
        "__none__"
    )