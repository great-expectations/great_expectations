import pytest

from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.render.util import (
    num_to_str,
    resource_key_passes_run_name_filter,
)


def test_num_to_str():
    f = 0.99999999999999
    # We can round
    assert num_to_str(f, precision=4) == "≈1"
    # Specifying extra precision should not cause a problem
    assert num_to_str(f, precision=20) == "0.99999999999999"

    f = 1234567890.123456  # Our float can only hold 17 significant digits
    assert num_to_str(f, precision=4) == "≈1.235e+9"
    assert num_to_str(f, precision=20) == "1234567890.123456"
    assert num_to_str(f, use_locale=True, precision=40) == "1,234,567,890.123456"

    f = 1.123456789012345  # 17 sig digits mostly after decimal
    assert num_to_str(f, precision=5) == "≈1.1235"
    assert num_to_str(f, precision=20) == "1.123456789012345"

    f = 0.1  # A classic difficulty for floating point arithmetic
    assert num_to_str(f) == "0.1"
    assert num_to_str(f, precision=20) == "0.1"
    assert num_to_str(f, no_scientific=True) == "0.1"

    f = 1.23456789012345e-10  # significant digits can come late
    assert num_to_str(f, precision=20) == "1.23456789012345e-10"
    assert num_to_str(f, precision=5) == "≈1.2346e-10"
    assert (
        num_to_str(f, precision=20, no_scientific=True) == "0.000000000123456789012345"
    )
    assert num_to_str(f, precision=5, no_scientific=True) == "≈0.00000000012346"

    f = 100.0  # floats should have trailing digits and numbers stripped
    assert num_to_str(f, precision=10, no_scientific=True) == "100"
    assert num_to_str(f, precision=10) == "100"
    assert num_to_str(f, precision=10, use_locale=True) == "100"

    f = 100  # integers should never be stripped!
    assert num_to_str(f, precision=10, no_scientific=True) == "100"
    assert num_to_str(f, precision=10) == "100"
    assert num_to_str(f, precision=10, use_locale=True) == "100"

    f = 1000  # If we have a number longer than our precision, we should still be able to correctly format
    assert num_to_str(f, precision=4) == "1000"
    assert num_to_str(f) == "1000"


def test_resource_key_passes_run_name_filter():
    resource_key = ValidationResultIdentifier(
        expectation_suite_identifier=ExpectationSuiteIdentifier("test_suite"),
        run_id=RunIdentifier(run_name="foofooprofilingfoo"),
        batch_identifier="f14c3d2f6e8028c2db0c25edabdb0d61",
    )

    assert (
        resource_key_passes_run_name_filter(
            resource_key, run_name_filter={"equals": "profiling"}
        )
        is False
    )
    assert (
        resource_key_passes_run_name_filter(
            resource_key, run_name_filter={"equals": "foofooprofilingfoo"}
        )
        is True
    )

    assert (
        resource_key_passes_run_name_filter(
            resource_key, run_name_filter={"not_equals": "profiling"}
        )
        is True
    )
    assert (
        resource_key_passes_run_name_filter(
            resource_key, run_name_filter={"not_equals": "foofooprofilingfoo"}
        )
        is False
    )

    assert (
        resource_key_passes_run_name_filter(
            resource_key, run_name_filter={"includes": "profiling"}
        )
        is True
    )
    assert (
        resource_key_passes_run_name_filter(
            resource_key, run_name_filter={"includes": "foobar"}
        )
        is False
    )

    assert (
        resource_key_passes_run_name_filter(
            resource_key, run_name_filter={"not_includes": "foobar"}
        )
        is True
    )
    assert (
        resource_key_passes_run_name_filter(
            resource_key, run_name_filter={"not_includes": "profiling"}
        )
        is False
    )

    assert (
        resource_key_passes_run_name_filter(
            resource_key,
            run_name_filter={"matches_regex": "(foo){2}profiling(" "foo)+"},
        )
        is True
    )
    assert (
        resource_key_passes_run_name_filter(
            resource_key,
            run_name_filter={"matches_regex": "(foo){3}profiling(" "foo)+"},
        )
        is False
    )
    with pytest.warns(DeprecationWarning):
        assert (
            resource_key_passes_run_name_filter(
                resource_key, run_name_filter={"eq": "profiling"}
            )
            is False
        )
        assert (
            resource_key_passes_run_name_filter(
                resource_key, run_name_filter={"eq": "foofooprofilingfoo"}
            )
            is True
        )
    with pytest.warns(DeprecationWarning):
        assert (
            resource_key_passes_run_name_filter(
                resource_key, run_name_filter={"ne": "profiling"}
            )
            is True
        )
        assert (
            resource_key_passes_run_name_filter(
                resource_key, run_name_filter={"ne": "foofooprofilingfoo"}
            )
            is False
        )
