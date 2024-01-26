import pytest

from great_expectations.analytics.anonymizer import Anonymizer


@pytest.mark.unit
def test_anonymizer_anonymize():
    anonymizer = Anonymizer(salt="salt")
    expected_hash = "162902aed084586c5ae5be2973bcbe35"
    assert anonymizer.anonymize("string") == expected_hash
