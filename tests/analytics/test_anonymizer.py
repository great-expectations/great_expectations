import pytest

from great_expectations.analytics.anonymizer import anonymize


@pytest.mark.unit
def test_anonymizer_anonymize():
    assert anonymize("string") == "b45cffe084dd3d20d928bee85e7b0f21"
