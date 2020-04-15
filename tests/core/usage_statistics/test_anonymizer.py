import uuid

from great_expectations.core.usage_statistics.anonymizers.anonymizer import Anonymizer


def test_anonymizer_no_salt():
    # No salt will generate a random one.
    anonymizer1 = Anonymizer()
    anonymizer2 = Anonymizer()

    test_name = "i_am_a_name"

    anon_name_1 = anonymizer1.anonymize(test_name)
    anon_name_2 = anonymizer2.anonymize(test_name)
    assert anon_name_1 != anon_name_2
    assert len(anon_name_1) == 32
    assert len(anon_name_2) == 32

    # Provided different salt will produce different results
    anonymizer1 = Anonymizer("hello, friend")
    anonymizer2 = Anonymizer("hello, enemy")

    test_name = "i_am_a_name"

    anon_name_1 = anonymizer1.anonymize(test_name)
    anon_name_2 = anonymizer2.anonymize(test_name)
    assert anon_name_1 != anon_name_2
    assert len(anon_name_1) == 32
    assert len(anon_name_2) == 32


def test_anonymizer_consistent_salt():
    # Provided same salt will produce same results
    data_context_id = str(uuid.uuid4())
    anonymizer1 = Anonymizer(data_context_id)
    anonymizer2 = Anonymizer(data_context_id)

    test_name = "i_am_a_name"

    anon_name_1 = anonymizer1.anonymize(test_name)
    anon_name_2 = anonymizer2.anonymize(test_name)
    assert anon_name_1 == anon_name_2
    assert len(anon_name_1) == 32
    assert len(anon_name_2) == 32
