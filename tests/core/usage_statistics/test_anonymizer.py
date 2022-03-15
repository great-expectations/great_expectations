import uuid

import pytest

from great_expectations.core.batch import BatchRequest
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.usage_statistics.anonymizers.anonymizer import Anonymizer


@pytest.fixture
def anonymizer_with_consistent_salt() -> Anonymizer:
    anonymizer: Anonymizer = Anonymizer(salt="00000000-0000-0000-0000-00000000a004")
    return anonymizer


# The following empty classes are used in this test module only.
# They are used to ensure class hierarchy is appropriately processed by Anonymizer utilities.


class BaseTestClass:
    pass


class TestClass(BaseTestClass):
    pass


class MyCustomTestClass(TestClass):
    pass


class SomeOtherClass:
    pass


class MyCustomExpectationSuite(ExpectationSuite):
    pass


class MyCustomMultipleInheritanceClass(ExpectationSuite, BatchRequest):
    pass


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


def test_anonymizer_get_parent_class():
    """
    What does this test and why?
    The method Anonymizer.get_parent_class() should return the name of the parent class if it is or is a subclass of one of the classes_to_check. If not, it should return None. It should do so regardless of the parameter used to pass in the object definition (object_, object_class, object_config). It should also return the first matching class in classes_to_check, even if a later class also matches.
    """
    anonymizer = Anonymizer()

    # classes_to_check in order of inheritance hierarchy
    classes_to_check = [TestClass, BaseTestClass]
    assert (
        anonymizer.get_parent_class(
            classes_to_check=classes_to_check, object_class=MyCustomTestClass
        )
        == "TestClass"
    )
    assert (
        anonymizer.get_parent_class(
            classes_to_check=classes_to_check, object_class=SomeOtherClass
        )
        is None
    )
    classes_to_check = [BaseTestClass]
    assert (
        anonymizer.get_parent_class(
            classes_to_check=classes_to_check, object_class=TestClass
        )
        == "BaseTestClass"
    )

    # classes_to_check in order of inheritance hierarchy
    my_custom_test_class = MyCustomTestClass()
    test_class = TestClass()
    some_other_class = SomeOtherClass()
    classes_to_check = [TestClass, BaseTestClass]
    assert (
        anonymizer.get_parent_class(
            classes_to_check=classes_to_check, object_=my_custom_test_class
        )
        == "TestClass"
    )
    assert (
        anonymizer.get_parent_class(
            classes_to_check=classes_to_check, object_=some_other_class
        )
        is None
    )
    classes_to_check = [BaseTestClass]
    assert (
        anonymizer.get_parent_class(
            classes_to_check=classes_to_check, object_=test_class
        )
        == "BaseTestClass"
    )

    # classes_to_check in order of inheritance hierarchy
    my_custom_test_class_config = {
        "class_name": "MyCustomTestClass",
        "module_name": "tests.core.usage_statistics.test_anonymizer",
    }
    test_class_config = {
        "class_name": "TestClass",
        "module_name": "tests.core.usage_statistics.test_anonymizer",
    }
    some_other_class_config = {
        "class_name": "SomeOtherClass",
        "module_name": "tests.core.usage_statistics.test_anonymizer",
    }
    classes_to_check = [TestClass, BaseTestClass]
    assert (
        anonymizer.get_parent_class(
            classes_to_check=classes_to_check, object_config=my_custom_test_class_config
        )
        == "TestClass"
    )
    assert (
        anonymizer.get_parent_class(
            classes_to_check=classes_to_check, object_config=some_other_class_config
        )
        is None
    )
    classes_to_check = [BaseTestClass]
    assert (
        anonymizer.get_parent_class(
            classes_to_check=classes_to_check, object_config=test_class_config
        )
        == "BaseTestClass"
    )


def test_anonymize_object_info_with_core_ge_object(
    anonymizer_with_consistent_salt: Anonymizer,
):
    anonymized_result: dict = anonymizer_with_consistent_salt.anonymize_object_info(
        anonymized_info_dict={},
        object_=ExpectationSuite(expectation_suite_name="my_suite"),
    )

    assert anonymized_result == {"parent_class": "ExpectationSuite"}


def test_anonymize_object_info_with_custom_user_defined_object_with_single_parent(
    anonymizer_with_consistent_salt: Anonymizer,
):
    anonymized_result: dict = anonymizer_with_consistent_salt.anonymize_object_info(
        anonymized_info_dict={},
        object_=MyCustomExpectationSuite(expectation_suite_name="my_suite"),
    )

    assert anonymized_result == {
        "anonymized_class": "54ab2657b855f8075e5d1f28e81ca7cd",
        "parent_class": "ExpectationSuite",
    }


def test_anonymize_object_info_with_custom_user_defined_object_with_no_parent(
    anonymizer_with_consistent_salt: Anonymizer,
):
    anonymized_result: dict = anonymizer_with_consistent_salt.anonymize_object_info(
        anonymized_info_dict={}, object_=BaseTestClass()
    )

    assert anonymized_result == {
        "anonymized_class": "760bfe8b56356bcd56012edfd512019b",
        "parent_class": "__not_recognized__",
    }


def test_anonymize_object_info_with_custom_user_defined_object_with_multiple_parents(
    anonymizer_with_consistent_salt: Anonymizer,
):
    anonymized_result: dict = anonymizer_with_consistent_salt.anonymize_object_info(
        anonymized_info_dict={},
        object_=MyCustomMultipleInheritanceClass(expectation_suite_name="my_name"),
    )

    assert anonymized_result == {
        "anonymized_class": "1e1716661acfa73d538a191ed13efcfd",
        "parent_class": "ExpectationSuite,BatchRequest",
    }


def test_anonymize_object_info_with_missing_args_raises_error(
    anonymizer_with_consistent_salt: Anonymizer,
):
    with pytest.raises(AssertionError) as e:
        anonymizer_with_consistent_salt.anonymize_object_info(
            anonymized_info_dict={},
            object_=None,
            object_class=None,
            object_config=None,
        )

    assert "Must pass either" in str(e.value)
