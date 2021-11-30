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


# The following empty classes are used in this test module only.
# They are used to ensure class hierarchy is appropriately processed by Anonymizer._is_parent_class_recognized()


class BaseTestClass:
    pass


class TestClass(BaseTestClass):
    pass


class MyCustomTestClass(TestClass):
    pass


class SomeOtherClass:
    pass


def test_anonymizer__is_parent_class_recognized():
    """
    What does this test and why?
    The method Anonymizer._is_parent_class_recognized() should return the name of the parent class if it is or is a subclass of one of the classes_to_check. If not, it should return None. It should do so regardless of the parameter used to pass in the object definition (object_, object_class, object_config). It should also return the first matching class in classes_to_check, even if a later class also matches.
    """
    anonymizer = Anonymizer()

    # classes_to_check in order of inheritance hierarchy
    classes_to_check = [TestClass, BaseTestClass]
    assert (
        anonymizer._is_parent_class_recognized(
            classes_to_check=classes_to_check, object_class=MyCustomTestClass
        )
        == "TestClass"
    )
    assert (
        anonymizer._is_parent_class_recognized(
            classes_to_check=classes_to_check, object_class=SomeOtherClass
        )
        is None
    )
    classes_to_check = [BaseTestClass]
    assert (
        anonymizer._is_parent_class_recognized(
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
        anonymizer._is_parent_class_recognized(
            classes_to_check=classes_to_check, object_=my_custom_test_class
        )
        == "TestClass"
    )
    assert (
        anonymizer._is_parent_class_recognized(
            classes_to_check=classes_to_check, object_=some_other_class
        )
        is None
    )
    classes_to_check = [BaseTestClass]
    assert (
        anonymizer._is_parent_class_recognized(
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
        anonymizer._is_parent_class_recognized(
            classes_to_check=classes_to_check, object_config=my_custom_test_class_config
        )
        == "TestClass"
    )
    assert (
        anonymizer._is_parent_class_recognized(
            classes_to_check=classes_to_check, object_config=some_other_class_config
        )
        is None
    )
    classes_to_check = [BaseTestClass]
    assert (
        anonymizer._is_parent_class_recognized(
            classes_to_check=classes_to_check, object_config=test_class_config
        )
        == "BaseTestClass"
    )
