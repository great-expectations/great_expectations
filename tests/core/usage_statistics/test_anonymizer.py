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
# They are used to ensure class hierarchy is appropriately processed by Anonymizer._get_parent_class()


class BaseTestClass:
    pass


class TestClass(BaseTestClass):
    pass


class MyCustomTestClass(TestClass):
    pass


class SomeOtherClass:
    pass


def test_anonymizer__get_parent_class():
    """
    What does this test and why?
    The method Anonymizer._get_parent_class() should return the name of the parent class if it is or is a subclass of one of the classes_to_check. If not, it should return None. It should do so regardless of the parameter used to pass in the object definition (object_, object_class, object_config). It should also return the first matching class in classes_to_check, even if a later class also matches.
    """
    anonymizer = Anonymizer()

    # As these classes are declared within the "tests" module,
    # we need to be explicit that they originate outside of "great_expectations"
    parent_module_prefix: str = "tests"

    assert (
        anonymizer._get_parent_class(
            object_class=MyCustomTestClass, parent_module_prefix=parent_module_prefix
        )
        == "TestClass"
    )
    assert (
        anonymizer._get_parent_class(
            object_class=SomeOtherClass, parent_module_prefix=parent_module_prefix
        )
        is None
    )
    assert (
        anonymizer._get_parent_class(
            object_class=TestClass, parent_module_prefix=parent_module_prefix
        )
        == "BaseTestClass"
    )

    # classes_to_check in order of inheritance hierarchy
    my_custom_test_class = MyCustomTestClass()
    test_class = TestClass()
    some_other_class = SomeOtherClass()
    assert (
        anonymizer._get_parent_class(
            object_=my_custom_test_class, parent_module_prefix=parent_module_prefix
        )
        == "TestClass"
    )
    assert (
        anonymizer._get_parent_class(
            object_=some_other_class, parent_module_prefix=parent_module_prefix
        )
        is None
    )
    assert (
        anonymizer._get_parent_class(
            object_=test_class, parent_module_prefix=parent_module_prefix
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
    assert (
        anonymizer._get_parent_class(
            object_config=my_custom_test_class_config,
            parent_module_prefix=parent_module_prefix,
        )
        == "TestClass"
    )
    assert (
        anonymizer._get_parent_class(
            object_config=some_other_class_config,
            parent_module_prefix=parent_module_prefix,
        )
        is None
    )
    assert (
        anonymizer._get_parent_class(
            object_config=test_class_config, parent_module_prefix=parent_module_prefix
        )
        == "BaseTestClass"
    )
