"""
This file is intended to
1. test the basic behavior of SerializableDictDot, in combination with @dataclass, and
2. provides examples of best practice for working with typed objects within the Great Expectations codebase
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional, Tuple

import pytest
from pytest import raises

from great_expectations.types import DictDot, SerializableDictDot


@dataclass
class MyClassA(SerializableDictDot):
    foo: str
    bar: int


@dataclass
class MyClassB(MyClassA):
    baz: List[str]
    qux: Optional[int] = None
    quux: int = 42

    @property
    def num_bazzes(self):
        return len(self.baz)


class MyEnum(Enum):
    VALUE_X = "x"
    VALUE_Y = "y"
    VALUE_Z = "z"


@dataclass
class MyClassC(SerializableDictDot):
    alpha_var: int
    beta_var: MyEnum
    A_list: List[MyClassA]
    B_list: List[MyClassB]
    enum_list: List[MyEnum] = field(default_factory=list)
    some_tuple: Optional[Tuple[MyClassA, MyClassB]] = None

    @property
    def num_As(self):
        return len(self.A_list)

    @property
    def num_Bs(self):
        return len(self.B_list)


def test_access_using_dict_notation():
    "Keys can be accessed using dict notation"
    my_A = MyClassA(
        **{
            "foo": "a string",
            "bar": 1,
        }
    )
    assert my_A["foo"] == "a string"
    assert my_A["bar"] == 1


def test_has_keys():
    "the .keys method works"
    my_A = MyClassA(
        **{
            "foo": "a string",
            "bar": 1,
        }
    )
    assert set(my_A.keys()) == {"foo", "bar"}


def test_has_items():
    "the .items method works"
    my_A = MyClassA(
        **{
            "foo": "a string",
            "bar": 1,
        }
    )
    items = list(my_A.items())
    assert items == [("foo", "a string"), ("bar", 1)]


def test_has_to_dict():
    "the .to_dict method works"
    my_A = MyClassA(
        **{
            "foo": "a string",
            "bar": 1,
        }
    )
    assert my_A.to_dict() == {
        "foo": "a string",
        "bar": 1,
    }


@pytest.mark.skip(reason="Not sure what our preferred pattern for this is")
def test_incorrect_type():
    "Throws an error if instantiated with an incorrect type"
    MyClassA(**{"foo": "a string", "bar": "SHOULD BE AN INT", "baz": ["a", "b", "c"]})


def test_renders_to_a_useful_str():
    "Renders to a string that exposes the internals of the object"
    assert (
        MyClassA(
            foo="a string",
            bar=1,
        ).__str__()
        == """MyClassA(foo='a string', bar=1)"""
    )


@pytest.mark.skip(reason="hmmm. We should be able to make this work by default")
def test_is_json_serializable():
    assert MyClassA(
        foo="a string",
        bar=1,
    ).to_json_dict() == {"foo": "a string", "bar": 1}


def test_missing_keys():
    "Throws a TypeError if instantiated with missing arguments"

    with raises(TypeError):
        MyClassA()

    with raises(TypeError):
        MyClassA(
            foo="a string",
        )

    with raises(TypeError):
        MyClassA(bar=1)


def test_extra_keys():
    "Throws a TypeError if instantiated with extra arguments"

    with raises(TypeError):
        MyClassA(**{"foo": "a string", "bar": 1, "baz": ["a", "b", "c"]})


def test_update_after_instantiation():
    "Can be updated after instantiation, using both dot and dict notation"

    my_A = MyClassA(
        **{
            "foo": "a string",
            "bar": 1,
        }
    )
    assert my_A["foo"] == "a string"
    assert my_A["bar"] == 1

    my_A.foo = "different string"
    assert my_A["foo"] == "different string"
    assert my_A.foo == "different string"

    my_A["foo"] = "a third string"
    assert my_A["foo"] == "a third string"
    assert my_A.foo == "a third string"


def test_can_be_subclassed():
    my_B = MyClassB(
        foo="a string",
        bar=1,
        baz=["a", "b", "c"],
        qux=-100,
        quux=43,
    )

    assert my_B["foo"] == "a string"
    assert my_B["bar"] == 1
    assert my_B["baz"] == ["a", "b", "c"]
    assert my_B["qux"] == -100
    assert my_B["quux"] == 43

    assert my_B.foo == "a string"
    assert my_B.bar == 1
    assert my_B.baz == ["a", "b", "c"]
    assert my_B.qux == -100
    assert my_B.quux == 43

    my_B = MyClassB(
        **{
            "foo": "a string",
            "bar": 1,
            "baz": ["a", "b", "c"],
            "qux": -100,
            "quux": 43,
        }
    )

    assert my_B["foo"] == "a string"
    assert my_B["bar"] == 1
    assert my_B["baz"] == ["a", "b", "c"]
    assert my_B["qux"] == -100
    assert my_B["quux"] == 43

    assert my_B.foo == "a string"
    assert my_B.bar == 1
    assert my_B.baz == ["a", "b", "c"]
    assert my_B.qux == -100
    assert my_B.quux == 43


def test_can_have_derived_properties():
    "Can have derived properties"
    my_B = MyClassB(
        **{
            "foo": "a string",
            "bar": 1,
            "baz": ["a", "b", "c"],
            "qux": -100,
        }
    )
    assert my_B.num_bazzes == 3


def test_can_have_optional_arguments():
    "Can have optional arguments"
    my_B = MyClassB(
        **{
            "foo": "a string",
            "bar": 1,
            "baz": ["a", "b", "c"],
            # "qux": -100, #Optional property
        }
    )
    assert my_B.qux == None


def test_can_have_default_values():
    "Can have default values"
    my_B = MyClassB(
        **{
            "foo": "a string",
            "bar": 1,
            "baz": ["a", "b", "c"],
            "qux": -100,  # Optional property
        }
    )
    assert my_B.quux == 42


def test_can_use_positional_arguments():
    "Can use a normal mix of positional and keyword arguments"
    my_B = MyClassB(
        "a string",
        1,
        baz=["a", "b", "c"],
        qux=-100,
    )
    assert my_B.foo == "a string"
    assert my_B.bar == 1
    assert my_B.baz == ["a", "b", "c"]
    assert my_B.qux == -100

    my_B = MyClassB(
        qux=-100,
        baz=["a", "b", "c"],
        bar=1,
        foo="a string",
    )
    assert my_B.foo == "a string"
    assert my_B.bar == 1
    assert my_B.baz == ["a", "b", "c"]
    assert my_B.qux == -100


def test_can_be_nested():
    "Objects of this type can be nested"

    my_C = MyClassC(
        alpha_var=20,
        beta_var=MyEnum("x"),
        A_list=[
            MyClassA(
                foo="A-1",
                bar=101,
            ),
            MyClassA(
                **{
                    "foo": "A-2",
                    "bar": 102,
                }
            ),
        ],
        B_list=[
            MyClassB(
                **{
                    "foo": "B-1",
                    "bar": 201,
                    "baz": ["a", "b", "c"],
                    "qux": -100,
                    "quux": 43,
                }
            )
        ],
    )

    # Demonstrate that we can access sub-objects using a mix of dict and dot notation:
    assert my_C.A_list[0].foo == "A-1"
    assert my_C["A_list"][1].bar == 102
    assert my_C["B_list"][0]["quux"] == 43

    # Note: we don't currently support dot notation access within lists: `assert my_C["A_list"].1.bar == 102`

    # Demonstrate that we can access Enum sub-objects
    assert my_C["beta_var"] == MyEnum("x")


def test_to_dict_works_recursively():
    "the .to_dict method works recursively on both DotDicts and Enums"

    my_C = MyClassC(
        alpha_var=20,
        beta_var=MyEnum("x"),
        A_list=[
            MyClassA(
                foo="A-1",
                bar=101,
            ),
            MyClassA(
                **{
                    "foo": "A-2",
                    "bar": 102,
                }
            ),
        ],
        B_list=[
            MyClassB(
                **{
                    "foo": "B-1",
                    "bar": 201,
                    "baz": ["a", "b", "c"],
                    "qux": -100,
                    "quux": 43,
                }
            )
        ],
        enum_list=[
            MyEnum("x"),
            MyEnum("y"),
            MyEnum("z"),
        ],
        some_tuple=(
            MyClassA(
                foo="A-1",
                bar=101,
            ),
            MyClassB(
                foo="B-1",
                bar=201,
                baz=["a", "b", "c"],
                qux=-100,
                quux=43,
            ),
        ),
    )

    C_dict = my_C.to_dict()

    # Make sure it's a dictionary, not a DictDot
    assert type(C_dict) == dict
    assert isinstance(C_dict, DictDot) == False
    # Dictionaries don't support dot notation.
    with raises(AttributeError):
        C_dict.A_list

    assert type(C_dict["A_list"][0]) == dict
    assert type(C_dict["B_list"][0]) == dict

    assert C_dict == {
        "alpha_var": 20,
        "beta_var": "x",
        "A_list": [
            {
                "foo": "A-1",
                "bar": 101,
            },
            {
                "foo": "A-2",
                "bar": 102,
            },
        ],
        "B_list": [
            {
                "foo": "B-1",
                "bar": 201,
                "baz": ["a", "b", "c"],
                "qux": -100,
                "quux": 43,
            }
        ],
        "enum_list": [
            "x",
            "y",
            "z",
        ],
        "some_tuple": [
            {
                "foo": "A-1",
                "bar": 101,
            },
            {
                "foo": "B-1",
                "bar": 201,
                "baz": ["a", "b", "c"],
                "qux": -100,
                "quux": 43,
            },
        ],
    }


def test_instantiation_with_a_from_legacy_dict_method():
    """Can be instantiated from a legacy dictionary.

    Note: This pattern is helpful for cases where we're migrating from dictionary-based objects to typed objects.
    One especially thorny example is when the dictionary contains keys that are reserved words in python.

    For example, test cases use the reserved word: "in" as one of their required fields.
    """

    import inspect
    import logging

    @dataclass
    class MyClassE(SerializableDictDot):
        foo: str
        bar: int
        input: int

        @classmethod
        def from_legacy_dict(cls, dict):
            """This method is an adapter to allow typing of legacy my_class_e dictionary objects, without needing to immediately clean up every object."""
            temp_dict = {}
            for k, v in dict.items():
                # Ignore parameters that don't match the type definition
                if k in inspect.signature(cls).parameters:
                    temp_dict[k] = v
                else:
                    if k == "in":
                        temp_dict["input"] = v
                    else:
                        logging.warning(
                            f"WARNING: Got extra parameter: {k} while instantiating MyClassE."
                            "This parameter will be ignored."
                            "You probably need to clean up a library_metadata object."
                        )

            return cls(**temp_dict)

    my_E = MyClassE.from_legacy_dict(
        {
            "foo": "a string",
            "bar": 1,
            "in": 10,
        }
    )

    assert my_E["foo"] == "a string"
    assert my_E["bar"] == 1
    assert my_E["input"] == 10
    assert my_E.input == 10

    # Note that after instantiation, the class does NOT have an "in" property
    with raises(AttributeError):
        my_E["in"] == 10

    # Because `in` is a reserved word, this will raise a SyntaxError:
    # my_F.in == 100

    # Because `in` is a reserved word, this will also raise a SyntaxError:
    # my_F.in = 100
