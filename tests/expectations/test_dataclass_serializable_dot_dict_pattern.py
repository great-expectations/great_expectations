from pytest import raises
from dataclasses import (
    dataclass,
    FrozenInstanceError,
)
from enum import Enum
from typing import Any, Dict, List, Optional

import pytest
from great_expectations.types import (
    SerializableDictDot,
)

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

def test_basic_instantiation_with_arguments():
    "Can be instantiated with arguments"
    MyClassA(
        foo="a string",
        bar=1,
    )

def test_basic_instantiation_from_a_dictionary():
    "Can be instantiated from a dictionary"
    MyClassA(**{
        "foo": "a string",
        "bar": 1,
    })

def test_access_using_dot_notation():
    "Keys can be accessed using dot notation"
    my_A = MyClassA(**{
        "foo": "a string",
        "bar": 1,
    })
    assert my_A.foo == "a string"
    assert my_A.bar == 1
    
def test_access_using_dict_notation():
    "Keys can be accessed using dict notation"
    my_A = MyClassA(**{
        "foo": "a string",
        "bar": 1,
    })
    assert my_A["foo"] == "a string"
    assert my_A["bar"] == 1

def test_has_keys():
    "Keys can be accessed using dot notation"
    my_A = MyClassA(**{
        "foo": "a string",
        "bar": 1,
    })
    assert my_A.keys() == {"foo", "bar"}

@pytest.mark.skip(reason="Not sure what our preferred pattern for this is")
def test_incorrect_type():
    "Throws an error if instantiated with an incorrect type"
    MyClassA(**{
        "foo": "a string",
        "bar": "SHOULD BE AN INT",
        "baz": ["a", "b", "c"]
    })

def test_renders_to_a_useful_str():
    "Renders to a string that exposes the internals of the object"
    assert MyClassA(
        foo="a string",
        bar=1,    
    ).__str__() == """MyClassA(foo='a string', bar=1)"""


@pytest.mark.skip(reason="hmmm. We should be able to make this work by default")
def test_is_json_serializable():
    assert MyClassA(
        foo="a string",
        bar=1,    
    ).to_json_dict() == {
        "foo": "a string",
        "bar": 1
    }

def test_missing_keys():
    "Throws a TypeError if instantiated with missing arguments"

    with raises(TypeError):
        MyClassA()

    with raises(TypeError):
        MyClassA(
            foo="a string",
        )

    with raises(TypeError):
        MyClassA(
            bar=1
        )

def test_extra_keys():
    "Throws a TypeError if instantiated with extra arguments"

    with raises(TypeError):
        MyClassA(**{
            "foo": "a string",
            "bar": 1,
            "baz": ["a", "b", "c"]
        })

def test_update_after_instantiation():
    "Can be updated after instantiation, using both dot and dict notation"

    my_A = MyClassA(**{
        "foo": "a string",
        "bar": 1,
    })
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

    my_B = MyClassB(**{
        "foo": "a string",
        "bar": 1,
        "baz": ["a", "b", "c"],
        "qux": -100,
        "quux": 43,
    })

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
    my_B = MyClassB(**{
        "foo": "a string",
        "bar": 1,
        "baz": ["a", "b", "c"],
        "qux": -100,
    })
    assert my_B.num_bazzes == 3

def test_can_have_optional_arguments():
    "Can have optional arguments"
    my_B = MyClassB(**{
        "foo": "a string",
        "bar": 1,
        "baz": ["a", "b", "c"],
        # "qux": -100, #Optional property
    })
    assert my_B.qux == None

def test_can_have_default_values():
    "Can have default values"
    my_B = MyClassB(**{
        "foo": "a string",
        "bar": 1,
        "baz": ["a", "b", "c"],
        "qux": -100, #Optional property
    })
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

    @dataclass
    class MyClassC(SerializableDictDot):
        alpha_var: int
        beta_var: str
        A_list: List[MyClassA]
        B_list: List[MyClassB]

        @property
        def num_As(self):
            return len(self.A_list)

        @property
        def num_Bs(self):
            return len(self.B_list)

    
    my_C = MyClassC(
        alpha_var=20,
        beta_var="beta, I guess",
        A_list=[
            MyClassA(
                foo="A-1",
                bar=101,
            ),
            MyClassA(**{
                "foo": "A-2",
                "bar": 102,
            }),
        ],
        B_list=[
            MyClassB(**{
                "foo": "B-1",
                "bar": 201,
                "baz": ["a", "b", "c"],
                "qux": -100,
                "quux": 43,
            })
        ]
    )

    # Demonstrate that we can access sub-objects using a mix of dict and dot notation:
    assert my_C.A_list[0].foo == "A-1"
    assert my_C["A_list"][1].bar == 102
    assert my_C["B_list"][0]["quux"] == 43

    # Note: we don't currently support dot notation access within lists: `assert my_C["A_list"].1.bar == 102`


def test_immutability():
    "Can be made immutable"

    @dataclass(frozen=True)
    class MyClassD(SerializableDictDot):
        foo: str
        bar: int

    my_D = MyClassD(**{
        "foo": "a string",
        "bar": 1,
    })
    assert my_D["foo"] == "a string"
    assert my_D["bar"] == 1

    with raises(FrozenInstanceError):
        my_D.foo = "different string"

    assert my_D["foo"] == "a string"
    assert my_D.foo == "a string"

    with raises(FrozenInstanceError):
        my_D["foo"] = "a third string"

    assert my_D["foo"] == "a string"
    assert my_D.foo == "a string"




def test_reserved_word_key():
    "Can be instantiated with a key that's also a reserved word"

    @dataclass
    class MyClassE(SerializableDictDot):
        foo: str
        bar: int
        _in : int


    class MyClassF(MyClassE):
        def __init__(self,
            foo,
            bar,
            **kwargs,
        ):
            super().__init__(
                foo=foo,
                bar=bar,
                _in=kwargs["in"]
            )


    my_F = MyClassF(
        foo= "a string",
        bar= 1,
        **{"in": 10},
    )
    assert my_F["foo"] == "a string"
    assert my_F["bar"] == 1
    assert my_F["_in"] == 10
    assert my_F._in == 10

    my_F = MyClassF(**{
        "foo": "a string",
        "bar": 1,
        "in": 10,
    })
    assert my_F["foo"] == "a string"
    assert my_F["bar"] == 1
    assert my_F["_in"] == 10
    assert my_F._in == 10