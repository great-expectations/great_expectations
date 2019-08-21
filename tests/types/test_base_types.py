import pytest

from six import PY2, string_types
import sys

from great_expectations.types import (
    DotDict,
    LooselyTypedDotDict,
    RequiredKeysDotDict,
    ListOf,
)

from ruamel.yaml import YAML, yaml_object
yaml = YAML()

"""
* dictionary syntax works for assignment and lookup `myobj["a"] = 10`, `print(myobjj["a"])`
* dot notation works for assignment and lookup `my_obj.a = 10`, `print(my obj.a)`
* Adding an unknown key raises an error
* Keys can be optional

* Values can be typed
"""


def test_DotDict_dictionary_syntax():
    D = DotDict({
        'x': [1, 2, 4],
        'y': [1, 2, 5],
        'z': ['hello', 'jello', 'mello'],
    })
    D["w"] = 10
    assert D["x"][0] == D["y"][0]
    assert D["w"] == 10


def test_DotDict_dot_syntax():
    D = DotDict({
        'x': [1, 2, 4],
        'y': [1, 2, 5],
        'z': ['hello', 'jello', 'mello'],
    })
    assert D.x[0] == D.y[0]
    assert D.x[0] != D.z[0]

    d = DotDict()
    d["y"] = 2
    assert d.y == 2
    assert d["y"] == 2

    d.x = 1
    assert d.x == 1
    assert d["x"] == 1

    assert d == {
        "x" : 1,
        "y" : 2
    }


def test_LooselyTypedDotDict_raises_error():
    with pytest.raises(KeyError):
        D = LooselyTypedDotDict(**{
            'x': 1,
        })

    d = LooselyTypedDotDict(**{})

    with pytest.raises(KeyError):
        d["x"] = "hello?"
    
    with pytest.raises(KeyError):
        d.x = "goodbye?"

    # Note that we raise AttributeError instead of KeyError here since the
    # intended semantics when using dot notation are class property style
    with pytest.raises(AttributeError):
        assert d.x is None

    # In contrast, when we explicitly use the dictionary notation, we get
    # the KeyError, also following the standard conventions
    with pytest.raises(KeyError):
        assert d["x"] is None


def test_LooselyTypedDotDict_subclass():
    class MyLooselyTypedDotDict(LooselyTypedDotDict):
        _allowed_keys = {
            "x", "y", "z"
        }

    d = MyLooselyTypedDotDict(**{
        'x': 1,
    })
    assert d.x == 1
    assert d["x"] == 1
    
    d["y"] = 100
    assert d["y"] == 100
    assert d.y == 100

    d.z = "estella"
    assert d.z == "estella"
    assert d["z"] == "estella"

    assert d == {
        "x": 1,
        "y": 100,
        "z": "estella",
    }

    del d["x"]

    assert d == {
        "y": 100,
        "z": "estella",
    }

    with pytest.raises(KeyError):
        d["w"] = 100

    with pytest.raises(KeyError):
        d.w = 100

    with pytest.raises(AttributeError):
        assert d.w is None

    with pytest.raises(KeyError):
        assert d["w"] is None


def test_LooselyTypedDotDict_subclass_required_keys():
    class MyLooselyTypedDotDict(LooselyTypedDotDict):
        _allowed_keys = set([
            "x", "y", "z"
        ])
        _required_keys = set([
            "x"
        ])

    with pytest.raises(KeyError):
        d = MyLooselyTypedDotDict(**{
            'y': 1,
        })

    d = MyLooselyTypedDotDict(**{
        'x': 1,
    })
    assert d.x == 1

    d.x += 10
    assert d.x == 11

    d["x"] = "hi"
    assert d.x == "hi"

    # Can't delete a required key
    with pytest.raises(KeyError):
        del d["x"]

    # _required_keys must be a subset of _allowed_keys
    with pytest.raises(ValueError):
        class MyLooselyTypedDotDict(LooselyTypedDotDict):
            _allowed_keys = set([
                "x", "y", "z"
            ])
            _required_keys = set([
                "w"
            ])
        
        #Unfortunately, I don't have a good way to test this condition until the class is instantiated
        d = MyLooselyTypedDotDict(x=True)


def test_LooselyTypedDotDict_subclass_key_types():
    class MyLooselyTypedDotDict(LooselyTypedDotDict):
        _allowed_keys = set([
            "x", "y", "z"
        ])
        _required_keys = set([
            "x",
        ])
        _key_types = {
            "x" : int,
            "y" : str,
        }

    d = MyLooselyTypedDotDict(**{
        'x': 1,
    })

    with pytest.raises(TypeError):
        d = MyLooselyTypedDotDict(**{
            'x': "1",
        })

    d = MyLooselyTypedDotDict(**{
        "x": 1,
        "y": "hello",
    })

    with pytest.raises(TypeError):
        d = MyLooselyTypedDotDict(**{
            'x': 1,
            'y': 10,
        })

    d = MyLooselyTypedDotDict(
        coerce_types=True,
        **{
            "x": "1",
            "y": 10
        }
    )
    assert d == {
        "x": 1,
        "y": "10",
    }

    with pytest.raises(TypeError):
        d["x"] = "not an int"

    with pytest.raises(TypeError):
        d.x = "not an int"

    with pytest.raises(ValueError):
        d = MyLooselyTypedDotDict(
            coerce_types=True,
            **{
                "x": "quack",
            }
        )

def test_LooselyTypedDotDict_ListOf_typing():
    class MyLooselyTypedDotDict(LooselyTypedDotDict):
        _allowed_keys = set([
            "a", "b", "c"
        ])
        _key_types = {
            "a" : int,
            "b" : ListOf(int),
        }
    
    d = MyLooselyTypedDotDict(
        **{
            "a" : 10,
            "b" : [10, 20, 30]
        }
    )

    d = MyLooselyTypedDotDict(
        coerce_types=True,
        **{
            "a" : 10,
            "b" : ["10", "20", "30"]
        }
    )

    with pytest.raises(TypeError):
        d = MyLooselyTypedDotDict(
            **{
                "a" : 10,
                "b" : [10, 20, "rabbit"]
            }
        )

def test_LooselyTypedDotDict_recursive_coercion():
    class MyNestedDotDict(LooselyTypedDotDict):
        _allowed_keys = set([
            "a", "b", "c"
        ])
        _required_keys = set([
            "a",
        ])
        _key_types = {
            "a" : int,
            "b" : str,
        }

    class MyLooselyTypedDotDict(LooselyTypedDotDict):
        _allowed_keys = set([
            "x", "y", "z"
        ])
        _required_keys = set([
            "x",
        ])
        _key_types = {
            "x" : str,
            "y" : MyNestedDotDict,
        }

    d = MyLooselyTypedDotDict(
        coerce_types=True,
        **{
            "x" : "hello",
            "y" : {
                "a" : 1,
                "b" : "hello"
            },
        }
    )
    assert d == {
        "x" : "hello",
        "y" : {
            "a" : 1,
            "b" : "hello"
        },
    }

    with pytest.raises(ValueError):
        MyLooselyTypedDotDict(
            coerce_types=True,
            **{
                "x" : "hello",
                "y" : {
                    "a" : "broken",
                },
            }
        )

    with pytest.raises(KeyError):
        MyLooselyTypedDotDict(
            coerce_types=True,
            **{
                "x" : "hello",
                "y" : {
                    "b" : "wait, a is required!",
                },
            }
        )

def test_LooselyTypedDotDict_recursive_coercion_with_ListOf():
    class MyNestedDotDict(LooselyTypedDotDict):
        _allowed_keys = set([
            "a",
        ])
        _required_keys = set([
            "a",
        ])
        _key_types = {
            "a" : int,
        }

    class MyLooselyTypedDotDict(LooselyTypedDotDict):
        _allowed_keys = set([
            "x", "y", "z"
        ])
        _required_keys = set([
            "x",
        ])
        _key_types = {
            "x" : str,
            "y" : ListOf(MyNestedDotDict),
        }

    d = MyLooselyTypedDotDict(
        coerce_types=True,
        **{
            "x" : "hello",
            "y" : [
                {"a": 1},
                {"a": 2},
                {"a": 3},
                {"a": 4}
            ]
        }
    )
    assert d == {
        "x" : "hello",
        "y" : [
            {"a": 1},
            {"a": 2},
            {"a": 3},
            {"a": 4}    
        ]
    }

    with pytest.raises(TypeError):
        d = MyLooselyTypedDotDict(
            coerce_types=True,
            **{
                "x" : "hello",
                "y" : {
                    "a" : [1, 2, 3, 4],
                },
            }
        )


def test_LooselyTypedDotDict_unicode_issues():
    class MyLTDD(LooselyTypedDotDict):
        _allowed_keys = set([
            "a",
        ])
        _required_keys = set([
            "a",
        ])
        _key_types = {
            "a" : str,
        }

    MyLTDD(**{
        "a" : "hello"
    })

    if PY2:
        with pytest.raises(TypeError):
            MyLTDD(**{
                "a" : u"hello"
            })

    class MyLTDD(LooselyTypedDotDict):
        _allowed_keys = set([
            "a",
        ])
        _required_keys = set([
            "a",
        ])
        _key_types = {
            "a" : string_types,
        }

    MyLTDD(**{
        "a" : "hello"
    })

    MyLTDD(**{
        "a" : u"hello"
    })


def test_LooselyTypedDotDict_multiple_types():
    class MyLTDD(LooselyTypedDotDict):
        _allowed_keys = set(["a"])
        _key_types = {
            "a" : [int, float],
        }
    
    A = MyLTDD(**{
        "a" : 1
    })

    B = MyLTDD(**{
        "a" : 1.5
    })

    with pytest.raises(TypeError):
        B = MyLTDD(**{
            "a" : "string"
        })

    with pytest.raises(TypeError):
        B = MyLTDD(**{
            "a" : None
        })

    class MyLTDD(LooselyTypedDotDict):
        _allowed_keys = set(["a"])
        _key_types = {
            "a" : [int, None],
        }
    
    A = MyLTDD(**{
        "a" : 1
    })

    B = MyLTDD(**{
        "a" : None
    })

def test_dotdict_yaml_serialization(capsys):
    # To enable yaml serialization, we simply annotate the class as a @yaml_object
    # Note that this annotation be inherited, even though in our case we know that

    # NOTE: JPC - 20190821: We may want to reach out to the ruamel authors to inquire about
    # adding support for searching through the entire mro (instead of only the last entry) for supported representers

    # Note that we are also using the nested type coercion in this case.
    @yaml_object(yaml)
    class MyLooselyTypedDotDict(LooselyTypedDotDict):
        _allowed_keys = {"a", "b"}
        _required_keys = {"a"}
        _key_types = {
            "a": str,
            "b": RequiredKeysDotDict
        }

    d = MyLooselyTypedDotDict({
            "a": "fish",
            "b": {
                "pet": "dog"
            }
        },
        coerce_types=True
    )

    yaml.dump(d, sys.stdout)

    assert """\
a: fish
b:
  pet: dog
""" in capsys.readouterr()


def test_required_keys_dotdict():
    class MyRequiredKeysDotDict(RequiredKeysDotDict):
        _required_keys = {"class_name"}

    with pytest.raises(KeyError):
        # required key is missing
        d = MyRequiredKeysDotDict({"blarg": "isarealthreat"})

    d = MyRequiredKeysDotDict({"class_name": "physics", "blarg": "isarealthreat"})
    assert d["class_name"] == d.class_name
    assert d["blarg"] == "isarealthreat"

    # Note that since there is not a concept of allowed_keys, we do not raise attributeerror here, picking
    # up dictionary semantics instead
    assert d.doesnotexist is None
