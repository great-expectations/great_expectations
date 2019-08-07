import pytest

from great_expectations.types import (
    DotDict,
    LooselyTypedDotDict,
    ListOf,
)

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
    
    assert d.x == None

    with pytest.raises(KeyError):
        assert d["x"]


def test_LooselyTypedDotDict_subclass():
    class MyLooselyTypedDotDict(LooselyTypedDotDict):
        _allowed_keys = set([
            "x", "y", "z"
        ])

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

    assert d.w == None

    with pytest.raises(KeyError):
        assert d["w"]


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
