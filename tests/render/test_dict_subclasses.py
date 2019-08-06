import pytest

from great_expectations.render.types import (
    DotDict,
    LimitedDotDict,
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


def test_LimitedDotDict_raises_error():
    with pytest.raises(KeyError):
        D = LimitedDotDict(**{
            'x': 1,
        })

    d = LimitedDotDict(**{})

    with pytest.raises(KeyError):
        d["x"] = "hello?"
    
    with pytest.raises(KeyError):
        d.x = "goodbye?"
    
    assert d.x == None

    with pytest.raises(KeyError):
        assert d["x"]


def test_LimitedDotDict_subclass():
    class MyLimitedDotDict(LimitedDotDict):
        _allowed_keys = set([
            "x", "y", "z"
        ])

    d = MyLimitedDotDict(**{
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

    with pytest.raises(KeyError):
        d["w"] = 100

    with pytest.raises(KeyError):
        d.w = 100

    assert d.w == None

    with pytest.raises(KeyError):
        assert d["w"]
