# import pytest
#
# from six import PY2, string_types
# import sys
#
# from great_expectations.exceptions import (
#     MissingTopLevelConfigKeyError,
#     InvalidConfigValueTypeError,
#     InvalidTopLevelConfigKeyError,
# )
# from great_expectations.types import (
#     DotDict,
#     AllowedKeysDotDict,
#     RequiredKeysDotDict,
#     OrderedKeysDotDict,
#     ListOf,
#     DictOf,
# )
# from great_expectations.data_context.types import (
#     ValidationResultIdentifier,
#     DataAssetIdentifier,
# )
#
# from ruamel.yaml import YAML, yaml_object
# yaml = YAML()
#
# """
# * dictionary syntax works for assignment and lookup `myobj["a"] = 10`, `print(myobjj["a"])`
# * dot notation works for assignment and lookup `my_obj.a = 10`, `print(my obj.a)`
# * Adding an unknown key raises an error
# * Keys can be optional
#
# * Values can be typed
# """

#
# def test_DotDict_dictionary_syntax():
#     D = DotDict({
#         'x': [1, 2, 4],
#         'y': [1, 2, 5],
#         'z': ['hello', 'jello', 'mello'],
#     })
#     D["w"] = 10
#     assert D["x"][0] == D["y"][0]
#     assert D["w"] == 10
#
#
# def test_DotDict_dot_syntax():
#     D = DotDict({
#         'x': [1, 2, 4],
#         'y': [1, 2, 5],
#         'z': ['hello', 'jello', 'mello'],
#     })
#     assert D.x[0] == D.y[0]
#     assert D.x[0] != D.z[0]
#
#     d = DotDict()
#     d["y"] = 2
#     assert d.y == 2
#     assert d["y"] == 2
#
#     d.x = 1
#     assert d.x == 1
#     assert d["x"] == 1
#
#     assert d == {
#         "x": 1,
#         "y": 2
#     }
#
#
# def test_allowed_keys_dot_dict_raises_error():
#     with pytest.raises(InvalidTopLevelConfigKeyError):
#         D = AllowedKeysDotDict(**{
#             'x': 1,
#         })
#
#     d = AllowedKeysDotDict(**{})
#
#     with pytest.raises(InvalidTopLevelConfigKeyError):
#         d["x"] = "hello?"
#
#     with pytest.raises(InvalidTopLevelConfigKeyError):
#         d.x = "goodbye?"
#
#     # Note that we raise AttributeError instead of KeyError here since the
#     # intended semantics when using dot notation are class property style
#     with pytest.raises(AttributeError):
#         assert d.x is None
#
#     # In contrast, when we explicitly use the dictionary notation, we get
#     # the KeyError, also following the standard conventions
#     with pytest.raises(KeyError):
#         assert d["x"] is None
#
#
# def test_allowed_keys_dot_dict_subclass():
#     class MyAllowedKeysDotDict(AllowedKeysDotDict):
#         _allowed_keys = {
#             "x", "y", "z"
#         }
#
#     d = MyAllowedKeysDotDict(**{
#         'x': 1,
#     })
#     assert d.x == 1
#     assert d["x"] == 1
#
#     d["y"] = 100
#     assert d["y"] == 100
#     assert d.y == 100
#
#     d.z = "estella"
#     assert d.z == "estella"
#     assert d["z"] == "estella"
#
#     assert d == {
#         "x": 1,
#         "y": 100,
#         "z": "estella",
#     }
#
#     del d["x"]
#
#     assert d == {
#         "y": 100,
#         "z": "estella",
#     }
#
#     with pytest.raises(InvalidTopLevelConfigKeyError):
#         d["w"] = 100
#
#     with pytest.raises(InvalidTopLevelConfigKeyError):
#         d.w = 100
#
#     with pytest.raises(AttributeError):
#         assert d.w is None
#
#     with pytest.raises(KeyError):
#         assert d["w"] is None
#
#
# def test_allowed_keys_dot_dict_subclass_required_keys():
#     class MyAllowedKeysDotDict(AllowedKeysDotDict):
#         _allowed_keys = {"x", "y", "z"}
#         _required_keys = {"x"}
#
#     with pytest.raises(MissingTopLevelConfigKeyError):
#         d = MyAllowedKeysDotDict(**{
#             'y': 1,
#         })
#
#     d = MyAllowedKeysDotDict(**{
#         'x': 1,
#     })
#     assert d.x == 1
#
#     d.x += 10
#     assert d.x == 11
#
#     d["x"] = "hi"
#     assert d.x == "hi"
#
#     # Can't delete a required key
#     with pytest.raises(KeyError):
#         del d["x"]
#
#     # _required_keys must be a subset of _allowed_keys
#     with pytest.raises(ValueError):
#         class MyAllowedKeysDotDict(AllowedKeysDotDict):
#             _allowed_keys = {"x", "y", "z"}
#             _required_keys = {"w"}
#
#         #Unfortunately, I don't have a good way to test this condition until the class is instantiated
#         d = MyAllowedKeysDotDict(x=True)
#
#
# def test_allowed_keys_dot_dict_subclass_key_types():
#     class MyAllowedKeysDotDict(AllowedKeysDotDict):
#         _allowed_keys = {"x", "y", "z"}
#         _required_keys = { "x" }
#         _key_types = {
#             "x": int,
#             "y": str,
#         }
#
#     d = MyAllowedKeysDotDict(**{
#         'x': 1,
#     })
#
#     with pytest.raises(InvalidConfigValueTypeError):
#         d = MyAllowedKeysDotDict(**{
#             'x': "1",
#         })
#
#     d = MyAllowedKeysDotDict(**{
#         "x": 1,
#         "y": "hello",
#     })
#
#     with pytest.raises(InvalidConfigValueTypeError):
#         d = MyAllowedKeysDotDict(**{
#             'x': 1,
#             'y': 10,
#         })
#
#     d = MyAllowedKeysDotDict(
#         coerce_types=True,
#         **{
#             "x": "1",
#             "y": 10
#         }
#     )
#     assert d == {
#         "x": 1,
#         "y": "10",
#     }
#
#     with pytest.raises(InvalidConfigValueTypeError):
#         d["x"] = "not an int"
#
#     with pytest.raises(InvalidConfigValueTypeError):
#         d.x = "not an int"
#
#     with pytest.raises(ValueError):
#         d = MyAllowedKeysDotDict(
#             coerce_types=True,
#             **{
#                 "x": "quack",
#             }
#         )
#
#
# def test_allowed_keys_dot_dict_ListOf_typing():
#     class MyAllowedKeysDotDict(AllowedKeysDotDict):
#         _allowed_keys = {"a", "b", "c" }
#         _key_types = {
#             "a": int,
#             "b": ListOf(int),
#         }
#
#     d = MyAllowedKeysDotDict(
#         **{
#             "a": 10,
#             "b": [10, 20, 30]
#         }
#     )
#
#     d = MyAllowedKeysDotDict(
#         coerce_types=True,
#         **{
#             "a": 10,
#             "b": ["10", "20", "30"]
#         }
#     )
#
#     with pytest.raises(TypeError):
#         d = MyAllowedKeysDotDict(
#             **{
#                 "a": 10,
#                 "b": [10, 20, "rabbit"]
#             }
#         )
#
# def test_allowed_keys_dot_dict_DictOf_typing():
#     class MyAllowedKeysDotDict(AllowedKeysDotDict):
#         _allowed_keys = {"a", "b", "c" }
#         _key_types = {
#             "a": int,
#             "b": DictOf(int),
#         }
#
#     d = MyAllowedKeysDotDict(
#         **{
#             "a": 10,
#             "b": {
#                 "x" : 10,
#                 "y" : 20,
#                 "z" : 30
#             }
#         }
#     )
#
#     d = MyAllowedKeysDotDict(
#         coerce_types=True,
#         **{
#             "a": 10,
#             "b": {
#                 "x" : "10",
#                 "y" : "20",
#                 "z" : "30"
#             }
#         }
#     )
#
#     with pytest.raises(TypeError):
#         d = MyAllowedKeysDotDict(
#             **{
#                 "a": 10,
#                 "b": {
#                     "x" : 10,
#                     "y" : 20,
#                     "z" : "duck"
#                 }
#             }
#         )
#
#     with pytest.raises(TypeError):
#         d = MyAllowedKeysDotDict(
#             **{
#                 "a": 10,
#                 "b": [10, 20, 30],
#             }
#         )
#
# def test_allowed_keys_dot_dict_recursive_coercion():
#     class MyNestedDotDict(AllowedKeysDotDict):
#         _allowed_keys = {"a", "b", "c", "d"}
#         _required_keys = {"a"}
#         _key_types = {
#             "a": int,
#             "b": str,
#             "d": string_types,
#         }
#
#     class MyAllowedKeysDotDict(AllowedKeysDotDict):
#         _allowed_keys = {"x", "y", "z"}
#         _required_keys = {"x"}
#         _key_types = {
#             "x": str,
#             "y": MyNestedDotDict,
#         }
#
#     d = MyAllowedKeysDotDict(
#         coerce_types=True,
#         **{
#             "x": "hello",
#             "y": {
#                 "a": 1,
#                 "b": "hello",
#                 "d": "hi"
#             },
#         }
#     )
#     assert d == {
#         "x": "hello",
#         "y": {
#             "a": 1,
#             "b": "hello",
#             "d": "hi",
#         },
#     }
#
#     with pytest.raises(ValueError):
#         MyAllowedKeysDotDict(
#             coerce_types=True,
#             **{
#                 "x": "hello",
#                 "y": {
#                     "a": "broken",
#                 },
#             }
#         )
#
#     with pytest.raises(MissingTopLevelConfigKeyError):
#         MyAllowedKeysDotDict(
#             coerce_types=True,
#             **{
#                 "x": "hello",
#                 "y": {
#                     "b": "wait, a is required!",
#                 },
#             }
#         )
#
# def test_allowed_keys_dot_dict_recursive_coercion_with_ListOf():
#     class MyNestedDotDict(AllowedKeysDotDict):
#         _allowed_keys = {"a"}
#         _required_keys = {"a"}
#         _key_types = {
#             "a": int,
#         }
#
#     class MyAllowedKeysDotDict(AllowedKeysDotDict):
#         _allowed_keys = {"x", "y", "z"}
#         _required_keys = {"x"}
#         _key_types = {
#             "x": str,
#             "y": ListOf(MyNestedDotDict),
#         }
#
#     d = MyAllowedKeysDotDict(
#         coerce_types=True,
#         **{
#             "x": "hello",
#             "y": [
#                 {"a": 1},
#                 {"a": 2},
#                 {"a": 3},
#                 {"a": 4}
#             ]
#         }
#     )
#     assert d == {
#         "x": "hello",
#         "y": [
#             {"a": 1},
#             {"a": 2},
#             {"a": 3},
#             {"a": 4}
#         ]
#     }
#
#     with pytest.raises(TypeError):
#         d = MyAllowedKeysDotDict(
#             coerce_types=True,
#             **{
#                 "x": "hello",
#                 "y": {
#                     "a": [1, 2, 3, 4],
#                 },
#             }
#         )
#
# def test_allowed_keys_dot_dict_recursive_coercion_with_DictOf():
#     class MyNestedDotDict(AllowedKeysDotDict):
#         _allowed_keys = {"a"}
#         _required_keys = {"a"}
#         _key_types = {
#             "a": int,
#         }
#
#     class MyAllowedKeysDotDict(AllowedKeysDotDict):
#         _allowed_keys = {"x", "y", "z"}
#         _required_keys = {"x"}
#         _key_types = {
#             "x": str,
#             "y": DictOf(MyNestedDotDict),
#         }
#
#     d = MyAllowedKeysDotDict(
#         coerce_types=True,
#         **{
#             "x": "hello",
#             "y": {
#                 "who" : {"a": 1},
#                 "what" : {"a": 2},
#                 "when" : {"a": 3},
#             }
#         }
#     )
#     assert d == {
#         "x": "hello",
#         "y": {
#             "who" : {"a": 1},
#             "what" : {"a": 2},
#             "when" : {"a": 3},
#         }
#     }
#
#     with pytest.raises(TypeError):
#         d = MyAllowedKeysDotDict(
#             coerce_types=True,
#             **{
#                 "x": "hello",
#                 "y": {
#                     "a": [1, 2, 3, 4],
#                 },
#             }
#         )
#
#     with pytest.raises(ValueError):
#         d = MyAllowedKeysDotDict(
#             coerce_types=True,
#             **{
#                 "x": "hello",
#                 "y": {
#                     "who" : {"a": 1},
#                     "what" : {"a": "not an int"},
#                 },
#             }
#         )
#
# def test_allowed_keys_dot_dict_unicode_issues():
#     class MyLTDD(AllowedKeysDotDict):
#         _allowed_keys = {"a"}
#         _required_keys = {"a"}
#         _key_types = {
#             "a": str,
#         }
#
#     MyLTDD(**{
#         "a": "hello"
#     })
#
#     if PY2:
#         with pytest.raises(InvalidConfigValueTypeError):
#             MyLTDD(**{
#                 "a": u"hello"
#             })
#
#     class MyLTDD(AllowedKeysDotDict):
#         _allowed_keys = {"a"}
#         _required_keys = {"a"}
#         _key_types = {
#             "a": string_types,
#         }
#
#     MyLTDD(**{
#         "a": "hello"
#     })
#
#     MyLTDD(**{
#         "a": u"hello"
#     })
#
#
# def test_allowed_keys_dot_dict_multiple_types():
#     class MyLTDD(AllowedKeysDotDict):
#         _allowed_keys = set(["a"])
#         _key_types = {
#             "a": [int, float],
#         }
#
#     A = MyLTDD(**{
#         "a": 1
#     })
#
#     B = MyLTDD(**{
#         "a": 1.5
#     })
#
#     with pytest.raises(InvalidConfigValueTypeError):
#         B = MyLTDD(**{
#             "a": "string"
#         })
#
#     with pytest.raises(InvalidConfigValueTypeError):
#         B = MyLTDD(**{
#             "a": None
#         })
#
#     class MyLTDD(AllowedKeysDotDict):
#         _allowed_keys = {"a"}
#         _key_types = {
#             "a": [int, None],
#         }
#
#     A = MyLTDD(**{
#         "a": 1
#     })
#
#     B = MyLTDD(**{
#         "a": None
#     })

#
# def test_dotdict_yaml_serialization(capsys):
#     # To enable yaml serialization, we simply annotate the class as a @yaml_object
#     # Note that this annotation cannot be inherited, even though in our case we know that
#     # the subtype is still serializable
#
#     # NOTE: JPC - 20190821: We may want to reach out to the ruamel authors to inquire about
#     # adding support for searching through the entire mro (instead of only the last entry) for supported representers
#
#     # Note that we are also using the nested type coercion in this case.
#     @yaml_object(yaml)
#     class MyAllowedKeysDotDict(AllowedKeysDotDict):
#         _allowed_keys = {"a", "b"}
#         _required_keys = {"a"}
#         _key_types = {
#             "a": str,
#             "b": RequiredKeysDotDict
#         }
#
#     d = MyAllowedKeysDotDict({
#             "a": "fish",
#             "b": {
#                 "pet": "dog"
#             }
#         },
#         coerce_types=True
#     )
#
#     yaml.dump(d, sys.stdout)
#
#     assert """\
# a: fish
# b:
#   pet: dog
# """ in capsys.readouterr()

#
# def test_required_keys_dotdict():
#     class MyRequiredKeysDotDict(RequiredKeysDotDict):
#         _required_keys = {"class_name"}
#
#     with pytest.raises(MissingTopLevelConfigKeyError):
#         # required key is missing
#         d = MyRequiredKeysDotDict({"blarg": "isarealthreat"})
#
#     d = MyRequiredKeysDotDict({"class_name": "physics", "blarg": "isarealthreat"})
#     assert d["class_name"] == d.class_name
#     assert d["blarg"] == "isarealthreat"
#
#     # Note that since there is not a concept of allowed_keys, we do not raise attributeerror here, picking
#     # up dictionary semantics instead
#     assert d.doesnotexist is None
#
# def test_OrderedKeysDotDict_subclass():
#
#     class MyOKDD(OrderedKeysDotDict):
#         _key_order = ["A", "B", "C"]
#         _key_types = {
#             "A" : string_types,
#             "B" : int,
#         }
#
#         # NOTE: This pattern is kinda awkward.
#         # It would be nice to ONLY specify _key_order
#         # Instead, we need to add these two lines at the end of every OrderedKeysDotDict class definition
#         # ... There's probably a way to do this with decorators...
#         _allowed_keys = set(_key_order)
#         _required_keys = set(_key_order)
#
#     MyOKDD(**{
#         "A" : "A",
#         "B" : 10,
#         "C" : "C",
#     })
#
#     #OrderedKeysDotDicts can parse from tuples
#     MyOKDD("a", 10, "c")
#
#     #OrderedKeysDotDicts coerce to _key_types by default
#     assert MyOKDD("10", "10", "20") == {
#         "A" : "10",
#         "B" : 10, # <- Not a string anymore!
#         "C" : "20",
#     }
#
#     with pytest.raises(ValueError):
#         assert MyOKDD("a", "10.5", 20)
#
#     #OrderedKeysDotDicts raise an IndexError if args don't line up with keys
#     with pytest.raises(IndexError):
#         MyOKDD("a")
#
#     with pytest.raises(IndexError):
#         MyOKDD("a", 10, "c", "d")
#

# def test_OrderedKeysDotDict__recursively_get_key_length():
#
#     class MyOKDD(OrderedKeysDotDict):
#         _key_order = ["A", "B", "C"]
#         _key_types = {
#             "A" : string_types,
#             "B" : int,
#         }
#         _allowed_keys = set(_key_order)
#         _required_keys = set(_key_order)
#
#     assert MyOKDD._recursively_get_key_length() == 3
#     assert DataAssetIdentifier._recursively_get_key_length() == 3
#     assert ValidationResultIdentifier._recursively_get_key_length() == 5
#
