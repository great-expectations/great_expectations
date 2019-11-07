# from six import string_types
# from ruamel.yaml import YAML, yaml_object
#
#
# # from great_expectations.types import AllowedKeysDotDict
#
# yaml = YAML()

#
# class Config(AllowedKeysDotDict):
#     pass
from marshmallow import Schema, fields


class ClassConfig(object):
    """Defines information sufficient to identify a class to be (dynamically) loaded for a DataContext."""
    def __init__(self, class_name, module_name=None):
        self._class_name = class_name
        self._module_name = module_name

    @property
    def class_name(self):
        return self._class_name

    @property
    def module_name(self):
        return self._module_name


class ClassConfigSchema(Schema):
    class_name = fields.Str()
    module_name = fields.Str(allow_none=True)

#
#     #
#     # _allowed_keys = {
#     #     "module_name",
#     #     "class_name"
#     # }
#     # _required_keys = {
#     #     "class_name"
#     # }
#     # _key_types = {
#     #     "module_name": string_types,
#     #     "class_name": string_types
#     # }
