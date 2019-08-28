# NOTE: JPC - 20190828 This class system's use of redefined allowed and required keys confuses inheritance since
# a child class may not even have the same properties as its parent. I think it needs a major rework.


# from six import string_types
#
# from great_expectations.datasource import Datasource
# from great_expectations.types import (
#     # FIXME: Merge with branch containing DictOf
#     # DictOf,
#     AllowedKeysDotDict,
#     ClassConfig
# )
#
#
# class GeneratorConfig(AllowedKeysDotDict):
#     _allowed_keys = {
#         "class_name",
#         "module_name",
#         "datasource"
#     }
#
#     _required_keys = {
#         "class_name"
#     }
#
#     _key_types = {
#         "datasource": Datasource
#     }
#
#
# class DatasourceConfig(AllowedKeysDotDict):
#     _allowed_keys = {
#         "generators",
#         "data_asset_type",
#         "class_name",
#         "module_name"
#     }
#
#     _required_keys = {
#         "generators",
#         "data_asset_type",
#         "class_name",
#     }
#
#     _key_types = {
#         # FIXME: Merge with branch containing DictOf
#         # "generators": DictOf(GeneratorConfig),
#         "data_asset_type": ClassConfig,
#         "class_name": string_types,
#         "module_name": string_types
#     }
#
#
# class DeprecatedDatasourceConfig(AllowedKeysDotDict):
#     _allowed_keys = {
#         "generators",
#         "data_asset_type",
#         "type"
#     }
#
#     _required_keys = {
#         "generators",
#         "data_asset_type",
#         "type",
#     }
#
#     _key_types = {
#         # FIXME: Merge with branch containing DictOf
#         # "generators": DictOf(GeneratorConfig),
#         "data_asset_type": ClassConfig,
#         "type": string_types
#     }
