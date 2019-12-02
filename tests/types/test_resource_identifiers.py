# import logging
#
# from great_expectations.data_context.types.resource_identifiers import (
#     DataAssetIdentifier,
#     ExpectationSuiteIdentifier,
#     ValidationResultIdentifier,
# )
#
# logger = logging.getLogger(__name__)

# def test_DataAssetIdentifier():
#
#     DataAssetIdentifier(**{
#         "datasource" : "A",
#         "generator" : "B",
#         "generator_asset" : "C",
#     })
#
#     my_id = DataAssetIdentifier("A", "B", "C")
#     assert my_id.to_string() == "DataAssetIdentifier.A.B.C"
#
# # NOTE: The following tests are good tests of OrderedDotDict's ability to handle abbreviated input to __init__ when nested
# def test_ValidationResultIdentifier__init__totally_nested():
#     my_id = ValidationResultIdentifier(
#         coerce_types=True,
#         **{
#             "expectation_suite_identifier" : {
#                 "data_asset_name" : {
#                     "datasource" : "a",
#                     "generator" : "b",
#                     "generator_asset" : "c",
#                 },
#                 "expectation_suite_name": "failure",
#             },
#             "run_id" : "testing-12345",
#         }
#     )
#
#     assert my_id.to_string() == "ValidationResultIdentifier.a.b.c.failure.testing-12345"
#
#     # FIXME : This should throw an error. coerce_types is too permissive.
#     # my_id = ValidationResultIdentifier(
#     #     coerce_types=True,
#     #     **{
#     #         "expectation_suite_identifier" : {
#     #             "data_asset_name" : {
#     #                 "datasource" : "a",
#     #                 "generator" : "b",
#     #                 "generator_asset" : "c",
#     #             },
#     #             "expectation_suite_name": "failure",
#     #         },
#     #         "run_id" : {
#     #             "bogus_key_A" : "I should not be",
#     #             "bogus_key_A" : "nested",
#     #         }
#     #     }
#     # )
#
#
# def test_ValidationResultIdentifier__init__mostly_nested():
#     ValidationResultIdentifier(
#         coerce_types=True,
#         **{
#             "expectation_suite_identifier" : {
#                 "data_asset_name" : ("a", "b", "c"),
#                 "expectation_suite_name": "warning",
#             },
#             "run_id" : "testing-12345",
#         }
#     )
#
# def test_ValidationResultIdentifier__init__mostly_nested_with_typed_child():
#     ValidationResultIdentifier(
#         coerce_types=True,
#         **{
#             "expectation_suite_identifier" : {
#                 "data_asset_name" : DataAssetIdentifier("a", "b", "c"),
#                 "expectation_suite_name": "quarantine",
#             },
#             "run_id" : "testing-12345",
#         }
#     )
#
# def test_ValidationResultIdentifier__init__partially_flat():
#     ValidationResultIdentifier(
#         coerce_types=True,
#         **{
#             "expectation_suite_identifier" : {
#                 "data_asset_name" : ("a", "b", "c"),
#                 "expectation_suite_name": "hello",
#             },
#             "run_id" : "testing-12345",
#         }
#     )
#
# # NOTE: Abe 2019/08/24 : This style of instantiation isn't currently supported. Might want to enable it at some point in the future
# # # def test_ValidationResultIdentifier__init__mostly_flat():
# #     ValidationResultIdentifier(
# #         coerce_types=True,
# #         **{
# #             "expectation_suite_identifier" : (("a", "b", "c"), "hello", "testing"),
# #             "run_id" : ("testing", 12345)
# #         }
# #     )
#
# # NOTE: Abe 2019/08/24 : This style of instantiation isn't currently supported. Might want to enable it at some point in the future
# # def test_ValidationResultIdentifier__init__very_flat():
# #     ValidationResultIdentifier(
# #         (("a", "b", "c"), "hello", "testing"),("testing", 12345),
# #         coerce_types=True,
# #     )
#
# def test_ValidationResultIdentifier__init__nested_except_the_top_layer():
#     ValidationResultIdentifier(
#         {
#             "data_asset_name" : {
#                 "datasource" : "a",
#                 "generator" : "b",
#                 "generator_asset" : "c",
#             },
#             "expectation_suite_name": "hello",
#         },
#         "testing-12345",
#         coerce_types=True
#     )
#
# def test_ValidationResultIdentifier__init__entirely_flat():
#     ValidationResultIdentifier(
#         "a", "b", "c", "warning", "testing-12345",
#         coerce_types=True,
#     )
#
# def test_OrderedKeysDotDict__zip_keys_and_args_to_dict():
#     assert ValidationResultIdentifier._zip_keys_and_args_to_dict(
#         ["a", "b", "c", "warning", "testing-12345"],
#     ) == {
#         "expectation_suite_identifier" : ["a", "b", "c", "warning"],
#         "run_id" : "testing-12345"
#     }
#
#     assert ExpectationSuiteIdentifier._zip_keys_and_args_to_dict(
#         ["a", "b", "c", "warning"]
#     ) == {
#         "data_asset_name" : ["a", "b", "c"],
#         "expectation_suite_name" : "warning",
#     }
#
#     assert ValidationResultIdentifier._zip_keys_and_args_to_dict(
#         ["a", "b", "c", "hello", "testing-12345"]
#     ) == {
#         "expectation_suite_identifier" : ["a", "b", "c", "hello",],
#         "run_id" : "testing-12345"
#     }
#
# # TODO: Put this with the other tests for utils.
# def test_parse_string_to_data_context_resource_identifier():
#
#     assert parse_string_to_data_context_resource_identifier("DataAssetIdentifier.A.B.C") == DataAssetIdentifier("A", "B", "C")
#
#     assert parse_string_to_data_context_resource_identifier(
#         "ValidationResultIdentifier.a.b.c.hello.testing-12345"
#     ) == ValidationResultIdentifier(
#         coerce_types=True,
#         **{
#             "expectation_suite_identifier" : {
#                 "data_asset_name" : {
#                     "datasource" : "a",
#                     "generator" : "b",
#                     "generator_asset" : "c",
#                 },
#                 "expectation_suite_name": "hello",
#             },
#             "run_id" : "testing-12345"
#         }
#     )
#
#
# def test_resource_identifier_to_string():
#
#     assert ValidationResultIdentifier(
#         coerce_types=True,
#         **{
#             "expectation_suite_identifier" : {
#                 "data_asset_name" : {
#                     "datasource" : "a",
#                     "generator" : "b",
#                     "generator_asset" : "c",
#                 },
#                 "expectation_suite_name": "hello",
#             },
#             "run_id" : "testing-12345",
#         }
#     ).to_string() == "ValidationResultIdentifier.a.b.c.hello.testing-12345"
