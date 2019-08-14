# import json
# import os
# import io

# from ..util import safe_mmkdir

# class Store(object):
#     """A simple key-value store that supports getting and setting.

#     Stores also support the concept of serialization.

#     See tests/data_context/test_store.py for examples.
#     """



# class InMemoryStore(Store):
#     """Uses an in-memory dictionary as a store.
#     """

#     def _setup(self):
#         self.store = {}

#     def _get(self, key):
#         return self.store[key]

#     def _set(self, key, value):
#         self.store[key] = value


# class FilesystemStore(Store):
#     """Uses a local filepath as a store.
#     """

#     def _setup(self):
#         safe_mmkdir(str(os.path.dirname(self.config.base_directory)))

#     def _get(self, key):
#         with open(os.path.join(self.config.base_directory, key)) as infile:
#             return infile.read()

#     def _set(self, key, value):
#         filename = os.path.join(self.config.base_directory, key)
#         safe_mmkdir(str(os.path.split(filename)[0]))
#         with open(filename, "w") as outfile:
#             outfile.write(value)

#     def _get_namespaced_key(self, key):
#         filepath = self.data_context._get_normalized_data_asset_name_filepath(
#             key.normalized_data_asset_name,
#             key.expectation_suite_name,
#             base_path=os.path.join(
#                 self.data_context.root_directory,
#                 self.config.base_directory,
#                 key.run_id
#             ),
#             file_extension=self.config.file_extension
#         )
#         return filepath


# # class S3Store(DataContextAwareStore):
# #     """Uses an S3 bucket+prefix as a store
# #     """

# #     def _get(self, key):
# #         raise NotImplementedError

# #     def _set(self, key, value):
# #         raise NotImplementedError
