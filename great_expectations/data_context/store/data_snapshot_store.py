# NOTE: Deprecated. Retain until DataSnapshotStore is implemented
# class NamespacedReadWriteStoreConfig(ReadWriteStoreConfig):
#     _allowed_keys = set({
#         "serialization_type",
#         "resource_identifier_class_name",
#         "store_backend",
#     })
#     _required_keys = set({
#         "resource_identifier_class_name",
#         "store_backend",
#     })

# class DataSnapshotStore(WriteOnlyStore):
    
#     config_class = NamespacedReadWriteStoreConfig

#     def __init__(self, config, root_directory):
#         # super(NamespacedReadWriteStore, self).__init__(config, root_directory)

#         # TODO: This method was copied and modified from the base class. 
#         # We need to refactor later to inherit sensibly.
#         assert hasattr(self, 'config_class')

#         assert isinstance(config, self.config_class)
#         self.config = config

#         self.root_directory = root_directory

#         # NOTE: hm. This is tricky.
#         # At this point, we need to add some keys to the store_backend config.
#         # The config from THIS class should be typed by this point.
#         # But if we insist that it's recursively typed, it will have failed before arriving at this point.
#         if self.config["store_backend"]["class_name"] == "FilesystemStoreBackend":
#             self.config["store_backend"]["key_length"] = self.resource_identifier_class._recursively_get_key_length()#+1 #Only add one if we prepend the identifier type
#             self.store_backend = self._configure_store_backend(self.config["store_backend"])
#             self.store_backend.verify_that_key_to_filepath_operation_is_reversible()

#         else:
#             self.store_backend = self._configure_store_backend(self.config["store_backend"])
    

#         self._setup()


#     def _get(self, key):
#         key_tuple = self._convert_resource_identifier_to_tuple(key)
#         return self.store_backend.get(key_tuple)

#     def _set(self, key, serialized_value):
#         key_tuple = self._convert_resource_identifier_to_tuple(key)
#         return self.store_backend.set(key_tuple, serialized_value)

#     def list_keys(self):
#         return [self._convert_tuple_to_resource_identifier(key) for key in self.store_backend.list_keys()]

#     def _convert_resource_identifier_to_tuple(self, key):
#         # TODO : Optionally prepend a source_id (the frontend Store name) to the tuple.

#         # TODO : Optionally prepend a resource_identifier_type to the tuple.
#         # list_ = [self.config.resource_identifier_class_name]

#         list_ = []
#         list_ += self._convert_resource_identifier_to_list(key)

#         return tuple(list_)

#     def _convert_resource_identifier_to_list(self, key):
#         # The logic in this function is recursive, so it can't return a tuple

#         list_ = []
#         #Fetch keys in _key_order to guarantee tuple ordering in both python 2 and 3
#         for key_name in key._key_order:
#             key_element = key[key_name]
#             if isinstance( key_element, DataContextResourceIdentifier ):
#                 list_ += self._convert_resource_identifier_to_list(key_element)
#             else:
#                 list_.append(key_element)

#         return list_

#     def _convert_tuple_to_resource_identifier(self, tuple_):
#         new_identifier = self.resource_identifier_class(*tuple_)#[1:]) #Only truncate one if we prepended the identifier type
#         return new_identifier

#     @property
#     def resource_identifier_class(self):
#         module = importlib.import_module("great_expectations.data_context.types.resource_identifiers")
#         class_ = getattr(module, self.config.resource_identifier_class_name)
#         return class_

#     def _validate_key(self, key):
#         if not isinstance(key, self.resource_identifier_class):
#             raise TypeError("key: {!r} must be a DataContextResourceIdentifier, not {!r}".format(
#                 key,
#                 type(key),
#             ))
