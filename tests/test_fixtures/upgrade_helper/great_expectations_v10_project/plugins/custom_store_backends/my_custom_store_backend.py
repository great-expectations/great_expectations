from great_expectations.data_context.store.tuple_store_backend import TupleStoreBackend


class MyCustomStoreBackend(TupleStoreBackend):
    def __init__(
        self,
        filepath_template=None,
        filepath_prefix=None,
        filepath_suffix=None,
        forbidden_substrings=None,
        platform_specific_separator=True,
        fixed_length_key=False,
        suppress_store_backend_id=False,
        store_name=None,
    ):
        super().__init__(
            filepath_template=filepath_template,
            filepath_prefix=filepath_prefix,
            filepath_suffix=filepath_suffix,
            forbidden_substrings=forbidden_substrings,
            platform_specific_separator=platform_specific_separator,
            fixed_length_key=fixed_length_key,
            suppress_store_backend_id=suppress_store_backend_id,
            store_name=store_name,
        )

    def _get(self, key):
        pass

    def _set(self, key, value, **kwargs):
        pass

    def _move(self, source_key, dest_key, **kwargs):
        pass

    def list_keys(self, prefix=()):
        pass

    def remove_key(self, key):
        pass
