import os

from great_expectations.data_context.store.store_backend import StoreBackend


class InlineStoreBackend(StoreBackend):
    """
    DOCSTRING GOES HERE
    """

    def __init__(
        self,
        project_config_path: str,
        runtime_environment=None,
        fixed_length_key=False,
        suppress_store_backend_id=False,
        manually_initialize_store_backend_id: str = "",
        store_name=None,
    ) -> None:
        if not os.path.exists(project_config_path):
            raise ValueError("'project_config_path' must be an existing file")

        self._project_config_path = project_config_path

        super().__init__(
            fixed_length_key=fixed_length_key,
            suppress_store_backend_id=suppress_store_backend_id,
            manually_initialize_store_backend_id=manually_initialize_store_backend_id,
            store_name=store_name,
        )

        self._config = {
            "runtime_environment": runtime_environment,
            "fixed_length_key": fixed_length_key,
            "suppress_store_backend_id": suppress_store_backend_id,
            "manually_initialize_store_backend_id": manually_initialize_store_backend_id,
            "store_name": store_name,
            "module_name": self.__class__.__module__,
            "class_name": self.__class__.__name__,
        }

    def _get(self, key) -> None:
        raise NotImplementedError

    def _set(self, key, value, **kwargs) -> None:
        raise NotImplementedError

    def _move(self, source_key, dest_key, **kwargs) -> None:
        raise NotImplementedError

    def list_keys(self, prefix=()) -> None:
        raise NotImplementedError

    def remove_key(self, key) -> None:
        raise NotImplementedError

    def _has_key(self, key) -> None:
        raise NotImplementedError

    @property
    def config(self) -> dict:
        return self._config
