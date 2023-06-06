from __future__ import annotations

import logging
from abc import ABCMeta
from typing import TYPE_CHECKING, Any, List

from great_expectations.core.id_dict import BatchSpec
from great_expectations.exceptions import InvalidBatchIdError, InvalidBatchSpecError
from great_expectations.types.base import SerializableDotDict

if TYPE_CHECKING:
    from great_expectations.alias_types import JSONValues, PathStr

logger = logging.getLogger(__name__)


# TODO: <Alex>This module needs to be cleaned up.
#  We have Batch used for the legacy design, and we also need Batch for the new design.
#  However, right now, the Batch from the legacy design is imported into execution engines of the new design.
#  As a result, we have multiple, inconsistent versions of BatchMarkers, extending legacy/new classes.</Alex>
# TODO: <Alex>See also "great_expectations/core/batch.py".</Alex>
# TODO: <Alex>The following class is part of the new design.</Alex>
class BatchMarkers(BatchSpec):
    """A BatchMarkers is a special type of BatchSpec (so that it has a batch_fingerprint) but it generally does
    NOT require specific keys and instead captures information about the OUTPUT of a datasource's fetch
    process, such as the timestamp at which a query was executed."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        if "ge_load_time" not in self:
            raise InvalidBatchIdError("BatchMarkers requires a ge_load_time")

    @property
    def ge_load_time(self):
        return self.get("ge_load_time")


class PandasBatchSpec(SerializableDotDict, BatchSpec, metaclass=ABCMeta):
    @property
    def reader_method(self) -> str:
        return self["reader_method"]

    @property
    def reader_options(self) -> dict:
        return self.get("reader_options", {})

    def to_json_dict(self) -> dict[str, JSONValues]:
        from great_expectations.datasource.fluent.pandas_datasource import (
            _EXCLUDE_TYPES_FROM_JSON,
        )

        json_dict: dict[str, JSONValues] = dict()
        json_dict["reader_method"] = self.reader_method
        json_dict["reader_options"] = {
            reader_option_name: reader_option
            for reader_option_name, reader_option in self.reader_options.items()
            if not isinstance(reader_option, tuple(_EXCLUDE_TYPES_FROM_JSON))
        }
        return json_dict


class PathBatchSpec(BatchSpec, metaclass=ABCMeta):
    def __init__(
        self,
        *args,
        path: PathStr = None,  # type: ignore[assignment] # error raised if not provided
        reader_options: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        if path:
            kwargs["path"] = str(path)
        if reader_options:
            kwargs["reader_options"] = reader_options
        super().__init__(*args, **kwargs)
        if "path" not in self:
            raise InvalidBatchSpecError("PathBatchSpec requires a path element")

    @property
    def path(self) -> str:
        return self.get("path")  # type: ignore[return-value]

    @property
    def reader_method(self) -> str:
        return self.get("reader_method")  # type: ignore[return-value]

    @property
    def reader_options(self) -> dict:
        return self.get("reader_options") or {}


class S3BatchSpec(PathBatchSpec):
    pass


class AzureBatchSpec(PathBatchSpec):
    pass


class GCSBatchSpec(PathBatchSpec):
    pass


class SqlAlchemyDatasourceBatchSpec(BatchSpec, metaclass=ABCMeta):
    """This is an abstract class and should not be instantiated. It's relevant for testing whether
    a subclass is allowed
    """

    @property
    def limit(self):
        return self.get("limit")

    @property
    def schema(self):
        return self.get("schema")


class RuntimeDataBatchSpec(BatchSpec):
    _id_ignore_keys = set("batch_data")

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        if self.batch_data is None:
            raise InvalidBatchSpecError(
                "RuntimeDataBatchSpec batch_data cannot be None"
            )

    @property
    def batch_data(self):
        return self.get("batch_data")

    @batch_data.setter
    def batch_data(self, batch_data) -> None:
        self["batch_data"] = batch_data


class RuntimeQueryBatchSpec(BatchSpec):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        if self.query is None:
            raise InvalidBatchSpecError("RuntimeQueryBatchSpec query cannot be None")

    @property
    def query(self):
        return self.get("query")

    @query.setter
    def query(self, query) -> None:
        self["query"] = query


class GlueDataCatalogBatchSpec(BatchSpec):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        if "database_name" not in self:
            raise InvalidBatchSpecError(
                "GlueDataCatalogBatchSpec requires a database_name"
            )
        if "table_name" not in self:
            raise InvalidBatchSpecError(
                "GlueDataCatalogBatchSpec requires a table_name"
            )

    @property
    def reader_method(self) -> str:
        return "table"

    @property
    def database_name(self) -> str:
        return self["database_name"]

    @property
    def table_name(self) -> str:
        return self["table_name"]

    @property
    def path(self) -> str:
        return f"{self.database_name}.{self.table_name}"

    @property
    def reader_options(self) -> dict:
        return self.get("reader_options", {})

    @property
    def partitions(self) -> List[str]:
        return self.get("partitions", [])
