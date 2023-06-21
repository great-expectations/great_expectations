from __future__ import annotations

import datetime
import json
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Type,
    TypedDict,
    Union,
    overload,
)

import pandas as pd

from great_expectations.compatibility import pyspark
from great_expectations.core._docs_decorators import deprecated_argument, public_api
from great_expectations.core.id_dict import BatchKwargs, BatchSpec, IDDict
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.exceptions import InvalidBatchIdError
from great_expectations.types import DictDot, SerializableDictDot, safe_deep_copy
from great_expectations.util import deep_filter_properties_iterable, load_class

if TYPE_CHECKING:
    from typing_extensions import NotRequired, TypeAlias

    from great_expectations.alias_types import JSONValues
    from great_expectations.datasource.data_connector.batch_filter import BatchSlice
    from great_expectations.datasource.fluent.interfaces import (
        Batch as FluentBatch,
    )
    from great_expectations.datasource.fluent.interfaces import (
        BatchRequest as FluentBatchRequest,
    )
    from great_expectations.datasource.fluent.interfaces import (
        BatchRequestOptions,
    )
    from great_expectations.validator.metrics_calculator import MetricsCalculator


logger = logging.getLogger(__name__)


class BlockConfigBatchRequestTypedDict(TypedDict):
    datasource_name: str
    data_connector_name: str
    data_asset_name: str
    runtime_parameters: NotRequired[dict]
    batch_identifiers: NotRequired[dict]
    batch_spec_passthrough: NotRequired[dict]
    data_connector_query: NotRequired[dict]
    limit: NotRequired[BatchSlice]


def _get_fluent_batch_request_class() -> Type[FluentBatchRequest]:
    """Using this function helps work around circular import dependncies."""
    module_name = "great_expectations.datasource.fluent.batch_request"
    class_name = "BatchRequest"
    return load_class(class_name=class_name, module_name=module_name)


def _get_fluent_batch_class() -> Type[FluentBatch]:
    """Using this function helps work around circular import dependncies."""
    module_name = "great_expectations.datasource.fluent.interfaces"
    class_name = "Batch"
    return load_class(class_name=class_name, module_name=module_name)


def _get_metrics_calculator_class() -> Type[MetricsCalculator]:
    """Using this function helps work around circular import dependncies."""
    module_name = "great_expectations.validator.metrics_calculator"
    class_name = "MetricsCalculator"
    return load_class(class_name=class_name, module_name=module_name)


@public_api
class BatchDefinition(SerializableDictDot):
    """Precisely identifies a set of data from a data source.

    More concretely, a BatchDefinition includes all the information required to precisely
    identify a set of data from the external data source that should be
    translated into a Batch. One or more BatchDefinitions should always be
    *returned* from the Datasource, as a result of processing the Batch Request.

    ---Documentation---
            - https://docs.greatexpectations.io/docs/terms/batch/#batches-and-batch-requests-design-motivation

    Args:
        datasource_name: name of the Datasource used to connect to the data
        data_connector_name: name of the DataConnector used to connect to the data
        data_asset_name: name of the DataAsset used to connect to the data
        batch_identifiers: key-value pairs that the DataConnector
            will use to obtain a specific set of data
        batch_spec_passthrough: a dictionary of additional parameters that
            the ExecutionEngine will use to obtain a specific set of data

    Returns:
        BatchDefinition
    """

    def __init__(  # noqa: PLR0913
        self,
        datasource_name: str,
        data_connector_name: str,
        data_asset_name: str,
        batch_identifiers: IDDict,
        batch_spec_passthrough: dict | None = None,
    ) -> None:
        self._validate_batch_definition(
            datasource_name=datasource_name,
            data_connector_name=data_connector_name,
            data_asset_name=data_asset_name,
            batch_identifiers=batch_identifiers,
        )

        assert type(batch_identifiers) == IDDict

        self._datasource_name = datasource_name
        self._data_connector_name = data_connector_name
        self._data_asset_name = data_asset_name
        self._batch_identifiers = batch_identifiers
        self._batch_spec_passthrough = batch_spec_passthrough

    @public_api
    def to_json_dict(self) -> dict[str, JSONValues]:
        """Returns a JSON-serializable dict representation of this BatchDefinition.

        Returns:
            A JSON-serializable dict representation of this BatchDefinition.
        """
        fields_dict: dict = {
            "datasource_name": self._datasource_name,
            "data_connector_name": self._data_connector_name,
            "data_asset_name": self._data_asset_name,
            "batch_identifiers": self._batch_identifiers,
        }
        if self._batch_spec_passthrough:
            fields_dict["batch_spec_passthrough"] = self._batch_spec_passthrough

        return convert_to_json_serializable(data=fields_dict)

    def __repr__(self) -> str:
        doc_fields_dict: dict = {
            "datasource_name": self._datasource_name,
            "data_connector_name": self._data_connector_name,
            "data_asset_name": self._data_asset_name,
            "batch_identifiers": self._batch_identifiers,
        }
        return str(doc_fields_dict)

    @staticmethod
    def _validate_batch_definition(
        datasource_name: str,
        data_connector_name: str,
        data_asset_name: str,
        batch_identifiers: IDDict,
    ) -> None:
        if datasource_name is None:
            raise ValueError("A valid datasource must be specified.")
        if datasource_name and not isinstance(datasource_name, str):
            raise TypeError(
                f"""The type of an datasource name must be a string (Python "str").  The type given is
"{str(type(datasource_name))}", which is illegal.
            """
            )
        if data_connector_name is None:
            raise ValueError("A valid data_connector must be specified.")
        if data_connector_name and not isinstance(data_connector_name, str):
            raise TypeError(
                f"""The type of a data_connector name must be a string (Python "str").  The type given is
"{str(type(data_connector_name))}", which is illegal.
                """
            )
        if data_asset_name is None:
            raise ValueError("A valid data_asset_name must be specified.")
        if data_asset_name and not isinstance(data_asset_name, str):
            raise TypeError(
                f"""The type of a data_asset name must be a string (Python "str").  The type given is
"{str(type(data_asset_name))}", which is illegal.
                """
            )
        if batch_identifiers and not isinstance(batch_identifiers, IDDict):
            raise TypeError(
                f"""The type of batch_identifiers must be an IDDict object.  The type given is \
"{str(type(batch_identifiers))}", which is illegal.
"""
            )

    @property
    def datasource_name(self) -> str:
        return self._datasource_name

    @property
    def data_connector_name(self) -> str:
        return self._data_connector_name

    @property
    def data_asset_name(self) -> str:
        return self._data_asset_name

    @property
    def batch_identifiers(self) -> IDDict:
        return self._batch_identifiers

    @property
    def batch_spec_passthrough(self) -> dict | None:
        return self._batch_spec_passthrough

    @batch_spec_passthrough.setter
    def batch_spec_passthrough(self, batch_spec_passthrough: dict | None) -> None:
        self._batch_spec_passthrough = batch_spec_passthrough

    @property
    def id(self) -> str:
        return IDDict(self.to_json_dict()).to_id()

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            # Delegate comparison to the other instance's __eq__.
            return NotImplemented
        return self.id == other.id

    def __str__(self):
        return json.dumps(self.to_json_dict(), indent=2)

    def __hash__(self) -> int:
        """Overrides the default implementation"""
        _result_hash: int = hash(self.id)
        return _result_hash


class BatchRequestBase(SerializableDictDot):
    """
    This class is for internal inter-object protocol purposes only.
    As such, it contains all attributes of a batch_request, but does not validate them.
    See the BatchRequest class, which extends BatchRequestBase and validates the attributes.

    BatchRequestBase is used for the internal protocol purposes exclusively, not part of API for the developer users.

    Previously, the very same BatchRequest was used for both the internal protocol purposes and as part of the API
    exposed to developers.  However, while convenient for internal data interchange, using the same BatchRequest class
    as arguments to the externally-exported DataContext.get_batch(), DataContext.get_batch_list(), and
    DataContext.get_validator() API calls for obtaining batches and/or validators was insufficiently expressive to
    fulfill the needs of both. In the user-accessible API, BatchRequest, must enforce that all members of the triple,
    consisting of data_source_name, data_connector_name, and data_asset_name, are not NULL.  Whereas for the internal
    protocol, BatchRequest is used as a flexible bag of attributes, in which any fields are allowed to be NULL.  Hence,
    now, BatchRequestBase is dedicated for the use as the bag oof attributes for the internal protocol use, whereby NULL
    values are allowed as per the internal needs.  The BatchRequest class extends BatchRequestBase and adds to it strong
    validation (described above plus additional attribute validation) so as to formally validate user specified fields.
    """

    def __init__(  # noqa: PLR0913
        self,
        datasource_name: str,
        data_connector_name: str,
        data_asset_name: str,
        data_connector_query: dict | None = None,
        limit: int | None = None,
        runtime_parameters: dict | None = None,
        batch_identifiers: dict | None = None,
        batch_spec_passthrough: dict | None = None,
    ) -> None:
        self._datasource_name = datasource_name
        self._data_connector_name = data_connector_name
        self._data_asset_name = data_asset_name
        self._data_connector_query = data_connector_query
        self._limit = limit

        self._runtime_parameters = runtime_parameters
        self._batch_identifiers = batch_identifiers
        self._batch_spec_passthrough = batch_spec_passthrough

    @property
    def datasource_name(self) -> str:
        return self._datasource_name

    @datasource_name.setter
    def datasource_name(self, value: str) -> None:
        self._datasource_name = value

    @property
    def data_connector_name(self) -> str:
        return self._data_connector_name

    @data_connector_name.setter
    def data_connector_name(self, value: str) -> None:
        self._data_connector_name = value

    @property
    def data_asset_name(self) -> str:
        return self._data_asset_name

    @data_asset_name.setter
    def data_asset_name(self, data_asset_name) -> None:
        self._data_asset_name = data_asset_name

    @property
    def data_connector_query(self) -> dict | None:
        return self._data_connector_query

    @data_connector_query.setter
    def data_connector_query(self, value: dict) -> None:
        self._data_connector_query = value

    @property
    def limit(self) -> int | None:
        return self._limit

    @limit.setter
    def limit(self, value: int) -> None:
        self._limit = value

    @property
    def runtime_parameters(self) -> dict | None:
        return self._runtime_parameters

    @runtime_parameters.setter
    def runtime_parameters(self, value: dict) -> None:
        self._runtime_parameters = value

    @property
    def batch_identifiers(self) -> dict | None:
        return self._batch_identifiers

    @batch_identifiers.setter
    def batch_identifiers(self, value: dict) -> None:
        self._batch_identifiers = value

    @property
    def batch_spec_passthrough(self) -> dict | None:
        return self._batch_spec_passthrough

    @batch_spec_passthrough.setter
    def batch_spec_passthrough(self, value: dict) -> None:
        self._batch_spec_passthrough = value

    @property
    def id(self) -> str:
        return IDDict(self.to_json_dict()).to_id()

    def to_dict(self) -> BlockConfigBatchRequestTypedDict:  # type: ignore[override] # TypedDict is more specific dict type
        return standardize_batch_request_display_ordering(
            batch_request=super().to_dict()  # type: ignore[arg-type] # TypedDict is more specific dict type
        )

    # While this class is private, it is inherited from and this method is part
    # of the public api on the child.
    @public_api
    def to_json_dict(self) -> dict[str, JSONValues]:
        """Returns a JSON-serializable dict representation of this BatchRequestBase.

        Returns:
            A JSON-serializable dict representation of this BatchRequestBase.
        """
        # TODO: <Alex>2/4/2022</Alex>
        # This implementation of "SerializableDictDot.to_json_dict() occurs frequently and should ideally serve as the
        # reference implementation in the "SerializableDictDot" class itself.  However, the circular import dependencies,
        # due to the location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules
        # make this refactoring infeasible at the present time.

        # if batch_data appears in BatchRequest, temporarily replace it with
        # str placeholder before calling convert_to_json_serializable so that
        # batch_data is not serialized
        serializeable_dict: dict
        if batch_request_contains_batch_data(batch_request=self):
            if self.runtime_parameters is None:
                raise ValueError(
                    "BatchRequestBase missing runtime_parameters during serialization"
                )
            batch_data: BatchRequestBase | dict = self.runtime_parameters["batch_data"]
            self.runtime_parameters["batch_data"] = str(type(batch_data))

            serializeable_dict = convert_to_json_serializable(data=self.to_dict())
            # after getting serializable_dict, restore original batch_data
            self.runtime_parameters["batch_data"] = batch_data
        else:
            serializeable_dict = convert_to_json_serializable(data=self.to_dict())

        return serializeable_dict

    def __deepcopy__(self, memo):
        cls = self.__class__
        result = cls.__new__(cls)

        memo[id(self)] = result

        for key, value in self.to_raw_dict().items():
            value_copy = safe_deep_copy(data=value, memo=memo)
            setattr(result, key, value_copy)

        return result

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            # Delegate comparison to the other instance's __eq__.
            return NotImplemented

        return self.id == other.id

    def __repr__(self) -> str:
        """
        # TODO: <Alex>2/4/2022</Alex>
        This implementation of a custom "__repr__()" occurs frequently and should ideally serve as the reference
        implementation in the "SerializableDictDot" class.  However, the circular import dependencies, due to the
        location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules make this
        refactoring infeasible at the present time.
        """
        json_dict: dict = self.to_json_dict()
        deep_filter_properties_iterable(
            properties=json_dict,
            inplace=True,
        )
        return json.dumps(json_dict, indent=2)

    def __str__(self) -> str:
        """
        # TODO: <Alex>2/4/2022</Alex>
        This implementation of a custom "__str__()" occurs frequently and should ideally serve as the reference
        implementation in the "SerializableDictDot" class.  However, the circular import dependencies, due to the
        location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules make this
        refactoring infeasible at the present time.
        """
        return self.__repr__()

    @staticmethod
    def _validate_init_parameters(
        datasource_name: str,
        data_connector_name: str,
        data_asset_name: str,
        data_connector_query: dict | None = None,
        limit: int | None = None,
    ) -> None:
        # TODO test and check all logic in this validator!
        if not (datasource_name and isinstance(datasource_name, str)):
            raise TypeError(
                f"""The type of an datasource name must be a string (Python "str").  The type given is
"{str(type(datasource_name))}", which is illegal.
            """
            )
        if not (data_connector_name and isinstance(data_connector_name, str)):
            raise TypeError(
                f"""The type of data_connector name must be a string (Python "str").  The type given is
"{str(type(data_connector_name))}", which is illegal.
                """
            )
        if not (data_asset_name and isinstance(data_asset_name, str)):
            raise TypeError(
                f"""The type of data_asset name must be a string (Python "str").  The type given is
        "{str(type(data_asset_name))}", which is illegal.
                        """
            )
        # TODO Abe 20201015: Switch this to DataConnectorQuery.
        if data_connector_query and not isinstance(data_connector_query, dict):
            raise TypeError(
                f"""The type of data_connector_query must be a dict object.  The type given is
"{str(type(data_connector_query))}", which is illegal.
                """
            )
        if limit and not isinstance(limit, int):
            raise TypeError(
                f"""The type of limit must be an integer (Python "int").  The type given is "{str(type(limit))}", which
is illegal.
                """
            )


@public_api
class BatchRequest(BatchRequestBase):
    """A BatchRequest is the way to specify which data Great Expectations will validate.

    A Batch Request is provided to a Datasource in order to create a Batch.

    ---Documentation---
        - https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/how_to_get_one_or_more_batches_of_data_from_a_configured_datasource/#1-construct-a-batchrequest
        - https://docs.greatexpectations.io/docs/terms/batch_request

    The `data_connector_query` parameter can include an index slice:

    ```python
    {
        "index": "-3:"
    }
    ```

    or it can include a filter:

    ```python
    {
        "batch_filter_parameters": {"year": "2020"}
    }
    ```

    Args:
        datasource_name: name of the Datasource used to connect to the data
        data_connector_name: name of the DataConnector used to connect to the data
        data_asset_name: name of the DataAsset used to connect to the data
        data_connector_query: a dictionary of query parameters the DataConnector
            should use to filter the batches returned from a BatchRequest
        limit: if specified, the maximum number of *batches* to be returned
            (limit does not affect the number of records in each batch)
        batch_spec_passthrough: a dictionary of additional parameters that
            the ExecutionEngine will use to obtain a specific set of data

    Returns:
        BatchRequest
    """

    include_field_names: ClassVar[set[str]] = {
        "datasource_name",
        "data_connector_name",
        "data_asset_name",
        "data_connector_query",
        "limit",
        "batch_spec_passthrough",
    }

    def __init__(  # noqa: PLR0913
        self,
        datasource_name: str,
        data_connector_name: str,
        data_asset_name: str,
        data_connector_query: dict | None = None,
        limit: int | None = None,
        batch_spec_passthrough: dict | None = None,
    ) -> None:
        self._validate_init_parameters(
            datasource_name=datasource_name,
            data_connector_name=data_connector_name,
            data_asset_name=data_asset_name,
            data_connector_query=data_connector_query,
            limit=limit,
        )
        super().__init__(
            datasource_name=datasource_name,
            data_connector_name=data_connector_name,
            data_asset_name=data_asset_name,
            data_connector_query=data_connector_query,
            limit=limit,
            batch_spec_passthrough=batch_spec_passthrough,
        )


@public_api
class RuntimeBatchRequest(BatchRequestBase):
    """A RuntimeBatchRequest creates a Batch for a RuntimeDataConnector.

    Instead of serving as a description of what data Great Expectations should
    fetch, a RuntimeBatchRequest serves as a wrapper for data that is passed in
    at runtime (as an in-memory dataframe, file/S3 path, or SQL query), with
    user-provided identifiers for uniquely identifying the data.

    ---Documentation---
        - https://docs.greatexpectations.io/docs/terms/batch_request/#runtimedataconnector-and-runtimebatchrequest
        - https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/how_to_configure_a_runtimedataconnector/

    runtime_parameters will vary depending on the Datasource used with the data.

    For a dataframe:

    ```python
    {"batch_data": df}
    ```

    For a path on a filesystem:

    ```python
        {"path": "/path/to/data/file.csv"}
    ```

    Args:
        datasource_name: name of the Datasource used to connect to the data
        data_connector_name: name of the DataConnector used to connect to the data
        data_asset_name: name of the DataAsset used to connect to the data
        runtime_parameters: a dictionary containing the data to process,
            a path to the data, or a query, depending on the associated Datasource
        batch_identifiers: a dictionary to serve as a persistent, unique
            identifier for the data included in the Batch
        batch_spec_passthrough: a dictionary of additional parameters that
            the ExecutionEngine will use to obtain a specific set of data
    Returns:
        BatchRequest
    """

    include_field_names: ClassVar[set[str]] = {
        "datasource_name",
        "data_connector_name",
        "data_asset_name",
        "runtime_parameters",
        "batch_identifiers",
        "batch_spec_passthrough",
    }

    def __init__(  # noqa: PLR0913
        self,
        datasource_name: str,
        data_connector_name: str,
        data_asset_name: str,
        runtime_parameters: dict,
        batch_identifiers: dict,
        batch_spec_passthrough: dict | None = None,
    ) -> None:
        self._validate_init_parameters(
            datasource_name=datasource_name,
            data_connector_name=data_connector_name,
            data_asset_name=data_asset_name,
        )
        self._validate_runtime_batch_request_specific_init_parameters(
            runtime_parameters=runtime_parameters,
            batch_identifiers=batch_identifiers,
            batch_spec_passthrough=batch_spec_passthrough,
        )
        super().__init__(
            datasource_name=datasource_name,
            data_connector_name=data_connector_name,
            data_asset_name=data_asset_name,
            runtime_parameters=runtime_parameters,
            batch_identifiers=batch_identifiers,
            batch_spec_passthrough=batch_spec_passthrough,
        )

    @staticmethod
    def _validate_runtime_batch_request_specific_init_parameters(
        runtime_parameters: dict | None,
        batch_identifiers: dict | None,
        batch_spec_passthrough: dict | None = None,
    ) -> None:
        """
        We must have both or neither of runtime_parameters and batch_identifiers (but not either one of them).
        This is strict equivalence ("if-and-only") condition ("exclusive NOR"); otherwise, ("exclusive OR") means error.
        """
        if (not runtime_parameters and batch_identifiers) or (
            runtime_parameters and not batch_identifiers
        ):
            raise ValueError(
                "It must be that either both runtime_parameters and batch_identifiers are present, or both are missing"
            )

        # if there is a value, make sure it is a dict
        if runtime_parameters and not (isinstance(runtime_parameters, dict)):
            raise TypeError(
                f"""The runtime_parameters must be a non-empty dict object.
                The type given is "{str(type(runtime_parameters))}", which is an illegal type or an empty dictionary."""
            )

        # if there is a value, make sure it is a dict
        if batch_identifiers and not isinstance(batch_identifiers, dict):
            raise TypeError(
                f"""The type for batch_identifiers must be a dict object, with keys being identifiers defined in the
                data connector configuration.  The type given is "{str(type(batch_identifiers))}", which is illegal."""
            )

        if batch_spec_passthrough and not (isinstance(batch_spec_passthrough, dict)):
            raise TypeError(
                f"""The type for batch_spec_passthrough must be a dict object. The type given is \
"{str(type(batch_spec_passthrough))}", which is illegal.
"""
            )


# TODO: <Alex>The following class is to support the backward compatibility with the legacy design.</Alex>
class BatchMarkers(BatchKwargs):
    """A BatchMarkers is a special type of BatchKwargs (so that it has a batch_fingerprint) but it generally does
    NOT require specific keys and instead captures information about the OUTPUT of a datasource's fetch
    process, such as the timestamp at which a query was executed."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        if "ge_load_time" not in self:
            raise InvalidBatchIdError("BatchMarkers requires a ge_load_time")

    @property
    def ge_load_time(self):
        return self.get("ge_load_time")


class BatchData:
    def __init__(self, execution_engine) -> None:
        self._execution_engine = execution_engine

    @property
    def execution_engine(self):
        return self._execution_engine

    # noinspection PyMethodMayBeStatic
    def head(self, *args, **kwargs) -> pd.DataFrame:
        # CONFLICT ON PURPOSE. REMOVE.
        return pd.DataFrame({})


if pyspark.DataFrame:  # type: ignore[truthy-function] # False if NotImported
    BatchDataType: TypeAlias = Union[
        Type[BatchData], Type[pd.DataFrame], Type[pyspark.DataFrame]
    ]
else:
    BatchDataType = Union[Type[BatchData], Type[pd.DataFrame]]  # type: ignore[misc] # Cannot assign multiple types


# TODO: <Alex>This module needs to be cleaned up.
#  We have Batch used for the legacy design, and we also need Batch for the new design.
#  However, right now, the Batch from the legacy design is imported into execution engines of the new design.
#  As a result, we have multiple, inconsistent versions of BatchMarkers, extending legacy/new classes.</Alex>
# TODO: <Alex>See also "great_expectations/datasource/types/batch_spec.py".</Alex>
@public_api
@deprecated_argument(argument_name="data_context", version="0.14.0")
@deprecated_argument(argument_name="datasource_name", version="0.14.0")
@deprecated_argument(argument_name="batch_parameters", version="0.14.0")
@deprecated_argument(argument_name="batch_kwargs", version="0.14.0")
class Batch(SerializableDictDot):
    """A Batch is a selection of records from a Data Asset.

    A Datasource produces Batch objects to interact directly with data. Creating
    a Batch does NOT require moving data; the Batch facilitates access to the
    data and maintains metadata.

    ---Documentation---
            - https://docs.greatexpectations.io/docs/terms/batch/

    Args:
        data: A BatchDataType object which interacts directly with the
            ExecutionEngine.
        batch_request: BatchRequest that was used to obtain the data.
        batch_definition: Complete BatchDefinition that describes the data.
        batch_spec: Complete BatchSpec that describes the data.
        batch_markers: Additional metadata that may be useful to understand
            batch.
        data_context: DataContext connected to the
        datasource_name: name of datasource used to obtain the batch
        batch_parameters: keyword arguments describing the batch data
        batch_kwargs: keyword arguments used to request a batch from a Datasource

    Returns:
        Batch instance created.
    """

    def __init__(  # noqa: PLR0913
        self,
        data: BatchDataType | None = None,
        batch_request: BatchRequestBase | dict | None = None,
        batch_definition: BatchDefinition | None = None,
        batch_spec: BatchSpec | None = None,
        batch_markers: BatchMarkers | None = None,
        # The remaining parameters are for backward compatibility.
        data_context=None,
        datasource_name=None,
        batch_parameters=None,
        batch_kwargs=None,
    ) -> None:
        self._data = data
        if batch_request is None:
            batch_request = {}

        self._batch_request = batch_request

        if batch_definition is None:
            batch_definition = IDDict()  # type: ignore[assignment]

        self._batch_definition = batch_definition

        if batch_spec is None:
            batch_spec = BatchSpec()

        self._batch_spec = batch_spec

        if batch_markers is None:
            batch_markers = BatchMarkers(
                {
                    "ge_load_time": datetime.datetime.now(
                        datetime.timezone.utc
                    ).strftime("%Y%m%dT%H%M%S.%fZ")
                }
            )

        self._batch_markers = batch_markers

        # The remaining parameters are for backward compatibility.
        self._data_context = data_context
        self._datasource_name = datasource_name
        self._batch_parameters = batch_parameters
        self._batch_kwargs = batch_kwargs or BatchKwargs()

    @property
    def data(self) -> BatchDataType | None:
        """Getter for Batch data"""
        return self._data

    @data.setter
    def data(self, value: BatchDataType) -> None:
        """Setter for Batch data"""
        self._data = value

    @property
    def batch_request(self):
        return self._batch_request

    @batch_request.setter
    def batch_request(self, batch_request) -> None:
        self._batch_request = batch_request

    @property
    def batch_definition(self):
        return self._batch_definition

    @batch_definition.setter
    def batch_definition(self, batch_definition) -> None:
        self._batch_definition = batch_definition

    @property
    def batch_spec(self):
        return self._batch_spec

    @property
    def batch_markers(self):
        return self._batch_markers

    # The remaining properties are for backward compatibility.
    @property
    def data_context(self):
        return self._data_context

    @property
    def datasource_name(self):
        return self._datasource_name

    @property
    def batch_parameters(self):
        return self._batch_parameters

    @property
    def batch_kwargs(self):
        return self._batch_kwargs

    def to_dict(self) -> dict:
        dict_obj: dict = {
            "data": str(self.data),
            "batch_request": self.batch_request.to_dict(),
            "batch_definition": self.batch_definition.to_json_dict()
            if isinstance(self.batch_definition, BatchDefinition)
            else {},
            "batch_spec": self.batch_spec,
            "batch_markers": self.batch_markers,
        }
        return dict_obj

    @public_api
    def to_json_dict(self) -> dict[str, JSONValues]:
        """Returns a JSON-serializable dict representation of this Batch.

        Returns:
            A JSON-serializable dict representation of this Batch.
        """
        json_dict: dict = self.to_dict()
        deep_filter_properties_iterable(
            properties=json_dict["batch_request"],
            inplace=True,
        )
        return json_dict

    @property
    def id(self):
        batch_definition = self._batch_definition
        if isinstance(batch_definition, BatchDefinition):
            return batch_definition.id

        if isinstance(batch_definition, IDDict):
            return batch_definition.to_id()

        if isinstance(batch_definition, dict):
            return IDDict(batch_definition).to_id()

        return IDDict({}).to_id()

    def __str__(self):
        return json.dumps(self.to_json_dict(), indent=2)

    @public_api
    def head(self, n_rows: int = 5, fetch_all: bool = False) -> pd.DataFrame:
        """Return the first n rows from the Batch.

        This function returns the first n_rows rows. It is useful for quickly testing
        if your object has the data you expected.

        It will always obtain data from the Datasource and return a Pandas
        DataFrame available locally.

        Args:
             n_rows: the number of rows to return
             fetch_all: whether to fetch all rows; overrides n_rows if set to True

        Returns:
            A Pandas DataFrame
        """
        self._data.execution_engine.batch_manager.load_batch_list(batch_list=[self])  # type: ignore[union-attr]
        metrics_calculator = _get_metrics_calculator_class()(
            execution_engine=self._data.execution_engine,  # type: ignore[union-attr, arg-type]
            show_progress_bars=True,
        )
        table_head_df: pd.DataFrame = metrics_calculator.head(
            n_rows=n_rows,
            domain_kwargs={"batch_id": self.id},
            fetch_all=fetch_all,
        )
        return table_head_df


def materialize_batch_request(
    batch_request: BatchRequestBase | dict | None = None,
) -> FluentBatchRequest | BatchRequestBase | None:
    def _is_fluent_batch_request(
        args: dict[str, Any] | BlockConfigBatchRequestTypedDict
    ) -> bool:
        from great_expectations.datasource.fluent.constants import _DATA_CONNECTOR_NAME

        return (
            ("datasource_name" in args)
            and ("data_asset_name" in args)
            and (
                ("data_connector_name" not in args)
                or (args["data_connector_name"].lower() == _DATA_CONNECTOR_NAME)
            )
        )

    effective_batch_request = get_batch_request_as_dict(batch_request=batch_request)

    if not effective_batch_request:
        return None

    batch_request_class: type
    if _is_fluent_batch_request(args=effective_batch_request):
        batch_request_class = _get_fluent_batch_request_class()
    elif batch_request_contains_runtime_parameters(
        batch_request=effective_batch_request
    ):
        batch_request_class = RuntimeBatchRequest
    else:
        batch_request_class = BatchRequest

    return batch_request_class(**effective_batch_request)


def batch_request_contains_batch_data(
    batch_request: BatchRequestBase
    | FluentBatchRequest
    | dict
    | BlockConfigBatchRequestTypedDict
    | None = None,
) -> bool:
    return (
        batch_request_contains_runtime_parameters(batch_request=batch_request)
        and batch_request["runtime_parameters"].get("batch_data") is not None  # type: ignore[index] # FDS BatchRequest is pydantic model
    )


def batch_request_contains_runtime_parameters(
    batch_request: BatchRequestBase
    | FluentBatchRequest
    | dict
    | BlockConfigBatchRequestTypedDict
    | None = None,
) -> bool:
    return (
        batch_request is not None
        and isinstance(batch_request, (dict, DictDot))
        and batch_request.get("runtime_parameters") is not None
    )


@overload
def get_batch_request_as_dict(  # type: ignore[misc] # Overload with None
    batch_request: BatchRequestBase
    | FluentBatchRequest
    | dict
    | BlockConfigBatchRequestTypedDict = ...,
) -> dict:
    ...


@overload
def get_batch_request_as_dict(
    batch_request: None = ...,
) -> None:
    ...


def get_batch_request_as_dict(
    batch_request: BatchRequestBase
    | FluentBatchRequest
    | dict
    | BlockConfigBatchRequestTypedDict
    | None = None,
) -> dict | None:
    if batch_request is None:
        return None

    if isinstance(batch_request, (BatchRequest, RuntimeBatchRequest)):
        batch_request = batch_request.to_dict()

    if isinstance(batch_request, _get_fluent_batch_request_class()):
        batch_request = batch_request.dict()

    return batch_request  # type: ignore[return-value] # FDS BatchRequest is missing data_connector (other fields)


def _get_block_batch_request(  # noqa: PLR0913
    *,
    datasource_name: str | None = None,
    data_connector_name: str | None = None,
    data_asset_name: str | None = None,
    batch_identifiers: dict | None = None,
    batch_spec_passthrough: dict | None = None,
    batch_filter_parameters: dict | None = None,
    data_connector_query: dict | None = None,
    limit: int | None = None,
    index: int | list | tuple | slice | str | None = None,
    custom_filter_function: Callable | None = None,
    sampling_method: str | None = None,
    sampling_kwargs: dict | None = None,
    splitter_method: str | None = None,
    splitter_kwargs: dict | None = None,
    **kwargs,
):
    """Returns a block-config batch request based on the provided parameters

    raises:
        ValueError if the parameters don't support creating a block-config batch request
    """
    if data_connector_query is None:
        if batch_filter_parameters is not None and batch_identifiers is not None:
            raise ValueError(
                'Must provide either "batch_filter_parameters" or "batch_identifiers", not both.'
            )

        if batch_filter_parameters is None and batch_identifiers is not None:
            logger.warning(
                'Attempting to build data_connector_query but "batch_identifiers" was provided '
                'instead of "batch_filter_parameters". The "batch_identifiers" key on '
                'data_connector_query has been renamed to "batch_filter_parameters". Please update '
                'your code. Falling back on provided "batch_identifiers".'
            )
            batch_filter_parameters = batch_identifiers
        elif batch_filter_parameters is None and batch_identifiers is None:
            batch_filter_parameters = kwargs
        else:
            # Raise a warning if kwargs exist
            pass

        data_connector_query_params: dict = {
            "batch_filter_parameters": batch_filter_parameters,
            "limit": limit,
            "index": index,
            "custom_filter_function": custom_filter_function,
        }
        data_connector_query = IDDict(data_connector_query_params)
    else:
        # TODO: Raise a warning if batch_filter_parameters or kwargs exist
        data_connector_query = IDDict(data_connector_query)

    if batch_spec_passthrough is None:
        batch_spec_passthrough = {}
        if sampling_method is not None:
            sampling_params: dict = {
                "sampling_method": sampling_method,
            }
            if sampling_kwargs is not None:
                sampling_params["sampling_kwargs"] = sampling_kwargs
            batch_spec_passthrough.update(sampling_params)
        if splitter_method is not None:
            splitter_params: dict = {
                "splitter_method": splitter_method,
            }
            if splitter_kwargs is not None:
                splitter_params["splitter_kwargs"] = splitter_kwargs
            batch_spec_passthrough.update(splitter_params)

    batch_request_as_dict: dict = {
        "datasource_name": datasource_name,
        "data_connector_name": data_connector_name,
        "data_asset_name": data_asset_name,
        "data_connector_query": data_connector_query,
        "batch_spec_passthrough": batch_spec_passthrough,
    }

    deep_filter_properties_iterable(
        properties=batch_request_as_dict,
        inplace=True,
    )

    return BatchRequest(**batch_request_as_dict)


def _get_runtime_batch_request(  # noqa: PLR0913
    *,
    datasource_name: str,
    data_connector_name: str,
    data_asset_name: str,
    runtime_parameters: dict | None = None,
    batch_identifiers: dict | None = None,
    batch_spec_passthrough: dict | None = None,
    batch_data: Any | None = None,
    query: str | None = None,
    path: str | None = None,
    **kwargs,
) -> RuntimeBatchRequest | None:
    """Returns a `RuntimeBatchRequest`, or `None` if the arguments don't support it."""
    if any(
        [batch_data, query, path, runtime_parameters]
    ):  # one of these must be specified for runtime batch requests
        # parameter checking
        if len([arg for arg in [batch_data, query, path] if arg is not None]) > 1:
            raise ValueError("Must provide only one of batch_data, query, or path.")

        if runtime_parameters and any(
            [
                batch_data and "batch_data" in runtime_parameters,
                query and "query" in runtime_parameters,
                path and "path" in runtime_parameters,
            ]
        ):
            raise ValueError(
                "If batch_data, query, or path arguments are provided, the same keys cannot appear in the "
                "runtime_parameters argument."
            )

        # build runtime parameters
        runtime_parameters = runtime_parameters or {}
        if batch_data:
            runtime_parameters["batch_data"] = batch_data
        elif query:
            runtime_parameters["query"] = query
        elif path:
            runtime_parameters["path"] = path

        if batch_identifiers is None:
            batch_identifiers = kwargs
        else:
            # TODO: Raise a warning if kwargs exist
            pass

        RuntimeBatchRequest._validate_runtime_batch_request_specific_init_parameters(
            runtime_parameters=runtime_parameters,
            batch_identifiers=batch_identifiers,
            batch_spec_passthrough=batch_spec_passthrough,
        )

        batch_request_as_dict: BlockConfigBatchRequestTypedDict = {
            "datasource_name": datasource_name,
            "data_connector_name": data_connector_name,
            "data_asset_name": data_asset_name,
            "runtime_parameters": runtime_parameters,
            "batch_identifiers": batch_identifiers,
            "batch_spec_passthrough": batch_spec_passthrough,  # type: ignore[typeddict-item] # could be None
        }

        deep_filter_properties_iterable(
            properties=batch_request_as_dict,  # type: ignore[call-overload] # TypeDict is dict at runtime
            inplace=True,
        )

        return RuntimeBatchRequest(**batch_request_as_dict)  # type: ignore[misc] # `data_connector_query` is not a valid kw arg.
    else:
        return None


def get_batch_request_from_acceptable_arguments(  # noqa: PLR0913
    datasource_name: str | None = None,
    data_connector_name: str | None = None,
    data_asset_name: str | None = None,
    *,
    batch_request: BatchRequestBase | None = None,
    batch_data: Any | None = None,
    data_connector_query: dict | None = None,
    batch_identifiers: dict | None = None,
    limit: int | None = None,
    index: int | list | tuple | slice | str | None = None,
    custom_filter_function: Callable | None = None,
    batch_spec_passthrough: dict | None = None,
    sampling_method: str | None = None,
    sampling_kwargs: dict | None = None,
    splitter_method: str | None = None,
    splitter_kwargs: dict | None = None,
    runtime_parameters: dict | None = None,
    query: str | None = None,
    path: str | None = None,
    batch_filter_parameters: dict | None = None,
    batch_request_options: dict | BatchRequestOptions | None = None,
    **kwargs,
) -> BatchRequest | RuntimeBatchRequest | FluentBatchRequest:
    """Obtain formal BatchRequest typed object from allowed attributes (supplied as arguments).
    This method applies only to the new (V3) Datasource schema.

    Args:
        datasource_name
        data_connector_name
        data_asset_name

        batch_request
        batch_data
        query
        path
        runtime_parameters
        data_connector_query
        batch_identifiers
        batch_filter_parameters

        limit
        index
        custom_filter_function

        sampling_method
        sampling_kwargs

        splitter_method
        splitter_kwargs

        batch_spec_passthrough

        batch_request_options

        **kwargs

    Returns:
        (BatchRequest, RuntimeBatchRequest or FluentBatchRequest) The formal BatchRequest, RuntimeBatchRequest or FluentBatchRequest object
    """

    # block-style batch-request args, includes arguments for both runtime and basic batch requests
    block_config_args = {
        "batch_data": batch_data,
        "query": query,
        "path": path,
        "runtime_parameters": runtime_parameters,
        "data_connector_query": data_connector_query,
        "batch_identifiers": batch_identifiers,
        "batch_filter_parameters": batch_filter_parameters,
        "limit": limit,
        "index": index,
        "custom_filter_function": custom_filter_function,
        "sampling_method": sampling_method,
        "sampling_kwargs": sampling_kwargs,
        "splitter_method": splitter_method,
        "splitter_kwargs": splitter_kwargs,
        "batch_spec_passthrough": batch_spec_passthrough,
    }

    def block_style_args() -> list[str]:
        """Returns a list of the block-config batch request arguments"""
        return [k for k, v in block_config_args.items() if v]

    # ensure that the first parameter is datasource_name, which should be a str. This check prevents users
    # from passing in batch_request as an unnamed parameter.
    if datasource_name and not isinstance(datasource_name, str):
        raise TypeError(
            f"the first parameter, datasource_name, must be a str, not {type(datasource_name)}"
        )

    # if batch_request is specified, ignore all other arguments and return the batch_request
    if batch_request:
        if not isinstance(
            batch_request,
            (BatchRequest, RuntimeBatchRequest, _get_fluent_batch_request_class()),
        ):
            raise TypeError(
                "batch_request must be a BatchRequest, RuntimeBatchRequest, or a "
                f"fluent BatchRequest object, not {type(batch_request)}"
            )

        return batch_request

    # try to get a runtime batch request
    result: RuntimeBatchRequest | FluentBatchRequest | None
    result = _get_runtime_batch_request(
        datasource_name=datasource_name,  # type: ignore[arg-type] # could be None
        data_connector_name=data_connector_name,  # type: ignore[arg-type] # could be None
        data_asset_name=data_asset_name,  # type: ignore[arg-type] # could be None
        runtime_parameters=runtime_parameters,
        batch_identifiers=batch_identifiers,
        batch_spec_passthrough=batch_spec_passthrough,
        batch_data=batch_data,
        query=query,
        path=path,
        **kwargs,
    )
    if result:
        return result

    # try to get a fluent batch request
    if (datasource_name and data_asset_name) and not data_connector_name:
        block_args = block_style_args()
        if block_args:
            raise ValueError(
                f"Arguments: {', '.join(block_args)} are not supported for Fluent Batch Requests. Block-config Requests require a data connector name"
            )

        result = _get_fluent_batch_request_class()(
            datasource_name=datasource_name,
            data_asset_name=data_asset_name,
            options=batch_request_options,
        )
        return result

    # try to get a block batch request
    return _get_block_batch_request(
        datasource_name=datasource_name,
        data_connector_name=data_connector_name,
        data_asset_name=data_asset_name,
        batch_identifiers=batch_identifiers,
        batch_spec_passthrough=batch_spec_passthrough,
        batch_filter_parameters=batch_filter_parameters,
        data_connector_query=data_connector_query,
        limit=limit,
        index=index,
        custom_filter_function=custom_filter_function,
        sampling_method=sampling_method,
        sampling_kwargs=sampling_kwargs,
        splitter_method=splitter_method,
        splitter_kwargs=splitter_kwargs,
        **kwargs,
    )


def standardize_batch_request_display_ordering(
    batch_request: BlockConfigBatchRequestTypedDict,
) -> BlockConfigBatchRequestTypedDict:
    batch_request_as_dict: dict = safe_deep_copy(data=batch_request)
    datasource_name: str = batch_request_as_dict["datasource_name"]
    data_connector_name: str = batch_request_as_dict["data_connector_name"]
    data_asset_name: str = batch_request_as_dict["data_asset_name"]
    runtime_parameters: dict | None = batch_request_as_dict.get("runtime_parameters")
    batch_identifiers: dict | None = batch_request_as_dict.get("batch_identifiers")
    batch_request_as_dict.pop("datasource_name")
    batch_request_as_dict.pop("data_connector_name")
    batch_request_as_dict.pop("data_asset_name")
    # NOTE: AJB 20211217 The below conditionals should be refactored
    if runtime_parameters is not None:
        batch_request_as_dict.pop("runtime_parameters")
    if batch_identifiers is not None:
        batch_request_as_dict.pop("batch_identifiers")
    if runtime_parameters is not None and batch_identifiers is not None:
        batch_request_as_dict = {
            "datasource_name": datasource_name,
            "data_connector_name": data_connector_name,
            "data_asset_name": data_asset_name,
            "runtime_parameters": runtime_parameters,
            "batch_identifiers": batch_identifiers,
            **batch_request_as_dict,
        }
    elif runtime_parameters is not None and batch_identifiers is None:
        batch_request_as_dict = {
            "datasource_name": datasource_name,
            "data_connector_name": data_connector_name,
            "data_asset_name": data_asset_name,
            "runtime_parameters": runtime_parameters,
            **batch_request_as_dict,
        }
    elif runtime_parameters is None and batch_identifiers is not None:
        batch_request_as_dict = {
            "datasource_name": datasource_name,
            "data_connector_name": data_connector_name,
            "data_asset_name": data_asset_name,
            "batch_identifiers": batch_identifiers,
            **batch_request_as_dict,
        }
    else:
        batch_request_as_dict = {
            "datasource_name": datasource_name,
            "data_connector_name": data_connector_name,
            "data_asset_name": data_asset_name,
            **batch_request_as_dict,
        }

    return batch_request_as_dict  # type: ignore[return-value] # TODO: create a new object to return instead of popping
