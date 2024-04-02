from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING, Dict, Iterator, List, Optional, Tuple, Union

import great_expectations.exceptions as gx_exceptions
from great_expectations._docs_decorators import public_api
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.core.batch import (
    BatchRequest,
    BatchSpec,
    IDDict,
    LegacyBatchDefinition,
)
from great_expectations.core.batch_spec import SqlAlchemyDatasourceBatchSpec
from great_expectations.datasource.data_connector.batch_filter import (
    BatchFilter,
    build_batch_filter,
)
from great_expectations.datasource.data_connector.data_connector import DataConnector
from great_expectations.datasource.data_connector.sorter import (
    DictionarySorter,
    LexicographicSorter,
    NumericSorter,
    Sorter,
)
from great_expectations.datasource.data_connector.util import (
    batch_definition_matches_batch_request,
    build_sorters_from_config,
)
from great_expectations.execution_engine.partition_and_sample.data_partitioner import (
    DatePart,
    PartitionerMethod,
)
from great_expectations.util import deep_filter_properties_iterable

if TYPE_CHECKING:
    from great_expectations.execution_engine import (
        SqlAlchemyExecutionEngine,
    )


@public_api
class ConfiguredAssetSqlDataConnector(DataConnector):
    """A DataConnector that requires explicit listing of SQL assets you want to connect to.

    Being a Configured Asset Data Connector, it requires an explicit list of each Data Asset it can
    connect to. While this allows for fine-grained control over which Data Assets may be accessed,
    it requires more setup.

    Args:
        name (str): The name of this DataConnector
        datasource_name (str): The name of the Datasource that contains it
        execution_engine (ExecutionEngine): An ExecutionEngine
        include_schema_name (bool): Should the data_asset_name include the schema as a prefix?
        partitioner_method (str): A method to partition the target table into multiple Batches
        partitioner_kwargs (dict): Keyword arguments to pass to partitioner_method
        sorters (list): List if you want to override the default sort for the data_references
        sampling_method (str): A method to downsample within a target Batch
        sampling_kwargs (dict): Keyword arguments to pass to sampling_method
        batch_spec_passthrough (dict): dictionary with keys that will be added directly to batch_spec

    """  # noqa: E501

    PARTITIONER_METHOD_TO_SORTER_METHOD_MAPPING: Dict[PartitionerMethod, type[Sorter] | None] = {
        PartitionerMethod.PARTITION_ON_YEAR: DictionarySorter,
        PartitionerMethod.PARTITION_ON_YEAR_AND_MONTH: DictionarySorter,
        PartitionerMethod.PARTITION_ON_YEAR_AND_MONTH_AND_DAY: DictionarySorter,
        PartitionerMethod.PARTITION_ON_DATE_PARTS: DictionarySorter,
        PartitionerMethod.PARTITION_ON_WHOLE_TABLE: None,
        PartitionerMethod.PARTITION_ON_COLUMN_VALUE: LexicographicSorter,
        PartitionerMethod.PARTITION_ON_CONVERTED_DATETIME: LexicographicSorter,
        PartitionerMethod.PARTITION_ON_DIVIDED_INTEGER: NumericSorter,
        PartitionerMethod.PARTITION_ON_MOD_INTEGER: NumericSorter,
        PartitionerMethod.PARTITION_ON_MULTI_COLUMN_VALUES: LexicographicSorter,
        PartitionerMethod.PARTITION_ON_HASHED_COLUMN: LexicographicSorter,
    }

    def __init__(  # noqa: PLR0913
        self,
        name: str,
        datasource_name: str,
        execution_engine: SqlAlchemyExecutionEngine,
        include_schema_name: bool = False,
        partitioner_method: Optional[str] = None,
        partitioner_kwargs: Optional[dict] = None,
        sorters: Optional[list] = None,
        sampling_method: Optional[str] = None,
        sampling_kwargs: Optional[dict] = None,
        assets: Optional[Dict[str, dict]] = None,
        batch_spec_passthrough: Optional[dict] = None,
        id: Optional[str] = None,
    ) -> None:
        super().__init__(
            name=name,
            id=id,
            datasource_name=datasource_name,
            execution_engine=execution_engine,
            batch_spec_passthrough=batch_spec_passthrough,
        )

        self._include_schema_name = include_schema_name
        self._partitioner_method = partitioner_method
        self._partitioner_kwargs = partitioner_kwargs

        self._sorters = build_sorters_from_config(config_list=sorters)

        self._sampling_method = sampling_method
        self._sampling_kwargs = sampling_kwargs

        self._assets = {}

        self._refresh_data_assets_cache(assets=assets)

        self._data_references_cache = {}

        self._validate_sorters_configuration()

    @property
    def execution_engine(self) -> SqlAlchemyExecutionEngine:
        return self._execution_engine

    @property
    def include_schema_name(self) -> bool:
        return self._include_schema_name

    @property
    def partitioner_method(self) -> Optional[str]:
        return self._partitioner_method

    @property
    def partitioner_kwargs(self) -> Optional[dict]:
        return self._partitioner_kwargs

    @property
    def sorters(self) -> Optional[dict]:
        return self._sorters

    @property
    def sampling_method(self) -> Optional[str]:
        return self._sampling_method

    @property
    def sampling_kwargs(self) -> Optional[dict]:
        return self._sampling_kwargs

    @property
    def assets(self) -> Optional[Dict[str, dict]]:
        return self._assets

    def add_data_asset(
        self,
        name: str,
        config: dict,
    ) -> None:
        """
        Add data_asset to DataConnector using data_asset name as key, and data_asset config as value.
        """  # noqa: E501
        name = self._update_data_asset_name_from_config(
            data_asset_name=name, data_asset_config=config
        )
        self._assets[name] = config

    def get_batch_definition_list_from_batch_request(self, batch_request: BatchRequest):  # noqa: C901
        """
        Retrieve batch_definitions that match batch_request

        First retrieves all batch_definitions that match batch_request
            - if batch_request also has a batch_filter, then select batch_definitions that match batch_filter.
            - NOTE : currently sql data connectors do not support sorters.

        Args:
            batch_request (BatchRequestBase): BatchRequestBase (BatchRequest without attribute validation) to process

        Returns:
            A list of BatchDefinition objects that match BatchRequest
        """  # noqa: E501
        self._validate_batch_request(batch_request=batch_request)

        if len(self._data_references_cache) == 0:
            self._refresh_data_references_cache()

        batch_definition_list: List[LegacyBatchDefinition] = []
        try:
            sub_cache = self._get_data_reference_list_from_cache_by_data_asset_name(
                data_asset_name=batch_request.data_asset_name
            )
        except KeyError:
            raise KeyError(f"data_asset_name {batch_request.data_asset_name} is not recognized.")  # noqa: TRY003

        for batch_identifiers in sub_cache:
            batch_definition = LegacyBatchDefinition(
                datasource_name=self.datasource_name,
                data_connector_name=self.name,
                data_asset_name=batch_request.data_asset_name,
                batch_identifiers=IDDict(batch_identifiers),
                batch_spec_passthrough=batch_request.batch_spec_passthrough,
            )
            if batch_definition_matches_batch_request(batch_definition, batch_request):
                batch_definition_list.append(batch_definition)

        data_asset: Dict[str, Union[str, list, None]] = self.assets[batch_request.data_asset_name]
        data_asset_partitioner_method: Optional[str] = data_asset.get("partitioner_method")
        data_asset_partitioner_kwargs: Optional[Dict[str, Union[str, list]]] = data_asset.get(
            "partitioner_kwargs"
        )
        data_asset_sorters: Optional[dict] = data_asset.get("sorters")

        # if sorters have been explicitly passed to the data connector use them for sorting,
        # otherwise sorting behavior can be inferred from partitioner_method.
        if data_asset_sorters is not None and len(data_asset_sorters) > 0:
            batch_definition_list = self._sort_batch_definition_list(
                batch_definition_list=batch_definition_list,
                partitioner_method_name=None,
                partitioner_kwargs=None,
                sorters=data_asset_sorters,
            )
        elif data_asset_partitioner_method is not None:
            batch_definition_list = self._sort_batch_definition_list(
                batch_definition_list=batch_definition_list,
                partitioner_method_name=data_asset_partitioner_method,
                partitioner_kwargs=data_asset_partitioner_kwargs,
                sorters=None,
            )

        if batch_request.data_connector_query is not None:
            data_connector_query_dict = batch_request.data_connector_query.copy()
            if batch_request.limit is not None and data_connector_query_dict.get("limit") is None:
                data_connector_query_dict["limit"] = batch_request.limit

            batch_filter_obj: BatchFilter = build_batch_filter(
                data_connector_query_dict=data_connector_query_dict
            )
            batch_definition_list = batch_filter_obj.select_from_data_connector_query(
                batch_definition_list=batch_definition_list
            )

        return batch_definition_list

    @public_api
    def get_available_data_asset_names(self) -> List[str]:
        """Return the list of asset names known by this DataConnector.

        Returns:
            A list of available names
        """
        return list(self.assets.keys())

    def get_unmatched_data_references(self) -> List[str]:
        """
        Returns the list of data_references unmatched by configuration by looping through items in _data_references_cache
        and returning data_reference that do not have an associated data_asset.

        Returns:
            list of data_references that are not matched by configuration.
        """  # noqa: E501
        return []

    def get_available_data_asset_names_and_types(self) -> List[Tuple[str, str]]:
        """
        Return the list of asset names and types known by this DataConnector.

        Returns:
        A list of tuples consisting of available names and types
        """
        return [(asset["table_name"], asset["type"]) for asset in self.assets.values()]

    def build_batch_spec(
        self, batch_definition: LegacyBatchDefinition
    ) -> SqlAlchemyDatasourceBatchSpec:
        """
        Build BatchSpec from batch_definition by calling DataConnector's build_batch_spec function.

        Args:
            batch_definition (LegacyBatchDefinition): to be used to build batch_spec

        Returns:
            BatchSpec built from batch_definition
        """

        data_asset_name: str = batch_definition.data_asset_name
        if (
            data_asset_name in self.assets
            and self.assets[data_asset_name].get("batch_spec_passthrough")
            and isinstance(self.assets[data_asset_name].get("batch_spec_passthrough"), dict)
        ):
            # batch_spec_passthrough from data_asset
            batch_spec_passthrough = deepcopy(
                self.assets[data_asset_name]["batch_spec_passthrough"]
            )
            batch_definition_batch_spec_passthrough = (
                deepcopy(batch_definition.batch_spec_passthrough) or {}
            )
            # batch_spec_passthrough from Batch Definition supersedes batch_spec_passthrough from data_asset  # noqa: E501
            batch_spec_passthrough.update(batch_definition_batch_spec_passthrough)
            batch_definition.batch_spec_passthrough = batch_spec_passthrough

        batch_spec: BatchSpec = super().build_batch_spec(batch_definition=batch_definition)

        return SqlAlchemyDatasourceBatchSpec(batch_spec)

    def _get_sorters_from_partitioner_method_name(
        self,
        partitioner_method_name: str,
        partitioner_kwargs: Dict[str, Union[str, list]],
    ) -> List[Sorter]:
        """Given a partitioner method and its kwargs, return an appropriately instantiated list of Sorters.

        Args:
            partitioner_method_name: partitioner name starting with or without preceding '_'.
            partitioner_kwargs: partitioner kwargs dictionary for partitioner directives.

        Returns:
            an ordered list of sorters required to sort partitioner batches.
        """  # noqa: E501
        partitioner_method_to_sorter_method_mapping: Dict[str, Optional[Sorter]] = (
            self.PARTITIONER_METHOD_TO_SORTER_METHOD_MAPPING
        )
        partitioner_method_name: str = self._get_partitioner_method_name(
            partitioner_method_name=partitioner_method_name,
        )
        try:
            sorter_method: Optional[Sorter] = partitioner_method_to_sorter_method_mapping[
                partitioner_method_name
            ]
        except KeyError:
            raise gx_exceptions.SorterError(  # noqa: TRY003
                f"No Sorter is defined in ConfiguredAssetSqlDataConnector.PARTITIONER_METHOD_TO_SORTER_METHOD_MAPPING for partitioner_method: {partitioner_method_name}"  # noqa: E501
            )

        if sorter_method == DictionarySorter:
            sorted_date_parts = [
                DatePart.YEAR,
                DatePart.MONTH,
                DatePart.DAY,
                DatePart.HOUR,
                DatePart.MINUTE,
                DatePart.SECOND,
            ]
            return [
                DictionarySorter(
                    name=partitioner_kwargs["column_name"],
                    key_reference_list=sorted_date_parts,
                )
            ]
        elif sorter_method == LexicographicSorter:
            if partitioner_method_name == PartitionerMethod.PARTITION_ON_MULTI_COLUMN_VALUES:
                return [
                    LexicographicSorter(name=column_name)
                    for column_name in partitioner_kwargs["column_names"]
                ]
            else:
                return [LexicographicSorter(name=partitioner_kwargs["column_name"])]
        elif sorter_method == NumericSorter:
            return [NumericSorter(name=partitioner_kwargs["column_name"])]
        else:
            return []

    def _validate_sorters_configuration(self) -> None:
        for data_asset_name, data_asset_config in self.assets.items():
            sorters = data_asset_config.get("sorters")
            partitioner_method = data_asset_config.get("partitioner_method")
            partitioner_kwargs = data_asset_config.get("partitioner_kwargs")
            if (
                partitioner_method is not None
                and partitioner_kwargs is not None
                and sorters is not None
                and len(sorters) > 0
            ):
                partitioner_group_names: List[str]
                if "column_names" in partitioner_kwargs:
                    partitioner_group_names = partitioner_kwargs["column_names"]
                else:
                    partitioner_group_names = [partitioner_kwargs["column_name"]]

                if any(
                    sorter_name not in partitioner_group_names for sorter_name in sorters.keys()
                ):
                    raise gx_exceptions.DataConnectorError(  # noqa: TRY003
                        f"""DataConnector "{self.name}" specifies one or more sort keys that do not appear among the
keys used for partitioning.
                        """  # noqa: E501
                    )
                if len(partitioner_group_names) < len(sorters):
                    raise gx_exceptions.DataConnectorError(  # noqa: TRY003
                        f"""DataConnector "{self.name}" is configured with {len(partitioner_group_names)} partitioner groups;
this is fewer than number of sorters specified, which is {len(sorters)}.
                        """  # noqa: E501
                    )

    @staticmethod
    def _get_partitioner_method_name(partitioner_method_name: str) -> str:
        """Accept partitioner methods with or without starting with `_`.

        Args:
            partitioner_method_name: partitioner name starting with or without preceding `_`.

        Returns:
            partitioner method name stripped of preceding underscore.
        """
        if partitioner_method_name.startswith("_"):
            return partitioner_method_name[1:]
        else:
            return partitioner_method_name

    def _sort_batch_definition_list(
        self,
        batch_definition_list: List[LegacyBatchDefinition],
        partitioner_method_name: Optional[str],
        partitioner_kwargs: Optional[Dict[str, Union[str, dict, None]]],
        sorters: Optional[dict],
    ) -> List[LegacyBatchDefinition]:
        """Sort a list of batch definitions given the partitioner method used to define them.

        Args:
            batch_definition_list: an unsorted list of batch definitions.
            partitioner_method_name: partitioner name used to define the batches, starting with or without preceding `_`.
            partitioner_kwargs: partitioner kwargs dictionary for partitioner directives.
            sorters: sorters configured for the batch_definition_list

        Returns:
            a list of batch definitions sorted depending on partitioner method used to define them.
        """  # noqa: E501
        if (
            partitioner_method_name is not None
            and partitioner_kwargs is not None
            and sorters is None
        ):
            sorters = self._get_sorters_from_partitioner_method_name(
                partitioner_method_name=partitioner_method_name,
                partitioner_kwargs=partitioner_kwargs,
            )
        else:
            sorters: Iterator[Sorter] = reversed(list(sorters.values()))

        for sorter in sorters:
            batch_definition_list = sorter.get_sorted_batch_definitions(
                batch_definitions=batch_definition_list
            )
        return batch_definition_list

    def _refresh_data_assets_cache(
        self,
        assets: Optional[Dict[str, dict]] = None,
    ) -> None:
        self._assets = {}

        if assets:
            data_asset_name: str
            data_asset_config: dict
            for data_asset_name, data_asset_config in assets.items():
                sorters = data_asset_config.get("sorters")
                if sorters is not None:
                    sorters = build_sorters_from_config(config_list=sorters)

                aux_config: dict = {
                    "partitioner_method": data_asset_config.get(
                        "partitioner_method", self.partitioner_method
                    ),
                    "partitioner_kwargs": data_asset_config.get(
                        "partitioner_kwargs", self.partitioner_kwargs
                    ),
                    "sampling_method": data_asset_config.get(
                        "sampling_method", self.sampling_method
                    ),
                    "sampling_kwargs": data_asset_config.get(
                        "sampling_kwargs", self.sampling_kwargs
                    ),
                    "sorters": sorters or self.sorters,
                }

                deep_filter_properties_iterable(
                    properties=aux_config,
                    inplace=True,
                )
                data_asset_config.update(aux_config)
                data_asset_config.update(
                    {
                        "type": data_asset_config.get("type"),
                        "table_name": data_asset_config.get("table_name", data_asset_name),
                    }
                )

                self.add_data_asset(name=data_asset_name, config=data_asset_config)

    def _update_data_asset_name_from_config(
        self, data_asset_name: str, data_asset_config: dict
    ) -> str:
        schema_name: str = data_asset_config.get("schema_name")
        include_schema_name: bool = data_asset_config.get(
            "include_schema_name", self.include_schema_name
        )

        if schema_name is not None and include_schema_name:
            schema_name = f"{schema_name}."
        else:
            schema_name = ""

        data_asset_name: str = f"{schema_name}{data_asset_name}"

        """
        In order to support "SimpleSqlalchemyDatasource", which supports "data_asset_name_prefix" and
        "data_asset_name_suffix" as part of "tables" (reserved key for configuring "ConfiguredAssetSqlDataConnector" for
        a table), these configuration attributes can exist in "data_asset_config" and must be handled appropriately.
        """  # noqa: E501
        data_asset_name_prefix: str = data_asset_config.get("data_asset_name_prefix", "")
        data_asset_name_suffix: str = data_asset_config.get("data_asset_name_suffix", "")

        data_asset_name: str = f"{data_asset_name_prefix}{data_asset_name}{data_asset_name_suffix}"

        return data_asset_name

    def _refresh_data_references_cache(self) -> None:
        self._data_references_cache = {}

        for data_asset_name in self.assets:
            data_asset_config = self.assets[data_asset_name]
            batch_identifiers_list = self._get_batch_identifiers_list_from_data_asset_config(
                data_asset_name=data_asset_name,
                data_asset_config=data_asset_config,
            )

            batch_definition_list = [
                LegacyBatchDefinition(
                    batch_identifiers=IDDict(batch_identifiers),
                    datasource_name=self.datasource_name,
                    data_connector_name=self.name,
                    data_asset_name=data_asset_name,
                )
                for batch_identifiers in batch_identifiers_list
            ]

            data_asset_partitioner_method: Optional[str] = data_asset_config.get(
                "partitioner_method"
            )
            data_asset_partitioner_kwargs: Optional[Dict[str, Union[str, list]]] = (
                data_asset_config.get("partitioner_kwargs")
            )
            data_asset_sorters: Optional[dict] = data_asset_config.get("sorters")

            # if sorters have been explicitly passed to the data connector use them for sorting,
            # otherwise sorting behavior can be inferred from partitioner_method.
            if data_asset_sorters is not None and len(data_asset_sorters) > 0:
                batch_definition_list = self._sort_batch_definition_list(
                    batch_definition_list=batch_definition_list,
                    partitioner_method_name=None,
                    partitioner_kwargs=None,
                    sorters=data_asset_sorters,
                )
            elif data_asset_partitioner_method is not None:
                batch_definition_list = self._sort_batch_definition_list(
                    batch_definition_list=batch_definition_list,
                    partitioner_method_name=data_asset_partitioner_method,
                    partitioner_kwargs=data_asset_partitioner_kwargs,
                    sorters=None,
                )
            self._data_references_cache[data_asset_name] = [
                batch_definition.batch_identifiers for batch_definition in batch_definition_list
            ]

    def _get_batch_identifiers_list_from_data_asset_config(
        self,
        data_asset_name: str,
        data_asset_config: dict,
    ) -> List[dict]:
        table_name: str = data_asset_config.get("table_name", data_asset_name)

        schema_name: str = data_asset_config.get("schema_name")
        if schema_name is not None:
            table_name = f"{schema_name}.{table_name}"

        batch_identifiers_list: List[dict]
        partitioner_method_name: Optional[str] = data_asset_config.get("partitioner_method")
        if partitioner_method_name is not None:
            partitioner_kwargs: Optional[dict] = data_asset_config.get("partitioner_kwargs")
            batch_identifiers_list = self.execution_engine.get_data_for_batch_identifiers(
                selectable=sa.text(table_name),
                partitioner_method_name=partitioner_method_name,
                partitioner_kwargs=partitioner_kwargs,
            )
        else:
            batch_identifiers_list = [{}]

        return batch_identifiers_list

    def _get_data_reference_list_from_cache_by_data_asset_name(
        self, data_asset_name: str
    ) -> List[dict]:
        return self._data_references_cache[data_asset_name]

    def _generate_batch_spec_parameters_from_batch_definition(
        self, batch_definition: LegacyBatchDefinition
    ) -> dict:
        """
        Build BatchSpec parameters from batch_definition with the following components:
            1. data_asset_name from batch_definition
            2. batch_identifiers from batch_definition
            3. data_asset from data_connector

        Args:
            batch_definition (LegacyBatchDefinition): to be used to build batch_spec

        Returns:
            dict built from batch_definition
        """
        data_asset_name: str = batch_definition.data_asset_name
        table_name: str = self._get_table_name_from_batch_definition(batch_definition)
        return {
            "data_asset_name": data_asset_name,
            "table_name": table_name,
            "batch_identifiers": batch_definition.batch_identifiers,
            **self.assets[data_asset_name],
        }

    def _get_table_name_from_batch_definition(self, batch_definition: LegacyBatchDefinition) -> str:
        """
        Helper method called by _generate_batch_spec_parameters_from_batch_definition() to parse table_name from
        data_asset_name in cases where schema is included.

        data_asset_name in those cases are [schema].[table_name].

        function will split data_asset_name on [schema]. and return the resulting table_name.
        """  # noqa: E501
        data_asset_name: str = batch_definition.data_asset_name
        data_asset_dict: dict = self.assets[data_asset_name]
        table_name: str = data_asset_dict["table_name"]
        schema_name: Optional[str] = None
        if "schema_name" in data_asset_dict:
            schema_name = data_asset_dict["schema_name"]

        if schema_name is not None and schema_name not in table_name:
            table_name = f"{schema_name}.{table_name}"

        return table_name

    def _map_data_reference_to_batch_definition_list(
        self,
        data_reference,
        data_asset_name: Optional[str] = None,  #: Any,
    ) -> Optional[List[LegacyBatchDefinition]]:
        # Note: This is a bit hacky, but it works. In sql_data_connectors, data references *are* dictionaries,  # noqa: E501
        # allowing us to invoke `IDDict(data_reference)`
        return [
            LegacyBatchDefinition(
                datasource_name=self.datasource_name,
                data_connector_name=self.name,
                data_asset_name=data_asset_name,
                batch_identifiers=IDDict(data_reference),
            )
        ]
